"""
Точка входа снайпинг-бота.

Архитектура:

  WsScanner / ChainScanner (Pump.fun через Helius)
      + GMGNClient (опционально)
      + SmartMoneyFeed (файл + proven из кэша/БД)
    -> BuyEvent
    -> WalletScreener / FundingGraph / InsiderDetector
    -> WalletProfiler (репутация) + PriceClient
    -> кластеризация + ScoringEngine
    -> RiskManager -> Telegram approve (опционально)
    -> UnifiedExecutor (PumpPortal, fallback Jupiter+Jito)
    -> PositionMonitor (whale-mirror / TP / SL / trailing)

  Параллельно: outcome resolver (lab_early + lab_accum shadow),
  health, Telegram, dashboard. История — SQLite.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from pathlib import Path
from logging.handlers import RotatingFileHandler

import aiohttp

from config import settings
from core.accumulation import AccumulationDetector
from core.whale_sit import VolSpikeDetector, LAB_TRACK as VOL_SPIKE_LAB
from core.whale_sit_poller import VolSpikePoller
from core.accum_balance import LAB_TRACK as ACCUM_BAL_LAB, WindowObs
from core.accum_balance_poller import AccumBalancePoller
from core.arkham_client import ArkhamClient
from core.catalyst_proxy import CatalystProxy
from core.chain_scanner import ChainScanner
from core.db import get_db
from core.executor import UnifiedExecutor
from core.funding_graph import FundingGraph
from core.gmgn_client import GMGNClient
from core.health import HealthMonitor
from core.insider_detector import InsiderDetector
from core.position_monitor import PositionMonitor
from core.price_client import PriceClient, TokenMarketInfo
from core.redis_cache import RedisCache
from core.risk_manager import RiskManager
from core import edge_model
from core.rug_checker import RugChecker
from core.scoring import EventFeatures, ScoringEngine
from core.smart_money_feed import SmartMoneyFeed
from core.telegram_bot import TelegramBot
from core.tx_parser import BuyEvent, SellEvent, TxParser
from core.ws_scanner import WsScanner
from core.wallet_profiler import WalletProfiler
from core.wallet_screener import WalletScreener
from dashboard.app import set_runtime, start_dashboard

settings.LOGS_DIR.mkdir(parents=True, exist_ok=True)
settings.DATA_DIR.mkdir(parents=True, exist_ok=True)

# Ротация: при работе без присмотра сутками лог не должен расти
# бесконечно, а если что-то уйдёт в горячий цикл — не должен забить диск.
_file_handler = RotatingFileHandler(
    settings.LOGS_DIR / "sniper_bot.log",
    maxBytes=50 * 1024 * 1024, backupCount=2, encoding="utf-8",
)

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), _file_handler],
)
logger = logging.getLogger("sniper.main")


class _SingleInstanceLock:
    """Не даёт запустить второй экземпляр бота на той же папке.

    Зачем: оба экземпляра держат свой снимок кэша в памяти и целиком
    переписывают data/cache_state.json каждые 30 секунд — то есть молча
    затирают наблюдения друг друга. Репутация кошельков после этого не
    накапливается, а расход лимитов Helius удваивается. Поймано на живом
    запуске, когда старый процесс остался работать после nohup.
    """

    def __init__(self, path: Path):
        self._path = path
        self._acquired = False

    def acquire(self) -> bool:
        if self._path.exists():
            try:
                existing_pid = int(self._path.read_text().strip())
            except (ValueError, OSError):
                existing_pid = None

            if existing_pid and self._is_running(existing_pid) and self._is_our_bot(existing_pid):
                logger.error(
                    "Бот уже запущен (PID %d). Второй экземпляр затирал бы накопленную "
                    "репутацию кошельков в общем кэше, поэтому запуск отменён.",
                    existing_pid,
                )
                logger.error("Остановить работающий: kill %d   (или pkill -f 'python main.py')", existing_pid)
                return False
            logger.warning("Найден файл блокировки от завершённого процесса — перезаписываю")

        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(str(os.getpid()))
        self._acquired = True
        return True

    @staticmethod
    def _is_running(pid: int) -> bool:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True

    @staticmethod
    def _is_our_bot(pid: int) -> bool:
        """PID мог быть переиспользован другой программой — проверяем cmdline."""
        try:
            import subprocess
            out = subprocess.check_output(["ps", "-p", str(pid), "-o", "command="], text=True)
        except Exception:  # noqa: BLE001
            return False
        return "main.py" in out

    def release(self):
        if self._acquired:
            try:
                self._path.unlink()
            except OSError:
                pass


class SniperBot:
    def __init__(self):
        self._buying: set[str] = set()   # монеты, по которым покупка уже в процессе
        self._session: aiohttp.ClientSession | None = None
        self._cache = RedisCache(settings.REDIS_URL)
        self._scoring = ScoringEngine()
        # young→wait: отложенный v2-вход, даже если новых buy не будет
        self._insider_pending: dict[str, dict] = {}
        self._insider_pending_tasks: dict[str, asyncio.Task] = {}
        self._force_insider_mints: set[str] = set()

    async def start(self):
        self._lock = _SingleInstanceLock(settings.DATA_DIR / "sniper_bot.pid")
        if not self._lock.acquire():
            return

        logger.info("Запуск снайпинг-бота (DRY_RUN=%s, PID=%d)", settings.DRY_RUN, os.getpid())
        logger.info(
            "Режимы: ENTRY=%s ENFORCE_EDGE=%s WHALE_MIRROR=%s SCORE≥%.2f CLUSTER≥%d",
            settings.ENTRY_MODE, settings.ENFORCE_EDGE_GATE, settings.WHALE_MIRROR_EXIT,
            settings.SCORE_THRESHOLD, settings.MIN_CLUSTER_SIZE_TO_CONSIDER,
        )
        if not settings.DRY_RUN:
            problems = settings.validate_for_live_trading()
            if problems:
                logger.error("Реальная торговля отключена: %s", "; ".join(problems))
                logger.error("Установите DRY_RUN=true или исправьте .env, прежде чем продолжить")
                return

        await self._cache.connect()
        self._session = aiohttp.ClientSession()
        self._db = get_db()

        self._arkham = ArkhamClient(self._session)
        self._price_client = PriceClient(self._session)
        self._risk_manager = RiskManager(self._cache)
        self._executor = UnifiedExecutor(self._session)
        self._tx_parser = TxParser(self._session)
        self._smart_money = SmartMoneyFeed(self._cache, self._db)
        self._profiler = WalletProfiler(
            self._cache, self._arkham, db=self._db, smart_money_feed=self._smart_money,
        )
        self._wallet_screener = WalletScreener(self._session, self._cache)
        self._rug_checker = RugChecker(settings.helius_rpc())
        self._funding = FundingGraph(self._session, self._db)
        self._insider = InsiderDetector(self._funding)
        self._accum = AccumulationDetector()
        self._vol_spike = VolSpikeDetector()
        self._whale_sit = self._vol_spike  # legacy alias
        self._catalyst = CatalystProxy(self._session)
        self._telegram = TelegramBot(self._session, self._db)
        self._health = HealthMonitor(self._db, self._telegram)
        self._funding.attach_health(self._health)

        if settings.USE_WEBSOCKET_SCANNER:
            self._chain_scanner = WsScanner(self._session, self._tx_parser)
            self._chain_scanner._health = self._health  # noqa: SLF001
            logger.info("Источник событий: подписка Helius (задержка ~1 сек)")
        else:
            self._chain_scanner = ChainScanner(self._session, self._tx_parser)
            logger.info("Источник событий: опрос раз в %ds", settings.CHAIN_SCAN_POLL_INTERVAL_SEC)
        self._gmgn = GMGNClient()
        self._position_monitor = PositionMonitor(self._cache, self._price_client, self._executor)
        self._position_monitor.attach(db=self._db, telegram=self._telegram)

        self._telegram.set_providers(self._status_text, self._positions_text)
        self._dashboard_runner = None

        tasks = [
            asyncio.create_task(self._supervise(self._run_chain_scanner, "chain_scanner"), name="chain_scanner"),
            asyncio.create_task(self._supervise(self._run_outcome_resolver, "outcome_resolver"), name="outcome_resolver"),
            asyncio.create_task(self._supervise(self._position_monitor.run_forever, "position_monitor"),
                                name="position_monitor"),
            asyncio.create_task(self._supervise(self._health.run_forever, "health"), name="health"),
            asyncio.create_task(self._supervise(self._smart_money.run_forever, "smart_money"), name="smart_money"),
            asyncio.create_task(self._supervise(self._telegram.run_forever, "telegram"), name="telegram"),
            asyncio.create_task(self._supervise(self._runtime_publisher, "runtime_publisher"),
                                name="runtime_publisher"),
        ]
        if settings.DASHBOARD_ENABLED:
            tasks.append(asyncio.create_task(
                self._supervise(self._run_dashboard, "dashboard"), name="dashboard",
            ))
        if settings.GMGN_WATCH_WALLETS.strip():
            tasks.append(asyncio.create_task(self._supervise(self._run_gmgn, "gmgn"), name="gmgn"))
        else:
            logger.info("GMGN_WATCH_WALLETS не задан — источник GMGN пропущен, работаем через ChainScanner")
        if settings.WHALE_SIT_SHADOW_ENABLED and settings.WHALE_SIT_POLLER_ENABLED:
            self._vol_spike_poller = VolSpikePoller(self._on_vol_spike_poll_signal)
            tasks.append(asyncio.create_task(
                self._supervise(self._vol_spike_poller.run_forever, "vol_spike_poller"),
                name="vol_spike_poller",
            ))
            logger.info(
                "Vol-spike: poller ON (graduates Dex, track=%s), WS path=%s",
                VOL_SPIKE_LAB,
                "on" if settings.WHALE_SIT_WS_ENABLED else "off",
            )
        if settings.ACCUM_BAL_ENABLED:
            self._accum_bal_poller = AccumBalancePoller(self._on_accum_balance_window)
            tasks.append(asyncio.create_task(
                self._supervise(self._accum_bal_poller.run_forever, "accum_balance_poller"),
                name="accum_balance_poller",
            ))
            logger.info(
                "Accum-balance: poller ON track=%s snap=%ds outcome=%dmin",
                ACCUM_BAL_LAB, settings.ACCUM_BAL_SNAP_SEC, settings.ACCUM_BAL_OUTCOME_DELAY_MIN,
            )

        try:
            await asyncio.gather(*tasks)
        finally:
            if self._dashboard_runner is not None:
                try:
                    await self._dashboard_runner.cleanup()
                except Exception:  # noqa: BLE001
                    pass
            await self._session.close()
            await self._cache.close()
            self._lock.release()

    async def _run_dashboard(self):
        try:
            self._dashboard_runner = await start_dashboard()
        except OSError as exc:
            logger.error("Dashboard не поднялся (%s) — бот продолжает без UI", exc)
            while True:
                await asyncio.sleep(3600)
            return
        while True:
            await asyncio.sleep(3600)

    async def _runtime_publisher(self):
        while True:
            try:
                positions = await self._cache.get_open_positions()
                set_runtime(
                    health=self._health,
                    smart_money_count=len(self._smart_money.wallets),
                    dry_run=settings.DRY_RUN,
                    paused=self._telegram.paused,
                    open_positions=positions,
                )
            except Exception:  # noqa: BLE001
                logger.debug("runtime publisher hiccup", exc_info=True)
            await asyncio.sleep(5)

    async def _status_text(self) -> str:
        snap = self._health.snapshot()
        summary = self._db.summary()
        positions = await self._cache.get_open_positions()
        return (
            f"DRY_RUN={settings.DRY_RUN} paused={self._telegram.paused}\n"
            f"WS gap={snap.get('ws_gap_sec')}s reconnects={snap.get('ws_reconnects')}\n"
            f"helius429={snap.get('helius_429')} parsed={snap.get('scanner_parsed')}\n"
            f"positions={len(positions)} smart_money={len(self._smart_money.wallets)}\n"
            f"db signals={summary.get('signals')} trades={summary.get('trades')} "
            f"outcomes={summary.get('outcomes')}"
        )

    async def _positions_text(self) -> str:
        positions = await self._cache.get_open_positions()
        if not positions:
            return "Нет открытых позиций"
        lines = []
        for mint, pos in positions.items():
            lines.append(
                f"{mint[:8]}… whale={(pos.get('whale') or '')[:8]} "
                f"size={pos.get('remaining_sol', pos.get('size_sol'))} SOL"
            )
        return "\n".join(lines)

    async def _on_vol_spike_poll_signal(
        self,
        *,
        mint: str,
        price_usd: float,
        liquidity_usd: float,
        vol_m5: float,
        buys_m5: int,
        sells_m5: int = 0,
        avg_trade_usd: float = 0.0,
        reason: str,
    ) -> None:
        """Регистрация lab_vol_spike из Dex-поллера (разгон, не «кит сидит»)."""
        now = int(time.time())
        wallet = "dex_avg_trade"
        detail = (
            f"{reason} vol_m5=${vol_m5:.0f} avg=${avg_trade_usd:.0f} "
            f"buys={buys_m5} sells={sells_m5} liq=${liquidity_usd:.0f} grad=True"
        )
        buy = BuyEvent(
            signature=f"vol-spike-poll-{mint[:12]}-{now}",
            wallet=wallet,
            token_mint=mint,
            sol_amount=max(float(settings.WHALE_SIT_MIN_SOL), 2.0),
            source="vol_spike_poller",
            timestamp=now,
        )
        market_liq = float(liquidity_usd or 0)
        market_px = float(price_usd or 0)
        feats = {
            "vol_spike_reason": reason,
            "whale_sit_reason": reason,  # legacy key in pending/features
            "mint_age_sec": None,
            "liquidity_usd": market_liq,
            "is_graduated": True,
            "vol_m5_usd": vol_m5,
            "buys_m5": buys_m5,
            "sells_m5": sells_m5,
            "avg_trade_usd": round(avg_trade_usd, 2),
            "score": None,
        }
        await self._profiler.register_early_buy(
            buy, market_liq, market_px,
            features=feats,
            lab_track=VOL_SPIKE_LAB,
            outcome_delay_min=settings.WHALE_SIT_OUTCOME_DELAY_MIN,
        )
        self._db.log_signal(
            token_mint=mint, wallet=wallet, sol_amount=buy.sol_amount,
            score=0.0, passed=True,
            features={k: v for k, v in feats.items() if k != "score"},
            decision="vol_spike_shadow",
            reason=detail,
        )
        self._vol_spike.mark_emitted(mint, float(now))
        self._vol_spike.prune()
        logger.info("VOL_SPIKE poller shadow %s: %s", mint[:8], detail)

    async def _on_accum_balance_window(self, obs: WindowObs) -> None:
        """Каждое полное не-void окно → pending outcome (сигнал и контроль)."""
        if obs.void:
            return
        wallet = "accum_bal_sig" if obs.is_signal else "accum_bal_ctrl"
        buy = BuyEvent(
            signature=f"accum-bal-{obs.mint[:12]}-{int(obs.ts)}",
            wallet=wallet,
            token_mint=obs.mint,
            sol_amount=float(settings.ACCUM_BAL_POSITION_SOL),
            source="accum_balance_poller",
            timestamp=int(obs.ts),
        )
        feats = {
            "is_signal": obs.is_signal,
            "conc_now": obs.conc_now,
            "conc_prev": obs.conc_prev,
            "d_conc_pct": round(obs.d_conc_pct, 6),
            "d_conc_usd": round(obs.d_conc_usd, 2),
            "d_price_pct": round(obs.d_price_pct, 6),
            "liq_usd": obs.liq_usd,
            "top_n_used": obs.top_n_used,
            "excluded_accounts": obs.excluded_accounts,
            "monotone_ok": obs.monotone_ok,
            "window_snapshots": obs.window_snapshots,
            "window_hours": round(obs.window_hours, 3),
            "top_retention_min": round(obs.top_retention_min, 4),
            "top_retention_mean": round(obs.top_retention_mean, 4),
            "persist_share": round(obs.persist_share, 4),
            "d_persist": obs.d_persist,
            "d_in": obs.d_in,
            "d_out": obs.d_out,
            "top_floor": obs.top_floor,
            "holders_count": obs.holders_count,
            "pool_account": obs.pool_account,
            "supply": obs.supply,
            "score": None,
        }
        await self._profiler.register_early_buy(
            buy, obs.liq_usd, obs.price_now,
            features=feats,
            lab_track=ACCUM_BAL_LAB,
            outcome_delay_min=settings.ACCUM_BAL_OUTCOME_DELAY_MIN,
        )
        if obs.is_signal:
            self._db.log_signal(
                token_mint=obs.mint, wallet=wallet,
                sol_amount=buy.sol_amount, score=0.0, passed=True,
                features={k: v for k, v in feats.items() if k != "score"},
                decision="accum_balance_signal",
                reason=(
                    f"d_conc={obs.d_conc_pct:+.1%} d_usd=${obs.d_conc_usd:.0f} "
                    f"d_price={obs.d_price_pct:+.1%} liq=${obs.liq_usd:.0f}"
                ),
            )
            logger.info(
                "ACCUM_BAL signal %s: d_conc=%+.1f%% d_usd=$%.0f d_price=%+.1f%% liq=$%.0f",
                obs.mint[:8], obs.d_conc_pct * 100, obs.d_conc_usd,
                obs.d_price_pct * 100, obs.liq_usd,
            )

    async def _supervise(self, factory, name: str, restart_delay_sec: int = 15):
        """Держит фоновую задачу живой.

        Без этого любое необработанное исключение в одной задаче через
        asyncio.gather роняет весь бот — при работе без присмотра это
        означает, что одна случайная сетевая ошибка ночью останавливает
        всё до утра. Отмену (Ctrl+C) пропускаем наружу как есть."""
        while True:
            try:
                await factory()
                logger.warning("Задача %s завершилась сама — перезапуск через %ds", name, restart_delay_sec)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                logger.exception("Задача %s упала — перезапуск через %ds", name, restart_delay_sec)
            await asyncio.sleep(restart_delay_sec)

    async def _run_chain_scanner(self):
        # Обработка НЕ блокирует чтение WS: иначе при Helius 429 каждый
        # rug-check держит цикл на секунды, очередь растёт, процесс душится.
        sem = asyncio.Semaphore(4)
        inflight: set[asyncio.Task] = set()

        async def _run(coro):
            async with sem:
                await coro

        async for event in self._chain_scanner.stream():
            # backpressure: слишком много незавершённых — дропаем новые buy
            if len(inflight) >= 40 and isinstance(event, BuyEvent):
                continue
            if isinstance(event, BuyEvent):
                task = asyncio.create_task(_run(self._handle_buy_event(event)))
            elif isinstance(event, SellEvent):
                task = asyncio.create_task(_run(self._handle_sell_event(event)))
            else:
                continue
            inflight.add(task)
            task.add_done_callback(inflight.discard)

    async def _run_gmgn(self):
        async for signal in self._gmgn.stream():
            if not signal.signature:
                continue
            events = await self._tx_parser.parse_signatures([signal.signature])
            for event in events:
                await self._handle_buy_event(event)

    async def _handle_sell_event(self, sell: SellEvent):
        """Если продавец — это тот самый кит, чья покупка привела нас в эту
        позицию, зеркалим его выход: продаём немедленно, не дожидаясь
        срабатывания цифровых правил (стоп-лосс/тейк-профит/трейлинг)."""
        try:
            await self._handle_sell_event_inner(sell)
        except Exception:  # noqa: BLE001
            logger.exception("Необработанная ошибка при обработке продажи")

    async def _handle_sell_event_inner(self, sell: SellEvent):
        if not settings.WHALE_MIRROR_EXIT:
            return  # эксперимент: только TP/SL/trailing/max_hold
        positions = await self._cache.get_open_positions()
        pos = positions.get(sell.token_mint)
        if not pos or pos.get("whale") != sell.wallet:
            return  # либо нет открытой позиции по этому токену, либо продал не наш кит

        logger.info("Кит %s продаёт %s — зеркалим выход", sell.wallet, sell.token_mint)
        await self._position_monitor.exit_if_open(
            sell.token_mint, reason_code="whale_sell",
            reason=f"кит {sell.wallet} продал токен (сигнатура {sell.signature})",
        )

    async def _run_outcome_resolver(self):
        """Раз в OUTCOME_CHECK_INTERVAL_SEC проверяет, выстрелили ли токены,
        за ранними покупателями которых мы наблюдали, и обновляет репутацию
        кошельков — это и есть механизм автообнаружения инсайдеров."""
        while True:
            await asyncio.sleep(settings.OUTCOME_CHECK_INTERVAL_SEC)
            try:
                resolved = await self._profiler.resolve_due_outcomes(self._price_client)
                if resolved:
                    logger.info("Обновлены исходы по %d ранним покупкам (winrate кошельков пересчитан)", resolved)
            except Exception:  # noqa: BLE001
                logger.exception("Ошибка при проверке исходов сделок")

    def _schedule_insider_pending(self, buy: BuyEvent, insider, mint_age: float, min_age: int) -> None:
        """Запомнить кластер и войти в v2 через (min_age − age), даже без нового buy."""
        mint = buy.token_mint
        delay = max(1.0, float(min_age) - float(mint_age) + 2.0)  # +2с буфер
        self._insider_pending[mint] = {
            "wallet": buy.wallet,
            "sol_amount": float(buy.sol_amount or 0.2),
            "price_sol": buy.price_sol,
            "liquidity_sol": buy.liquidity_sol,
            "signature": buy.signature or f"pending-{mint[:8]}",
            "source": buy.source or "insider_pending",
            "cluster_size": int(insider.cluster_size) if insider else 0,
            "funder": (insider.funder if insider else "") or "",
            "reason": (insider.reason if insider else "") or "pending",
            "ready_in": delay,
            "scheduled_at": time.time(),
        }
        old = self._insider_pending_tasks.get(mint)
        if old and not old.done():
            old.cancel()
        task = asyncio.create_task(
            self._run_insider_pending(mint, delay),
            name=f"insider_pending_{mint[:8]}",
        )
        self._insider_pending_tasks[mint] = task
        logger.info(
            "Insider PENDING %s: cluster≥%s → fire через %.0fs",
            mint[:8],
            (insider.cluster_size if insider else "?"),
            delay,
        )

    async def _run_insider_pending(self, mint: str, delay: float) -> None:
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            self._insider_pending.pop(mint, None)
            self._insider_pending_tasks.pop(mint, None)
            raise

        pend = self._insider_pending.pop(mint, None)
        self._insider_pending_tasks.pop(mint, None)
        if not pend:
            return

        try:
            open_positions = await self._cache.get_open_positions()
            if mint in open_positions or mint in self._buying:
                logger.info("Insider PENDING %s: уже позиция/покупка — skip", mint[:8])
                return

            # Возраст с first_seen; если touch потерян после рестарта — форсим ≥ min.
            age = self._accum.touch(mint)
            min_age = max(0, int(settings.INSIDER_MIN_MINT_AGE_SEC))
            if age < min_age:
                # first_seen мог сброситься — всё равно даём войти (таймер уже выждал)
                logger.info(
                    "Insider PENDING %s: touch_age=%.0fs < %ds после sleep — всё равно fire",
                    mint[:8], age, min_age,
                )

            buy = BuyEvent(
                signature=str(pend.get("signature") or f"pending-{mint[:8]}"),
                wallet=str(pend["wallet"]),
                token_mint=mint,
                sol_amount=max(float(pend.get("sol_amount") or 0.2), 0.2),
                source="insider_pending",
                timestamp=int(time.time()),
                price_sol=pend.get("price_sol"),
                liquidity_sol=pend.get("liquidity_sol"),
            )
            self._force_insider_mints.add(mint)
            logger.info(
                "Insider PENDING FIRE %s (waited %.0fs, cluster=%s funder=%s…)",
                mint[:8],
                delay,
                pend.get("cluster_size"),
                str(pend.get("funder") or "")[:8],
            )
            self._db.log_signal(
                token_mint=mint, wallet=buy.wallet, sol_amount=buy.sol_amount,
                score=0.0, passed=True,
                features={
                    "entry_via": "insider",
                    "pending_fire": True,
                    "cluster_size": pend.get("cluster_size"),
                    "funder": pend.get("funder"),
                    "experiment_phase": settings.INSIDER_EXPERIMENT_PHASE,
                },
                decision="insider_pending_fire",
                reason=str(pend.get("reason") or "pending"),
            )
            await self._handle_buy_event(buy)
        except Exception:  # noqa: BLE001
            self._force_insider_mints.discard(mint)
            logger.exception("Insider PENDING failed %s", mint[:8])

    async def _handle_buy_event(self, buy: BuyEvent):
        try:
            if buy.sol_amount < settings.MIN_BUY_SOL_TO_CONSIDER:
                return

            # --- Покупка самим девом своего только что созданного токена
            # (см. TxParser._is_creator_buy) — это НЕ инсайдерский сигнал:
            # у дева нет истории, а покупка собственного токена в той же
            # транзакции, что и его создание — стандартный первый шаг перед
            # накачкой и сливом на держателей. Полностью игнорируем: не
            # тратим на это ни скоринг, ни трекинг репутации.
            if buy.is_creator_buy:
                logger.debug("Пропуск %s: покупка дева собственного токена (создан и куплен в одной tx)",
                             buy.token_mint)
                return

            if getattr(self, "_telegram", None) is not None and self._telegram.paused:
                return

            # --- Скрининг кошелька: блок-лист + анти-спам ДО любых платных
            # запросов (Birdeye/Helius) — это самая дешёвая проверка, и она
            # не даёт шумовым/скам-кошелькам засорять кластеризацию и
            # статистику winrate.
            screen = await self._wallet_screener.screen(buy.wallet, buy.token_mint)
            if not screen.allowed:
                logger.debug("Пропуск кошелька %s: %s", buy.wallet, screen.reason)
                return

            # Live funding-граф / insider. При cooldown RPC работает из кэша
            # (пустой origin) — не отключаем детектор целиком.
            insider = None
            if settings.INSIDER_DETECT_ENABLED:
                try:
                    insider = await asyncio.wait_for(
                        self._insider.evaluate(
                            buy.token_mint, buy.wallet, buy.timestamp or None,
                        ),
                        timeout=3.0,
                    )
                except Exception:  # noqa: BLE001
                    logger.debug("Insider detect failed/timeout for %s", buy.wallet, exc_info=True)

            # Цена и ликвидность приезжают прямо в событии подписки —
            # запрос к кривой на этапе отбора не нужен. Это снимает по
            # одному сетевому запросу с КАЖДОГО события потока, а их
            # десятки в секунду.
            market = None
            if getattr(buy, "price_sol", None):
                sol_price_usd = await self._price_client.get_sol_price_usd()
                market = TokenMarketInfo(
                    mint=buy.token_mint,
                    price_usd=buy.price_sol * sol_price_usd,
                    liquidity_usd=(buy.liquidity_sol or 0) * sol_price_usd,
                    source="ws_event",
                    is_bonding_curve=True,
                )
            else:
                market = await self._price_client.get_market_info(buy.token_mint)

            if not market or market.liquidity_usd <= 0:
                logger.debug("Пропуск %s: нет данных о ликвидности", buy.token_mint)
                return

            is_new_token = await self._cache.is_new_mint(buy.token_mint, settings.MINT_SEEN_TTL_SEC)

            wallet_profile = await self._profiler.update_from_buy(buy)
            historical_winrate = float(wallet_profile.get("winrate", 0.5))
            is_proven_smart_money = bool(wallet_profile.get("is_proven_smart_money", False))
            if self._smart_money.contains(buy.wallet):
                is_proven_smart_money = True

            # Возраст с первого WS-buy (accum.touch) — не on-chain birth; сбрасывается
            # при рестарте. Graduates обходят age-gate через is_graduated().
            mint_age = self._accum.touch(buy.token_mint, buy.timestamp or None)
            sit_liq = float(market.liquidity_usd or 0)
            whale_sig = None
            if settings.WHALE_SIT_SHADOW_ENABLED and settings.WHALE_SIT_WS_ENABLED:
                # Для «истории» подтянуть Dex, если на кривой пул ещё мелкий, а mint уже старый/grad.
                if (
                    sit_liq < float(settings.WHALE_SIT_MIN_LIQ_USD)
                    and (
                        mint_age >= float(settings.WHALE_SIT_MIN_AGE_SEC)
                        or self._whale_sit.is_graduated(buy.token_mint)
                    )
                ):
                    try:
                        dex_m = await self._price_client.get_market_info(
                            buy.token_mint, with_activity=True,
                        )
                        if dex_m and float(dex_m.liquidity_usd or 0) > sit_liq:
                            sit_liq = float(dex_m.liquidity_usd or 0)
                            market = dex_m
                    except Exception:  # noqa: BLE001
                        pass

                whale_sig = self._whale_sit.observe(
                    buy.token_mint,
                    buy.wallet,
                    buy.sol_amount,
                    liquidity_usd=sit_liq,
                    is_smart_money=is_proven_smart_money,
                    mint_age_sec=mint_age,
                    ts=buy.timestamp or None,
                )
            if whale_sig is not None:
                await self._profiler.register_early_buy(
                    buy, market.liquidity_usd, market.price_usd,
                    features={
                        "vol_spike_reason": whale_sig.reason,
                        "whale_sit_reason": whale_sig.reason,
                        "mint_age_sec": round(whale_sig.mint_age_sec, 1),
                        "liquidity_usd": whale_sig.liquidity_usd,
                        "is_graduated": whale_sig.is_graduated,
                        "is_smart_money": is_proven_smart_money,
                        "score": None,
                    },
                    lab_track=VOL_SPIKE_LAB,
                    outcome_delay_min=settings.WHALE_SIT_OUTCOME_DELAY_MIN,
                )
                self._db.log_signal(
                    token_mint=buy.token_mint, wallet=buy.wallet, sol_amount=buy.sol_amount,
                    score=0.0, passed=True,
                    features={
                        "vol_spike_reason": whale_sig.reason,
                        "mint_age_sec": whale_sig.mint_age_sec,
                        "liquidity_usd": whale_sig.liquidity_usd,
                        "is_graduated": whale_sig.is_graduated,
                    },
                    decision="vol_spike_shadow",
                    reason=whale_sig.detail,
                )
                if settings.WHALE_SIT_BUY_ENABLED and settings.DRY_RUN:
                    logger.info(
                        "VolSpike PAPER buy candidate %s (%s)",
                        buy.token_mint[:8], whale_sig.reason,
                    )
                self._vol_spike.prune()

            # Lab-регистрация ПОСЛЕ cluster+score — иначе cluster_size/score
            # в outcomes уходят как None (старый дефект порядка).
            # observe() делаем сразу; запись исхода — ниже, с полными фичами.
            accum_sig = self._accum.observe(
                buy.token_mint, buy.wallet, buy.sol_amount, buy.timestamp or None,
            )

            cluster = await self._cache.register_token_buy(buy.token_mint, buy.wallet)

            sol_price_usd = await self._price_client.get_sol_price_usd()
            pool_share_pct = 0.0
            if market.liquidity_usd > 0:
                pool_share_pct = min((buy.sol_amount * sol_price_usd) / market.liquidity_usd, 1.0)

            features = EventFeatures(
                wallet_is_fresh=bool(wallet_profile.get("is_fresh", False)),
                liquidity_usd=market.liquidity_usd,
                pool_share_pct=pool_share_pct,
                cluster_size=len(cluster),
                is_smart_money=is_proven_smart_money,
                historical_winrate=historical_winrate,
                has_arkham_label=bool(wallet_profile.get("has_arkham_label", False)),
                insider_cluster=bool(insider and insider.is_insider_cluster),
                wallet_is_new=bool(insider and insider.is_fresh_wallet),
                insider_cluster_size=int(insider.cluster_size) if insider else 0,
                insider_score_boost=float(insider.score_boost) if insider else 0.0,
            )
            result = self._scoring.score(features)
            logger.info("Скоринг %s / кошелёк %s (%.3f SOL): %.3f (%s)%s",
                        buy.token_mint, buy.wallet, buy.sol_amount, result.score,
                        "ПРОХОДИТ" if result.passed_threshold else "не проходит",
                        f" | {insider.reason}" if insider else "")

            lab_common = {
                "wallet_age_days": screen.wallet_age_days,
                "mints_today": screen.mints_today,
                "is_bonding_curve": getattr(market, "is_bonding_curve", None),
                "price_source": getattr(market, "source", None),
                "cluster_size": len(cluster),
                "score": result.score,
                "insider_cluster": bool(insider and insider.is_insider_cluster),
                "insider_cluster_size": int(insider.cluster_size) if insider else 0,
                "wallet_is_new": bool(insider and insider.is_fresh_wallet),
                "insider_funder": (insider.funder if insider else "") or "",
            }

            # Обучение до торговых фильтров (не замыкать круг smart-money).
            if is_new_token:
                await self._profiler.register_early_buy(
                    buy, market.liquidity_usd, market.price_usd,
                    features=lab_common,
                    lab_track="lab_early",
                )

            if accum_sig is not None:
                catalyst_feats: dict = {}
                if accum_sig.mint_age_sec >= 1800:
                    catalyst_feats = await self._catalyst.enrich(buy.token_mint)
                else:
                    catalyst_feats = {"catalyst_skipped": True, "catalyst_reason": "mint_too_young"}
                await self._profiler.register_early_buy(
                    buy, market.liquidity_usd, market.price_usd,
                    features={
                        **lab_common,
                        "accum_reason": accum_sig.reason,
                        "mint_age_sec": round(accum_sig.mint_age_sec, 1),
                        "unique_wallets": accum_sig.unique_wallets,
                        "trigger_sol_total": accum_sig.trigger_sol_total,
                        "cluster_sol_total": accum_sig.cluster_sol_total,
                        **catalyst_feats,
                    },
                    lab_track="lab_accum",
                    outcome_delay_min=settings.ACCUM_OUTCOME_DELAY_MIN,
                )
                self._db.log_signal(
                    token_mint=buy.token_mint, wallet=buy.wallet, sol_amount=buy.sol_amount,
                    score=result.score, passed=True,
                    features={
                        "accum_reason": accum_sig.reason,
                        "mint_age_sec": accum_sig.mint_age_sec,
                        "unique_wallets": accum_sig.unique_wallets,
                        "cluster_size": len(cluster),
                        "score": result.score,
                        **catalyst_feats,
                    },
                    decision="accum_shadow",
                    reason=accum_sig.detail,
                )
                self._accum.prune()

            # Торговый фильтр кластера — ПОСЛЕ lab-записи
            # PENDING fire: co-buy redis может быть из 1 кошелька — не режем funding-кластер.
            pending_fire = buy.source == "insider_pending" or buy.token_mint in self._force_insider_mints
            min_cluster = settings.MIN_CLUSTER_SIZE_TO_CONSIDER
            if insider and insider.is_insider_cluster:
                min_cluster = min(min_cluster, settings.INSIDER_MIN_SHARED_FUNDER)
            if pending_fire:
                min_cluster = 1
            if len(cluster) < min_cluster:
                logger.debug("Пропуск %s: кластер из %d кошельков меньше требуемого %d",
                             buy.token_mint, len(cluster), min_cluster)
                return

            is_insider = bool(insider and insider.is_insider_cluster)
            if buy.token_mint in self._force_insider_mints:
                is_insider = True
                self._force_insider_mints.discard(buy.token_mint)
                logger.info("Insider %s: PENDING fire — форс entry_via=insider", buy.token_mint)

            mode = (settings.ENTRY_MODE or "hybrid").strip().lower()
            if mode not in ("score", "insider", "hybrid"):
                mode = "hybrid"

            entry_via: str | None = None
            if mode == "score":
                if result.passed_threshold:
                    entry_via = "score"
            elif mode == "insider":
                if is_insider:
                    entry_via = "insider"
            else:
                if is_insider:
                    entry_via = "insider"
                elif result.passed_threshold:
                    entry_via = "score"

            # Точка остановки insider-эксперимента (фаза v1/v2 — разные decision).
            # young → только ждём (чтобы v2 успел); demo — если после age рынок мёртвый.
            if entry_via == "insider":
                mint_age = self._accum.touch(buy.token_mint, buy.timestamp or None)
                min_age = max(0, int(settings.INSIDER_MIN_MINT_AGE_SEC))
                demo_reason = ""

                if mint_age < min_age and buy.source != "insider_pending":
                    logger.info(
                        "Insider %s: mint слишком молод %.0fs < %ds — pending v2",
                        buy.token_mint, mint_age, min_age,
                    )
                    self._db.log_signal(
                        token_mint=buy.token_mint, wallet=buy.wallet, sol_amount=buy.sol_amount,
                        score=result.score, passed=False,
                        features={
                            **(insider.as_features if insider else {}),
                            "entry_via": "insider",
                            "mint_age_sec": round(mint_age, 1),
                            "experiment_phase": settings.INSIDER_EXPERIMENT_PHASE,
                        },
                        decision="entry_reject_insider_young",
                        reason=f"mint_age {mint_age:.0f}s < {min_age}s",
                    )
                    self._schedule_insider_pending(buy, insider, mint_age, min_age)
                    return
                if mint_age < min_age and buy.source == "insider_pending":
                    logger.info(
                        "Insider %s: PENDING fire — age-gate skip (уже ждали)",
                        buy.token_mint,
                    )
                else:
                    # Живой buy после age — отменить таймер, чтобы не купить дважды
                    t = self._insider_pending_tasks.pop(buy.token_mint, None)
                    self._insider_pending.pop(buy.token_mint, None)
                    if t and not t.done():
                        t.cancel()

                if settings.INSIDER_REQUIRE_MARKET_ALIVE:
                    alive_market = await self._price_client.get_market_info(
                        buy.token_mint, with_activity=True,
                    )
                    if alive_market is not None:
                        if alive_market.liquidity_usd > market.liquidity_usd:
                            market = alive_market
                        elif alive_market.price_usd > 0 and market.price_usd <= 0:
                            market = alive_market
                    check = alive_market or market
                    curve_ok = False
                    if settings.INSIDER_CURVE_COUNTS_ALIVE:
                        src = str(getattr(check, "source", "") or "")
                        on_curve = bool(getattr(check, "is_bonding_curve", False)) or src in (
                            "ws_event", "pump_curve", "bonding_curve",
                        )
                        curve_ok = on_curve and float(getattr(check, "liquidity_usd", 0) or 0) > 0
                    if not check.market_alive() and not curve_ok:
                        logger.info(
                            "Insider %s: рынок не торгуется (нет txns/curve) — demo",
                            buy.token_mint,
                        )
                        self._db.log_signal(
                            token_mint=buy.token_mint, wallet=buy.wallet, sol_amount=buy.sol_amount,
                            score=result.score, passed=False,
                            features={
                                **(insider.as_features if insider else {}),
                                "entry_via": "insider",
                                "mint_age_sec": round(mint_age, 1),
                                "txns_m5": getattr(check, "txns_m5", None),
                                "txns_h1": getattr(check, "txns_h1", None),
                                "price_source": getattr(check, "source", None),
                                "experiment_phase": settings.INSIDER_EXPERIMENT_PHASE,
                            },
                            decision="entry_reject_untradeable",
                            reason="market_not_alive",
                        )
                        demo_reason = "untradeable"
                    elif curve_ok and not check.market_alive():
                        logger.info(
                            "Insider %s: Dex txns нет, но curve+liq — пускаем в v2",
                            buy.token_mint,
                        )

                if demo_reason:
                    can_demo = (
                        settings.DRY_RUN
                        and settings.INSIDER_DEMO_PAPER
                    )
                    if can_demo:
                        n_demo = self._db.count_signals("entry_ok_insider_demo")
                        if n_demo >= settings.INSIDER_DEMO_MAX_SIGNALS:
                            logger.info(
                                "Insider demo STOP: уже %d entry_ok_insider_demo (лимит %d)",
                                n_demo, settings.INSIDER_DEMO_MAX_SIGNALS,
                            )
                            return
                        logger.info(
                            "Insider DEMO paper %s: strict reject=%s → бумажная демка для анализа",
                            buy.token_mint, demo_reason,
                        )
                        entry_via = "insider_demo"
                    else:
                        return
                else:
                    decision_ok = settings.insider_experiment_decision
                    n_ins = self._db.count_signals(decision_ok)
                    if n_ins >= settings.INSIDER_EXPERIMENT_MAX_SIGNALS:
                        logger.info(
                            "Insider experiment STOP (%s): уже %d %s (лимит %d) — "
                            "новых бумажных входов нет, считайте lift",
                            settings.INSIDER_EXPERIMENT_PHASE,
                            n_ins, decision_ok, settings.INSIDER_EXPERIMENT_MAX_SIGNALS,
                        )
                        self._db.log_signal(
                            token_mint=buy.token_mint, wallet=buy.wallet, sol_amount=buy.sol_amount,
                            score=result.score, passed=False,
                            features={
                                "entry_via": "insider",
                                "experiment_n": n_ins,
                                "experiment_phase": settings.INSIDER_EXPERIMENT_PHASE,
                            },
                            decision="insider_experiment_done",
                            reason=f"limit {settings.INSIDER_EXPERIMENT_MAX_SIGNALS} {decision_ok}",
                        )
                        return

            if entry_via == "insider":
                exp_decision = settings.insider_experiment_decision
            elif entry_via == "insider_demo":
                exp_decision = "entry_ok_insider_demo"
            elif entry_via:
                exp_decision = "entry_ok_" + entry_via
            else:
                exp_decision = "entry_reject"
            self._db.log_signal(
                token_mint=buy.token_mint, wallet=buy.wallet, sol_amount=buy.sol_amount,
                score=result.score, passed=bool(entry_via),
                features={
                    **result.breakdown,
                    **(insider.as_features if insider else {}),
                    "entry_mode": mode,
                    "entry_via": entry_via or "",
                    "experiment_phase": (
                        settings.INSIDER_EXPERIMENT_PHASE
                        if entry_via in ("insider", "insider_demo") else ""
                    ),
                    "mint_age_sec": round(
                        self._accum.touch(buy.token_mint, buy.timestamp or None), 1
                    ) if entry_via in ("insider", "insider_demo") else None,
                },
                decision=exp_decision,
                reason=(insider.reason if insider else "") or mode,
            )

            if not entry_via:
                return

            # Порог безубыточности из геометрии кривой (core/edge_model.py).
            # Всегда логируем. Покупки:
            #   - live: edge обязателен всегда
            #   - DRY_RUN + ENFORCE_EDGE_GATE + score-путь: edge обязателен (честная бумага)
            #   - DRY_RUN + insider-путь: edge можно обойти — эксперимент гипотезы
            edge = edge_model.evaluate(
                (buy.liquidity_sol or 0.0) + edge_model.VSOL_START)
            logger.info(
                "Безубыточность %s: %s | ликв. %.1f SOL, нужна вероятность "
                "градации %.1f%% при базе %.2f%% → отбор должен поднимать ×%.1f "
                "(вход=%s)",
                buy.token_mint, "ПРОХОДИТ" if edge.passes else "ОТКАЗ",
                edge.detail["liquidity_sol"], edge.required_p * 100,
                edge.detail["base_rate"] * 100, edge.required_lift, entry_via)
            self._db.log_signal(
                token_mint=buy.token_mint, wallet=buy.wallet, sol_amount=buy.sol_amount,
                score=result.score, passed=edge.passes,
                features={
                    "entry_via": entry_via,
                    "required_lift": edge.required_lift,
                    "assumed_p": edge.assumed_p,
                    "required_p": edge.required_p,
                },
                decision="edge_pass" if edge.passes else "edge_fail",
                reason=edge.reason,
            )

            must_enforce_edge = (
                not settings.DRY_RUN
                or (settings.ENFORCE_EDGE_GATE and entry_via == "score")
            )
            if not edge.passes and must_enforce_edge:
                logger.info(
                    "Покупка %s заблокирована edge-gate (via=%s, dry=%s): %s",
                    buy.token_mint, entry_via, settings.DRY_RUN, edge.reason,
                )
                self._db.log_signal(
                    token_mint=buy.token_mint, wallet=buy.wallet, sol_amount=buy.sol_amount,
                    score=result.score, passed=False,
                    features={"entry_via": entry_via},
                    decision="edge_reject_trade",
                    reason=edge.reason,
                )
                return
            if not edge.passes and entry_via in ("insider", "insider_demo"):
                logger.info(
                    "Эксперимент insider %s (via=%s): edge ОТКАЗ, вход разрешён для измерения lift",
                    buy.token_mint, entry_via,
                )

            if entry_via == "insider":
                strategy_tag = settings.insider_strategy_tag
            elif entry_via == "insider_demo":
                strategy_tag = "insider_demo"
            else:
                strategy_tag = "score"
            # --- Точка невозврата перед реальными деньгами: две проверки,
            # которые не влияют на скоринг/обучение, а жёстко блокируют САМУ
            # ПОКУПКУ. Применяются только здесь (не на этапе трекинга выше),
            # потому что свежие Pump.fun токены по конструкции начинают с
            # низкой ликвидности — это нормально для наблюдения, но не для
            # входа реальными деньгами.
            # Грубый порог "токен вообще жив": ниже него в кривой почти нет
            # реального SOL — выйти будет не из чего независимо от размера позиции.
            # Для paper-demo и v2 на кривой порог мягче, иначе early mint всегда режется $300.
            min_liq = settings.MIN_LIQUIDITY_USD_TO_BUY
            if settings.DRY_RUN and entry_via in ("insider_demo", "insider"):
                min_liq = min(min_liq, float(settings.INSIDER_DEMO_MIN_LIQUIDITY_USD))
            if market.liquidity_usd < min_liq:
                logger.info("Пропуск покупки %s: ликвидность $%.0f ниже минимума $%.0f (via=%s)",
                            buy.token_mint, market.liquidity_usd, min_liq, entry_via)
                return

            if settings.RUG_CHECK_ENABLED:
                # При мёртвом Helius rug-check только жжёт таймауты и роняет бота
                helius_dead = (
                    not getattr(self._funding, "rpc_available", True)
                    or (self._health.snapshot().get("helius_429") or 0) >= 3
                )
                if helius_dead:
                    logger.debug("Rug-check пропущен (Helius недоступен) для %s", buy.token_mint)
                else:
                    rug_result = await self._get_cached_rug_check(buy.token_mint)
                    if rug_result.get("check_failed"):
                        logger.warning(
                            "Rug-check недоступен для %s (%s) — пропускаем проверку",
                            buy.token_mint, "; ".join(rug_result.get("issues") or []),
                        )
                    elif not rug_result["is_safe"]:
                        # В DRY paper (demo/v2) не блокируем на soft-fail RPC-шум уже выше;
                        # реальный скам-флаг всё ещё режет. На live — всегда.
                        if not settings.DRY_RUN or entry_via == "score":
                            logger.warning("Пропуск покупки %s: признаки скам-токена — %s",
                                            buy.token_mint, "; ".join(rug_result["issues"]))
                            return
                        logger.warning(
                            "Rug unsafe %s (via=%s, dry) — всё же пускаем paper: %s",
                            buy.token_mint, entry_via, "; ".join(rug_result["issues"]),
                        )

            # --- Не набирать одну и ту же монету дважды.
            #
            # Поймано на живых данных: два РАЗНЫХ кита купили один токен с
            # разницей в 246 мс, оба сигнала прошли фильтры, и бот открыл
            # позицию дважды. Запись позиции хранится по адресу токена, так
            # что вторая покупка затёрла первую: реальный объём получился
            # двойным, а в учёте осталась одна. Лимит MAX_POSITIONS_PER_WHALE
            # такую пару не ловит — киты разные.
            open_positions = await self._cache.get_open_positions()
            if buy.token_mint in open_positions:
                logger.debug("Пропуск %s: позиция по этой монете уже открыта", buy.token_mint)
                return
            if buy.token_mint in self._buying:
                logger.debug("Пропуск %s: покупка по этой монете уже выполняется", buy.token_mint)
                return

            decision = await self._risk_manager.evaluate(buy.wallet, result.score, market.liquidity_usd, sol_price_usd)
            if not decision.approved:
                logger.info("Риск-менеджмент отклонил сделку: %s", decision.reason)
                self._db.log_signal(
                    token_mint=buy.token_mint, wallet=buy.wallet, sol_amount=buy.sol_amount,
                    score=result.score, passed=False, features=result.breakdown,
                    decision="risk_reject", reason=decision.reason,
                )
                return

            # --- Исполнимость сделки: считается ТОЛЬКО здесь, потому что зависит
            # от размера позиции, который известен лишь после риск-менеджера.
            # Два независимых вопроса: сколько потеряем на входе и сможем ли выйти.
            if not self._is_trade_executable(buy.token_mint, market, decision.position_size_sol):
                return

            approved = await self._telegram.request_approve(
                token_mint=buy.token_mint,
                wallet=buy.wallet,
                sol_amount=decision.position_size_sol,
                score=result.score,
                detail=(insider.reason if insider else ""),
            )
            if not approved:
                self._db.log_signal(
                    token_mint=buy.token_mint, wallet=buy.wallet, sol_amount=decision.position_size_sol,
                    score=result.score, passed=False, features=result.breakdown,
                    decision="approve_reject", reason="telegram pause/timeout/reject",
                )
                return

            self._buying.add(buy.token_mint)
            try:
                exec_result = await self._executor.execute_buy(buy.token_mint, decision.position_size_sol)
            finally:
                self._buying.discard(buy.token_mint)

            if exec_result.success:
                # Цена входа с учётом проскальзывания: покупая, мы двигаем цену
                # вверх и получаем токены дороже спота. Записывать спот означало
                # бы завышать бумажный P&L и обманывать самих себя на этапе,
                # ради которого весь прогон и делается.
                entry_price = market.price_usd
                curve = getattr(market, "curve_state", None)
                if curve is not None:
                    slippage = curve.entry_slippage_pct(decision.position_size_sol)
                    entry_price = market.price_usd * (1 + slippage)

                logger.info("Сделка выполнена (dry_run=%s): %.4f SOL -> %s по $%.10f, tx=%s",
                            exec_result.dry_run, decision.position_size_sol, buy.token_mint,
                            entry_price, exec_result.signature)
                await self._cache.add_open_position(
                    buy.token_mint, buy.wallet, decision.position_size_sol,
                    entry_price_usd=entry_price,
                    strategy_tag=strategy_tag,
                )
                self._db.log_trade(
                    token_mint=buy.token_mint, whale=buy.wallet, side="buy",
                    size_sol=decision.position_size_sol, price_usd=entry_price,
                    signature=exec_result.signature or "", dry_run=bool(exec_result.dry_run),
                    reason_code=f"signal_{strategy_tag}",
                )
                # Исход для измерения lift: join по mint+wallet + lab_track
                if strategy_tag in ("insider_exp", "insider_exp_v2", "insider_demo"):
                    lab_track = (
                        "lab_insider_demo" if strategy_tag == "insider_demo"
                        else settings.insider_lab_track
                    )
                    await self._profiler.register_early_buy(
                        buy, market.liquidity_usd, entry_price,
                        features={
                            **lab_common,
                            **(insider.as_features if insider else {}),
                            "strategy_tag": strategy_tag,
                            "entry_via": entry_via,
                            "experiment_phase": settings.INSIDER_EXPERIMENT_PHASE,
                            "position_size_sol": decision.position_size_sol,
                        },
                        lab_track=lab_track,
                        outcome_delay_min=max(30, settings.OUTCOME_CHECK_DELAY_MIN),
                    )
                if self._telegram.enabled:
                    await self._telegram.send(
                        f"✅ Buy {buy.token_mint[:8]}… {decision.position_size_sol:.4f} SOL "
                        f"(dry={exec_result.dry_run}) score={result.score:.3f} tag={strategy_tag}"
                    )
            else:
                logger.error("Ошибка исполнения: %s", exec_result.error)
                if self._telegram.enabled:
                    await self._telegram.alert(f"Ошибка buy {buy.token_mint[:8]}…: {exec_result.error}")

        except Exception:  # noqa: BLE001
            logger.exception("Необработанная ошибка при обработке покупки")

    @staticmethod
    def _is_trade_executable(token_mint: str, market, position_sol: float) -> bool:
        """Проверяет, что сделку такого размера реально исполнить на этой кривой.

        Раньше вместо этого стоял единый порог "ликвидность > $3000", но он
        измерял не то: у Pump.fun проскальзывание определяется ВИРТУАЛЬНЫМИ
        резервами (старт ~30 SOL), а не реальными, поэтому вход на 0.1 SOL
        стоит доли процента даже у токена с $80 реальной ликвидности.
        Реальный SOL важен для другого — из него платят при выходе."""
        curve = getattr(market, "curve_state", None)
        if curve is None:
            return True  # цена пришла с обычного DEX — здесь эта математика неприменима

        slippage = curve.entry_slippage_pct(position_sol)
        if slippage > settings.MAX_ENTRY_SLIPPAGE_PCT:
            logger.info("Пропуск покупки %s: проскальзывание на входе %.2f%% выше лимита %.2f%%",
                        token_mint, slippage * 100, settings.MAX_ENTRY_SLIPPAGE_PCT * 100)
            return False

        exit_ratio = curve.exit_capacity_ratio(position_sol)
        if exit_ratio < settings.MIN_EXIT_LIQUIDITY_MULTIPLE:
            logger.info("Пропуск покупки %s: в кривой %.2f SOL — это лишь %.1fx позиции "
                        "(нужно %.0fx), выйти будет не из чего",
                        token_mint, curve.liquidity_sol, exit_ratio,
                        settings.MIN_EXIT_LIQUIDITY_MULTIPLE)
            return False

        logger.debug("%s исполнимо: проскальзывание %.2f%%, запас на выход %.1fx",
                     token_mint, slippage * 100, exit_ratio)
        return True

    async def _get_cached_rug_check(self, token_mint: str) -> dict:
        """Rug-check результат кэшируется на mint на 24 часа — mint/freeze
        authority и распределение держателей не меняются каждую минуту,
        а повторный запрос к Helius на каждую покупку того же токена был бы
        расточительным."""
        cached = await self._cache.get_mint_screen(token_mint)
        if cached is not None:
            return cached
        result = await self._rug_checker.full_check(token_mint)
        # Неудачную проверку НЕ кэшируем: иначе одна случайная сетевая
        # ошибка помечала бы монету небезопасной на целые сутки. Отличать
        # "монета плохая" от "проверить не удалось" здесь принципиально.
        if not result.get("check_failed"):
            await self._cache.set_mint_screen(token_mint, result)
        return result


def main():
    bot = SniperBot()
    try:
        asyncio.run(bot.start())
    except KeyboardInterrupt:
        logger.info("Остановлено пользователем")


if __name__ == "__main__":
    main()
