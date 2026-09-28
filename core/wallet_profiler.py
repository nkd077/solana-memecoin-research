"""
Профилировщик кошельков: строит/обновляет профиль кита в кэше на основе
истории Arkham и последних сделок — используется скоринг-движком.
"""
from __future__ import annotations

import logging
import time

from config import settings
from core.arkham_client import ArkhamClient
from core.redis_cache import RedisCache
from core.tx_parser import BuyEvent

logger = logging.getLogger("sniper.wallet_profiler")

FRESHNESS_WINDOW_SEC = 3600          # смотрим на переводы за последний час
FRESHNESS_MIN_SOL = 5.0              # крупный перевод перед покупкой считается признаком "свежести"


class WalletProfiler:
    def __init__(self, cache: RedisCache, arkham: ArkhamClient, db=None, smart_money_feed=None):
        self._cache = cache
        self._arkham = arkham
        self._db = db
        self._smart_money_feed = smart_money_feed

    async def update_from_buy(self, buy: BuyEvent) -> dict:
        profile = await self._cache.get_wallet_profile(buy.wallet) or {
            "trades_seen": 0,
            "winrate": 0.5,
        }

        profile["last_trade_sol"] = buy.sol_amount
        profile["last_trade_at"] = buy.timestamp or int(time.time())
        profile["trades_seen"] = int(profile.get("trades_seen", 0)) + 1
        profile["is_fresh"] = await self._check_freshness(buy)

        label = await self._arkham.get_wallet_label(buy.wallet)
        profile["has_arkham_label"] = bool(label)

        await self._cache.set_wallet_profile(buy.wallet, profile)
        return profile

    async def register_early_buy(self, buy: BuyEvent, entry_liquidity_usd: float, entry_price_usd: float,
                                 features: dict | None = None,
                                 *,
                                 lab_track: str = "lab_early",
                                 outcome_delay_min: int | None = None):
        """Регистрирует наблюдение для отложенной проверки исхода.

        lab_track:
          lab_early — база + winrate кошелька (как раньше)
          lab_accum — shadow accumulation; не портит early-статистику
        """
        delay = outcome_delay_min if outcome_delay_min is not None else settings.OUTCOME_CHECK_DELAY_MIN
        due_at = time.time() + delay * 60
        feats = dict(features or {})
        feats["lab_track"] = lab_track
        record = {
            "wallet": buy.wallet,
            "token_mint": buy.token_mint,
            "entry_price_usd": entry_price_usd,
            "entry_liquidity_usd": entry_liquidity_usd,
            "due_at": due_at,
            "buy_sol_amount": buy.sol_amount,
            "source": buy.source,
            "is_creator_buy": buy.is_creator_buy,
            "lab_track": lab_track,
            **feats,
        }
        key = f"{lab_track}:{buy.token_mint}:{buy.wallet}:{int(buy.timestamp or time.time())}"
        # TTL с запасом: due_at + 2 суток. Иначе при delay=7д запись
        # протухает ровно на границе и молча выпадает из выборки.
        delay_sec = float(delay) * 60.0
        ttl_sec = int(delay_sec) + 2 * 24 * 3600
        await self._cache.add_pending_outcome(key, record, ttl_sec=ttl_sec)

    async def resolve_due_outcomes(self, price_client) -> int:
        """Проверяет исходы сделок, срок которых наступил.
        lab_early → baseline + winrate; lab_accum → только журнал."""
        due = await self._cache.get_due_outcomes(time.time())
        sol_usd = 0.0
        try:
            sol_usd = float(await price_client.get_sol_price_usd() or 0.0)
        except Exception:  # noqa: BLE001
            sol_usd = 0.0
        for key, record in due:
            # Пыль / старый buys-count критерий — не volume-spike гипотеза.
            track = record.get("lab_track") or ""
            if track in ("lab_vol_spike", "lab_whale_sit") and (
                record.get("whale_sit_reason") == "dex_volume_spike"
                or record.get("vol_spike_reason") == "dex_volume_spike"
                or (
                    record.get("avg_trade_usd") is not None
                    and float(record.get("avg_trade_usd") or 0)
                    < float(settings.WHALE_SIT_MIN_AVG_TRADE_USD)
                )
            ):
                await self._cache.remove_pending_outcome(key)
                logger.info(
                    "vol_spike dust void %s (reason=%s avg=%s)",
                    record.get("token_mint", "")[:8],
                    record.get("vol_spike_reason") or record.get("whale_sit_reason"),
                    record.get("avg_trade_usd"),
                )
                continue
            entry_price = record.get("entry_price_usd") or 0
            # with_activity: отличить «кривая котирует» от «можно продать»
            market = await price_client.get_market_info(
                record["token_mint"], with_activity=True,
            )

            if market and market.price_usd > 0 and entry_price > 0:
                growth = (market.price_usd - entry_price) / entry_price
                is_win = growth >= settings.OUTCOME_WIN_THRESHOLD_PCT
            else:
                is_win = False

            lab_track = record.get("lab_track") or "lab_early"
            if lab_track == "lab_early":
                await self._cache.add_baseline_outcome(is_win)
                await self._update_winrate(record["wallet"], is_win)
            self._log_outcome(record, is_win, market, sol_price_usd=sol_usd)
            await self._cache.remove_pending_outcome(key)

        return len(due)

    @staticmethod
    def _log_outcome(record: dict, is_win: bool, market, *, sol_price_usd: float = 0.0):
        """Пишет каждый отслеженный исход в data/outcomes.jsonl.

        growth — наивный (по котировке). honest_growth — −100%, если выход
        неисполним: мало txns или пул не поглощает позицию (не «≥1 сделка»).
        """
        import json as _json
        try:
            entry = record.get("entry_price_usd") or 0
            price = market.price_usd if market else 0.0
            growth = (price - entry) / entry if entry > 0 and price > 0 else None
            buy_sol = float(record.get("buy_sol_amount") or record.get("position_size_sol") or 0.0)
            if buy_sol <= 0:
                buy_sol = float(settings.MIN_POSITION_SOL)
            pos_usd = buy_sol * float(sol_price_usd or 0.0)
            if pos_usd <= 0:
                pos_usd = buy_sol * 150.0

            legacy_alive = bool(market.market_alive()) if market else False
            tradeable = bool(market.is_exit_tradeable(pos_usd)) if market else False
            if not tradeable:
                honest = -1.0
            elif growth is None:
                honest = -1.0
            else:
                honest = growth
            txns_m5 = market.txns_m5 if market else None
            txns_h1 = market.txns_h1 if market else None
            row = {
                "ts": time.time(),
                "wallet": record.get("wallet"),
                "mint": record.get("token_mint"),
                "entry_price_usd": entry,
                "exit_price_usd": price,
                "growth": round(growth, 6) if growth is not None else None,
                "honest_growth": round(honest, 6),
                "market_alive": tradeable,
                "market_alive_legacy": legacy_alive,
                "exit_tradeable": tradeable,
                "exit_position_usd": round(pos_usd, 4),
                "exit_liquidity_usd": getattr(market, "liquidity_usd", None) if market else None,
                "exit_price_source": getattr(market, "source", None) if market else None,
                "exit_txns_m5": txns_m5,
                "exit_txns_h1": txns_h1,
                "is_win": bool(is_win),
                "lab_track": record.get("lab_track") or "lab_early",
                "entry_liquidity_usd": record.get("entry_liquidity_usd"),
                "buy_sol_amount": record.get("buy_sol_amount"),
                "cluster_size": record.get("cluster_size"),
                "wallet_age_days": record.get("wallet_age_days"),
                "mints_today": record.get("mints_today"),
                "score": record.get("score"),
                "rug_safe": record.get("rug_safe"),
                "top_holder_share": record.get("top_holder_share"),
                "source": record.get("source"),
                "is_creator_buy": record.get("is_creator_buy"),
                "accum_reason": record.get("accum_reason"),
                "catalyst_has_twitter": record.get("catalyst_has_twitter"),
                "catalyst_has_social": record.get("catalyst_has_social"),
                "insider_cluster": record.get("insider_cluster"),
                "insider_cluster_size": record.get("insider_cluster_size"),
                "insider_funder": record.get("insider_funder"),
            }
            path = settings.DATA_DIR / "outcomes.jsonl"
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "a", encoding="utf-8") as f:
                f.write(_json.dumps(row, ensure_ascii=False) + "\n")
            try:
                from core.db import get_db
                get_db().log_outcome(
                    wallet=row["wallet"] or "",
                    token_mint=row["mint"] or "",
                    entry_price_usd=entry,
                    exit_price_usd=price,
                    growth=float(growth or 0),
                    is_win=bool(is_win),
                    features={k: row.get(k) for k in (
                        "lab_track", "entry_liquidity_usd", "buy_sol_amount", "cluster_size",
                        "wallet_age_days", "mints_today", "score", "source",
                        "accum_reason", "catalyst_has_twitter", "catalyst_has_social",
                        "insider_cluster", "insider_cluster_size", "insider_funder",
                        "market_alive", "market_alive_legacy", "exit_tradeable",
                        "honest_growth", "exit_txns_m5", "exit_txns_h1",
                        "exit_liquidity_usd", "exit_price_source", "exit_position_usd",
                    )},
                )
            except Exception:  # noqa: BLE001
                pass
        except Exception as exc:  # noqa: BLE001
            logger.debug("Не удалось записать исход в журнал: %s", exc)

    async def _update_winrate(self, wallet: str, is_win: bool):
        profile = await self._cache.get_wallet_profile(wallet) or {
            "wins": 0, "losses": 0, "trades_seen": 0, "winrate": 0.5,
        }
        profile["wins"] = int(profile.get("wins", 0)) + (1 if is_win else 0)
        profile["losses"] = int(profile.get("losses", 0)) + (0 if is_win else 1)
        total = profile["wins"] + profile["losses"]
        profile["winrate"] = profile["wins"] / total if total else 0.5
        # Смарт-мани — это превосходство над случайным выбором, а не высокий
        # винрейт сам по себе. На рынке, где в плюс выходит каждый десятый
        # токен, абсолютный порог отсекал вообще всех.
        baseline_rate, baseline_n = await self._cache.get_baseline_rate()
        if baseline_n >= 100 and baseline_rate > 0:
            required = max(baseline_rate * settings.SMART_MONEY_EDGE_MULTIPLE,
                           settings.SMART_MONEY_MIN_WINRATE)
        else:
            # базовой частоты ещё нет — держим абсолютный запасной порог
            required = settings.SMART_MONEY_MIN_WINRATE

        profile["baseline_at_check"] = round(baseline_rate, 4)
        profile["required_winrate"] = round(required, 4)
        profile["edge_vs_baseline"] = round(profile["winrate"] / baseline_rate, 2) if baseline_rate > 0 else None
        profile["is_proven_smart_money"] = (
            total >= settings.MIN_TRADES_FOR_TRUSTED_WINRATE
            and profile["winrate"] >= required
        )
        await self._cache.set_wallet_profile(wallet, profile, ttl_sec=30 * 24 * 3600)
        if profile["is_proven_smart_money"]:
            logger.info("Кошелёк %s подтверждён как смарт-мани: winrate=%.3f при базовой %.3f "
                        "(в %.1f раза лучше случайного), наблюдений=%d",
                        wallet, profile["winrate"], baseline_rate,
                        profile.get("edge_vs_baseline") or 0, total)
            if self._smart_money_feed is not None:
                try:
                    await self._smart_money_feed.note_proven(wallet, profile["winrate"], total)
                except Exception:  # noqa: BLE001
                    pass
            if self._db is not None:
                try:
                    self._db.upsert_smart_money(wallet, "proven", profile["winrate"], total)
                except Exception:  # noqa: BLE001
                    pass

        # Симметричная сторона того же механизма: кошелёк, накопивший
        # достаточно исходов, но со стабильно провальным winrate, отправляется
        # в блок-лист — дальше его сигналы не тратят ни скоринг, ни трекинг.
        # Это не наказание за неудачу как таковую, а признание, что кошелёк
        # либо не информирован, либо сам является частью скам-паттерна
        # (например, ранний покупатель собственных rug-токенов дева).
        if total >= settings.MIN_TRADES_FOR_TRUSTED_WINRATE and baseline_rate > 0 and profile["winrate"] < baseline_rate * 0.5:
            await self._cache.add_to_blocklist(
                wallet, reason=f"winrate={profile['winrate']:.3f} вдвое ХУЖЕ базовой {baseline_rate:.3f} на {total} наблюдениях",
            )
            logger.info("Кошелёк %s добавлен в блок-лист (winrate=%.2f на %d сделках)",
                        wallet, profile["winrate"], total)

    async def _check_freshness(self, buy: BuyEvent) -> bool:
        """Признак "свежего" кошелька: крупный входящий перевод незадолго
        до покупки (частый паттерн для новых кошельков-инсайдеров)."""
        history = await self._arkham.get_wallet_history(buy.wallet, limit=20)
        if not history:
            return False

        reference_ts = buy.timestamp or int(time.time())
        for transfer in history:
            try:
                ts = int(transfer.get("timestamp", 0))
                amount_sol = float(transfer.get("unitValue", transfer.get("value", 0)))
                is_incoming = transfer.get("toAddress") == buy.wallet
            except (TypeError, ValueError):
                continue
            if is_incoming and amount_sol >= FRESHNESS_MIN_SOL and 0 <= reference_ts - ts <= FRESHNESS_WINDOW_SEC:
                return True
        return False
