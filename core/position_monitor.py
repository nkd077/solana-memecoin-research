"""
Монитор открытых позиций и логика выхода из них.

Выход из позиции происходит одним из способов:

  1. "Зеркальный" выход — если кит, чья покупка привела нас в эту позицию,
     сам продаёт токен, мы продаём вслед за ним (см. exit_if_open(), вызывается
     из main.py::_handle_sell_event). Это основной, "инсайдерский" способ
     выхода — мы ориентируемся на действия кита, а не только на цифры.
  2. Стоп-лосс — жёсткая защита от провала, работает всегда, независимо от
     того, продал кит или нет.
  3. Лестница частичных фиксаций прибыли (PARTIAL_TP_1/2) — на кратных
     уровнях роста цены продаём ЧАСТЬ позиции, а не всё сразу. Остаток
     продолжает расти без потолка — так бот не обрубает сам себе потенциальный
     20x/100x, если кит продолжает держать.
  4. Трейлинг-стоп на остаток — включается только ПОСЛЕ первой частичной
     фиксации (до неё даём позиции пространство для роста без ранних выходов
     по небольшой просадке).
  5. Максимальное время удержания — подстраховка от токенов, которые не
     растут и не падают, а просто "зависают" без кита.

Работает и в DRY_RUN: реальная транзакция не отправляется, но "бумажный"
P&L всё равно считается, пишется в дневную статистику (core/redis_cache.py)
и в статистику сделок (core/stats.py) — так можно оценить стратегию ещё до
включения реальной торговли.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import logging
import time

from config import settings
from core.price_client import PriceClient
from core.redis_cache import RedisCache
from core.stats import TradeStats

logger = logging.getLogger("sniper.position_monitor")


class PositionMonitor:
    def __init__(self, cache: RedisCache, price_client: PriceClient, executor):
        """executor: UnifiedExecutor | PumpExecutor | JitoExecutor — любой с execute_sell."""
        self._cache = cache
        self._price_client = price_client
        self._executor = executor
        self._stats = TradeStats()
        self._exit_locks: dict[str, asyncio.Lock] = {}
        self._db = None
        self._telegram = None

    def attach(self, db=None, telegram=None):
        self._db = db
        self._telegram = telegram

    def _lock_for(self, mint: str) -> asyncio.Lock:
        lock = self._exit_locks.get(mint)
        if lock is None:
            lock = asyncio.Lock()
            self._exit_locks[mint] = lock
        return lock

    async def run_forever(self):
        while True:
            await asyncio.sleep(settings.POSITION_CHECK_INTERVAL_SEC)
            try:
                await self.check_positions_once()
            except Exception:  # noqa: BLE001
                logger.exception("Ошибка при проверке открытых позиций")

    async def check_positions_once(self):
        positions = await self._cache.get_open_positions()
        for mint in list(positions.keys()):
            await self._check_one(mint)

    async def exit_if_open(self, mint: str, reason_code: str, reason: str) -> bool:
        """Немедленно закрывает позицию по mint целиком, если она открыта —
        используется для "зеркального" выхода вслед за китом. Возвращает True,
        если позиция была открыта и закрылась."""
        async with self._lock_for(mint):
            positions = await self._cache.get_open_positions()
            pos = positions.get(mint)
            if not pos:
                return False
            market = await self._price_client.get_market_info(mint)
            exit_price = market.price_usd if market else 0.0
            await self._close_remaining(mint, pos, reason_code=reason_code, reason=reason, exit_price_usd=exit_price)
            return True

    async def _check_one(self, mint: str):
        async with self._lock_for(mint):
            positions = await self._cache.get_open_positions()
            pos = positions.get(mint)
            if not pos:
                return  # уже закрыта другим путём (например, зеркальным выходом) между snapshot и проверкой

            entry_price = float(pos.get("entry_price_usd") or 0)
            opened_at = float(pos.get("opened_at") or time.time())
            held_minutes = (time.time() - opened_at) / 60
            tag = str(pos.get("strategy_tag") or "legacy")
            is_insider_v2 = tag in ("insider_exp_v2", "insider_demo")

            sl_pct = settings.INSIDER_STOP_LOSS_PCT if is_insider_v2 else settings.STOP_LOSS_PCT
            tp1_mult = settings.INSIDER_PARTIAL_TP_1_MULTIPLE if is_insider_v2 else settings.PARTIAL_TP_1_MULTIPLE
            tp1_frac = settings.INSIDER_PARTIAL_TP_1_FRACTION if is_insider_v2 else settings.PARTIAL_TP_1_FRACTION
            tp2_mult = settings.INSIDER_PARTIAL_TP_2_MULTIPLE if is_insider_v2 else settings.PARTIAL_TP_2_MULTIPLE
            tp2_frac = settings.INSIDER_PARTIAL_TP_2_FRACTION if is_insider_v2 else settings.PARTIAL_TP_2_FRACTION
            trail_act = settings.INSIDER_TRAILING_ACTIVATE_PCT if is_insider_v2 else settings.TRAILING_ACTIVATE_PCT
            trail_stop = settings.INSIDER_TRAILING_STOP_PCT if is_insider_v2 else settings.TRAILING_STOP_PCT
            max_hold = settings.INSIDER_MAX_HOLD_MINUTES if is_insider_v2 else settings.MAX_HOLD_MINUTES

            market = await self._price_client.get_market_info(mint)

            if not market or market.price_usd <= 0:
                if held_minutes >= max_hold:
                    await self._close_remaining(mint, pos, reason_code="max_hold",
                                                 reason="нет данных о цене и превышено время удержания",
                                                 exit_price_usd=0.0)
                return

            if entry_price <= 0:
                return  # нечего сравнивать (не должно происходить в норме)

            current_price = market.price_usd
            highest = max(float(pos.get("highest_price_usd") or entry_price), current_price)
            if highest > float(pos.get("highest_price_usd") or 0):
                await self._cache.update_position_high(mint, highest)

            multiple = current_price / entry_price
            growth = multiple - 1
            peak_growth = (highest - entry_price) / entry_price
            drawdown_from_high = (current_price - highest) / highest if highest > 0 else 0.0

            # 1. Стоп-лосс — работает всегда, в любой фазе
            if growth <= -sl_pct:
                await self._close_remaining(mint, pos, reason_code="stop_loss",
                                             reason=f"стоп-лосс ({growth:.0%})", exit_price_usd=current_price)
                return

            # 2. Лестница частичных фиксаций прибыли
            if not pos.get("partial_tp_1_done") and multiple >= tp1_mult:
                await self._take_partial(mint, pos, fraction=tp1_frac,
                                          level_key="partial_tp_1_done", exit_price_usd=current_price,
                                          reason_code="partial_tp_1",
                                          reason=f"частичный тейк-профит x{tp1_mult:g}")
                return

            if pos.get("partial_tp_1_done") and not pos.get("partial_tp_2_done") \
                    and multiple >= tp2_mult:
                await self._take_partial(mint, pos, fraction=tp2_frac,
                                          level_key="partial_tp_2_done", exit_price_usd=current_price,
                                          reason_code="partial_tp_2",
                                          reason=f"частичный тейк-профит x{tp2_mult:g}")
                return

            # 3. Трейлинг-стоп на остаток — только после первой частичной фиксации
            if pos.get("partial_tp_1_done") and peak_growth >= trail_act \
                    and drawdown_from_high <= -trail_stop:
                await self._close_remaining(
                    mint, pos, reason_code="trailing_stop",
                    reason=f"трейлинг-стоп на остатке (пик x{highest / entry_price:.1f}, откат {drawdown_from_high:.0%})",
                    exit_price_usd=current_price,
                )
                return

            # 4. Максимальное время удержания
            if held_minutes >= max_hold:
                await self._close_remaining(mint, pos, reason_code="max_hold",
                                             reason=f"превышено макс. время удержания ({max_hold} мин)",
                                             exit_price_usd=current_price)

    async def _take_partial(self, mint: str, pos: dict, fraction: float, level_key: str,
                             exit_price_usd: float, reason_code: str, reason: str):
        entry_price = float(pos.get("entry_price_usd") or 0)
        remaining_sol = float(pos.get("remaining_sol", pos.get("size_sol") or 0))
        sold_sol = remaining_sol * fraction

        result = await self._executor.execute_sell(mint, fraction=fraction)
        if not result.success:
            logger.error("Не удалось частично продать %s (%s): %s", mint, reason, result.error)
            return

        pnl_sol = sold_sol * (exit_price_usd - entry_price) / entry_price if entry_price > 0 else 0.0
        new_remaining = max(remaining_sol - sold_sol, 0.0)

        today = dt.date.today().isoformat()
        await self._cache.add_daily_pnl(today, pnl_sol)
        await self._cache.update_position_partial(mint, remaining_sol=new_remaining, level_key=level_key)

        self._stats.record_exit(
            mint=mint, whale=pos.get("whale", ""), kind="partial", reason_code=reason_code, reason=reason,
            sold_sol=sold_sol, pnl_sol=pnl_sol, entry_price_usd=entry_price, exit_price_usd=exit_price_usd,
            opened_at=float(pos.get("opened_at") or time.time()), dry_run=result.dry_run, signature=result.signature,
            strategy_tag=str(pos.get("strategy_tag") or "legacy"),
        )
        if self._db is not None:
            multiple = (exit_price_usd / entry_price) if entry_price > 0 else 0.0
            self._db.log_trade(
                token_mint=mint, whale=pos.get("whale", ""), side="sell_partial",
                size_sol=sold_sol, price_usd=exit_price_usd, signature=result.signature or "",
                dry_run=bool(result.dry_run), reason_code=reason_code, pnl_sol=pnl_sol, multiple=multiple,
            )
        if self._telegram is not None and self._telegram.enabled:
            asyncio.create_task(self._telegram.send(
                f"📤 Partial {mint[:8]}… {reason_code} P&L={pnl_sol:+.4f} SOL"
            ))

        logger.info(
            "Частичный выход %s: %s, продано %.4f SOL (осталось %.4f), P&L=%+.4f SOL (%s)",
            mint, reason, sold_sol, new_remaining, pnl_sol,
            "DRY_RUN, бумажный расчёт" if result.dry_run else f"tx={result.signature}",
        )

    async def _close_remaining(self, mint: str, pos: dict, reason_code: str, reason: str, exit_price_usd: float):
        entry_price = float(pos.get("entry_price_usd") or 0)
        remaining_sol = float(pos.get("remaining_sol", pos.get("size_sol") or 0))

        result = await self._executor.execute_sell(mint, fraction=1.0)
        if not result.success:
            logger.error("Не удалось продать %s (причина выхода: %s): %s", mint, reason, result.error)
            return

        if entry_price > 0 and exit_price_usd > 0:
            pnl_sol = remaining_sol * (exit_price_usd - entry_price) / entry_price
        elif exit_price_usd <= 0:
            # нет цены (токен, вероятно, "умер") — считаем остаток полностью потерянным
            pnl_sol = -remaining_sol
        else:
            pnl_sol = 0.0

        today = dt.date.today().isoformat()
        await self._cache.add_daily_pnl(today, pnl_sol)

        self._stats.record_exit(
            mint=mint, whale=pos.get("whale", ""), kind="close", reason_code=reason_code, reason=reason,
            sold_sol=remaining_sol, pnl_sol=pnl_sol, entry_price_usd=entry_price, exit_price_usd=exit_price_usd,
            opened_at=float(pos.get("opened_at") or time.time()), dry_run=result.dry_run, signature=result.signature,
            strategy_tag=str(pos.get("strategy_tag") or "legacy"),
        )
        if self._db is not None:
            multiple = (exit_price_usd / entry_price) if entry_price > 0 else 0.0
            self._db.log_trade(
                token_mint=mint, whale=pos.get("whale", ""), side="sell",
                size_sol=remaining_sol, price_usd=exit_price_usd, signature=result.signature or "",
                dry_run=bool(result.dry_run), reason_code=reason_code, pnl_sol=pnl_sol, multiple=multiple,
            )
        if self._telegram is not None and self._telegram.enabled:
            asyncio.create_task(self._telegram.send(
                f"🏁 Close {mint[:8]}… {reason_code} P&L={pnl_sol:+.4f} SOL"
            ))

        await self._cache.remove_open_position(mint)

        logger.info(
            "Закрыта позиция %s: причина=%s, продано %.4f SOL, P&L=%+.4f SOL (%s)",
            mint, reason, remaining_sol, pnl_sol,
            "DRY_RUN, бумажный расчёт" if result.dry_run else f"tx={result.signature}",
        )
