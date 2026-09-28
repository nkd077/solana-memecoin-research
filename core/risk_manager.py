"""
Управление рисками: размер позиции, лимиты одновременных сделок,
дневной стоп-лосс и ограничение доли ликвидности пула.
"""
from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass

from config import settings
from core.redis_cache import RedisCache

logger = logging.getLogger("sniper.risk_manager")


@dataclass
class RiskDecision:
    approved: bool
    reason: str
    position_size_sol: float = 0.0


class RiskManager:
    def __init__(self, cache: RedisCache):
        self._cache = cache

    @staticmethod
    def _today_key() -> str:
        return dt.date.today().isoformat()

    def size_position(self, score: float, liquidity_usd: float, sol_price_usd: float) -> float:
        """Линейно масштабирует размер позиции между MIN и MAX по скору,
        дополнительно ограничивая долей ликвидности пула."""
        span = settings.MAX_POSITION_SOL - settings.MIN_POSITION_SOL
        by_score = settings.MIN_POSITION_SOL + span * max(min(score, 1.0), 0.0)

        if sol_price_usd > 0 and liquidity_usd > 0:
            max_by_pool = (liquidity_usd * settings.MAX_POOL_SHARE_PCT) / sol_price_usd
            by_score = min(by_score, max_by_pool)

        return round(max(by_score, 0.0), 4)

    async def evaluate(self, whale_wallet: str, score: float, liquidity_usd: float,
                        sol_price_usd: float) -> RiskDecision:
        # 1. Дневной стоп-лосс (только для реальных денег).
        # В DRY_RUN его отключаем: иначе после −0.3 SOL бумаги бот встаёт
        # до полуночи и перестаёт копить статистику — как раз то, ради
        # чего бумажный режим и нужен.
        if not settings.DRY_RUN:
            pnl = await self._cache.get_daily_pnl(self._today_key())
            max_daily_loss = settings.DAILY_STOP_LOSS_PCT * settings.ACCOUNT_CAPITAL_SOL
            if pnl <= -abs(max_daily_loss):
                return RiskDecision(
                    False,
                    f"Достигнут дневной стоп-лосс ({pnl:.4f} SOL при лимите {max_daily_loss:.4f})",
                )

        # 2. Лимит одновременных позиций
        open_positions = await self._cache.get_open_positions()
        if len(open_positions) >= settings.MAX_OPEN_POSITIONS:
            return RiskDecision(False, "Достигнут лимит одновременных позиций")

        # 3. Лимит позиций на одного кита
        per_whale = sum(1 for p in open_positions.values() if p.get("whale") == whale_wallet)
        if per_whale >= settings.MAX_POSITIONS_PER_WHALE:
            return RiskDecision(False, "Достигнут лимит позиций на этого кита")

        # 4. Расчёт размера позиции
        size = self.size_position(score, liquidity_usd, sol_price_usd)
        if size < settings.MIN_POSITION_SOL:
            return RiskDecision(False, "Расчётный размер позиции ниже минимума")

        return RiskDecision(True, "OK", position_size_sol=size)
