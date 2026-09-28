"""
Единый слой исполнения сделок.

PumpPortal умеет bonding curve (ранние токены) — основной путь.
Jupiter+Jito — запасной для уже выпущенных на DEX токенов, если Pump
не смог собрать/подтвердить транзакцию.

Публичный интерфейс один: execute_buy / execute_sell → ExecutionResult.
PositionMonitor и main больше не знают, какой бэкенд сработал.
"""
from __future__ import annotations

import logging
from typing import Optional

import aiohttp

from config import settings
from core.jito_executor import ExecutionResult, JitoExecutor
from core.pump_executor import PumpExecutor

logger = logging.getLogger("sniper.executor")


class UnifiedExecutor:
    def __init__(self, session: aiohttp.ClientSession):
        self._session = session
        self._jito = JitoExecutor(session)
        self._pump = PumpExecutor(session, getattr(self._jito, "_keypair", None))
        self.mode = (settings.EXECUTOR_MODE or "auto").strip().lower()

    @property
    def keypair(self):
        return getattr(self._jito, "_keypair", None)

    @property
    def pubkey(self) -> Optional[str]:
        return self._pump.pubkey

    async def execute_buy(self, token_mint: str, amount_sol: float) -> ExecutionResult:
        if self.mode == "jito":
            return await self._jito.execute_buy(token_mint, amount_sol)
        if self.mode == "pump":
            return await self._pump.execute_buy(token_mint, amount_sol)

        # auto: сначала Pump (кривая), при провале — Jupiter/Jito
        result = await self._pump.execute_buy(token_mint, amount_sol)
        if result.success or result.dry_run:
            return result

        logger.warning(
            "PumpExecutor не купил %s (%s) — пробуем Jupiter/Jito",
            token_mint, result.error,
        )
        fallback = await self._jito.execute_buy(token_mint, amount_sol)
        if fallback.success:
            return fallback
        return ExecutionResult(
            success=False,
            error=f"pump: {result.error}; jito: {fallback.error}",
        )

    async def execute_sell(self, token_mint: str, fraction: float = 1.0) -> ExecutionResult:
        if self.mode == "jito":
            return await self._jito.execute_sell(token_mint, fraction)
        if self.mode == "pump":
            return await self._pump.execute_sell(token_mint, fraction)

        result = await self._pump.execute_sell(token_mint, fraction)
        if result.success or result.dry_run:
            return result

        logger.warning(
            "PumpExecutor не продал %s (%s) — пробуем Jupiter/Jito",
            token_mint, result.error,
        )
        fallback = await self._jito.execute_sell(token_mint, fraction)
        if fallback.success:
            return fallback
        return ExecutionResult(
            success=False,
            error=f"pump: {result.error}; jito: {fallback.error}",
        )
