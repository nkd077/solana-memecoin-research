"""
Скрининг кошельков против скам/дев-паттернов.

Отдельно от скоринга (core/scoring.py): скоринг оценивает "насколько это
похоже на инсайдерский сигнал", а скрининг отвечает на более грубый вопрос
"стоит ли вообще доверять этому кошельку как источнику сигнала". Скрининг
может ЖЁСТКО заблокировать кошелёк (блок-лист, спам-паттерн) — тогда его
покупка не идёт ни в скоринг, ни в отслеживание исходов (чтобы не портить
статистику winrate шумом).

Возраст кошелька и паттерн "тратит только что полученные деньги" НЕ
блокируют сделку жёстко — это неоднозначный сигнал (может быть и реальный
инсайдер, использующий свежий/burner-кошелёк из соображений OPSEC, и
дев, финансирующий подставной кошелёк для wash-трейдинга). Такие кошельки
просто не могут получить статус "проверенного" смарт-мани, пока не
накопят историю.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field as dc_field

import aiohttp

from config import settings
from core.redis_cache import RedisCache

logger = logging.getLogger("sniper.wallet_screener")


@dataclass
class ScreenResult:
    allowed: bool                      # False = жёсткая блокировка, сигнал полностью игнорируется
    reason: str = ""                   # причина блокировки (для логов)
    wallet_age_days: float = 0.0
    is_established: bool = False       # кошелёк старше MIN_WALLET_AGE_DAYS
    mints_today: int = 0               # сколько разных новых токенов куплено сегодня
    is_spray_pattern: bool = False     # похоже на бота, покупающего всё подряд


class WalletScreener:
    def __init__(self, session: aiohttp.ClientSession, cache: RedisCache):
        self._session = session
        self._cache = cache

    async def screen(self, wallet: str, token_mint: str) -> ScreenResult:
        if not settings.WALLET_SCREENING_ENABLED:
            return ScreenResult(allowed=True)

        # 1. Жёсткий блок-лист — самая быстрая и дешёвая проверка
        if await self._cache.is_blocklisted(wallet):
            return ScreenResult(allowed=False, reason="кошелёк в блок-листе (ранее пойман на скам-паттерне)")

        # 2. Анти-спам: считаем, сколько РАЗНЫХ новых токенов кошелёк купил сегодня.
        # Реальный инсайдер селективен. Кошелёк, скупающий 20+ токенов в день,
        # либо бот, либо фермер объёма — его "победы" ничего не говорят про
        # реальную информированность, и такой шум только портит winrate-статистику.
        mints_today = await self._cache.register_mint_for_wallet_today(wallet, token_mint)
        is_spray_pattern = mints_today > settings.MAX_NEW_MINTS_PER_WALLET_PER_DAY
        if is_spray_pattern:
            return ScreenResult(
                allowed=False,
                reason=f"спрей-паттерн: {mints_today} разных новых токенов за сегодня (лимит {settings.MAX_NEW_MINTS_PER_WALLET_PER_DAY})",
                mints_today=mints_today,
                is_spray_pattern=True,
            )

        # 3. Возраст кошелька — не блокирует. По умолчанию БЕЗ Helius RPC
        # (limit=1000 на каждый новый адрес сжигал квоту). Берём birth из
        # funding-кэша; иначе 0 (= unknown / not established).
        cached = await self._cache.get_wallet_screen(wallet)
        if cached is not None:
            age_days = float(cached.get("wallet_age_days", 0.0))
        else:
            age_days = self._age_from_funding_cache(wallet)
            if age_days <= 0 and settings.WALLET_SCREEN_AGE_RPC:
                age_days = await self._estimate_wallet_age_days(wallet)
            await self._cache.set_wallet_screen(
                wallet, {"wallet_age_days": age_days, "checked_at": time.time()},
            )

        is_established = age_days >= settings.MIN_WALLET_AGE_DAYS

        return ScreenResult(
            allowed=True,
            wallet_age_days=age_days,
            is_established=is_established,
            mints_today=mints_today,
            is_spray_pattern=False,
        )

    @staticmethod
    def _age_from_funding_cache(wallet: str) -> float:
        try:
            from core.db import get_db
            row = get_db().get_wallet_funder(wallet)
            birth = row.get("birth_unix") if row else None
            if birth:
                return round((time.time() - float(birth)) / 86400, 2)
        except Exception:  # noqa: BLE001
            pass
        return 0.0

    async def _estimate_wallet_age_days(self, wallet: str) -> float:
        """Грубая оценка возраста через Helius (дорого). Вкл. только
        WALLET_SCREEN_AGE_RPC=true."""
        from core.helius_gate import get_helius_gate
        gate = get_helius_gate()
        if not gate.allow():
            return 0.0
        try:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getSignaturesForAddress",
                "params": [wallet, {"limit": 100}],  # было 1000 — хватает для «не свежий»
            }
            async with self._session.post(settings.helius_rpc(), json=payload, timeout=10) as resp:
                if resp.status == 429:
                    gate.note_429()
                    return 0.0
                if resp.status != 200:
                    return 0.0
                data = await resp.json()
                result = data.get("result") or []
                if not result:
                    return 0.0
                oldest = result[-1]
                block_time = oldest.get("blockTime")
                if not block_time:
                    return 0.0
                age_sec = time.time() - block_time
                return round(age_sec / 86400, 2)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Не удалось оценить возраст кошелька %s: %s: %s", wallet, type(exc).__name__, exc)
            return 0.0
