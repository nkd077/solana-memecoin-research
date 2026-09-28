"""
Клиент Arkham Intelligence: метки кошельков (whale/institution/exchange)
и история их транзакций.
"""
from __future__ import annotations

import logging
from typing import Optional

import aiohttp

from config import settings

logger = logging.getLogger("sniper.arkham_client")

BASE_URL = "https://api.arkhamintelligence.com"


class ArkhamClient:
    def __init__(self, session: aiohttp.ClientSession):
        self._session = session

    def _headers(self) -> dict:
        return {"API-Key": settings.ARKHAM_API_KEY} if settings.ARKHAM_API_KEY else {}

    async def get_wallet_label(self, wallet: str) -> Optional[dict]:
        """Возвращает метку/сущность кошелька, если она известна Arkham."""
        if not settings.ARKHAM_API_KEY:
            return None
        url = f"{BASE_URL}/intelligence/address/{wallet}"
        try:
            async with self._session.get(url, headers=self._headers(), timeout=10) as resp:
                if resp.status != 200:
                    return None
                return await resp.json()
        except Exception as exc:  # noqa: BLE001
            logger.debug("Arkham недоступен для %s: %s", wallet, exc)
            return None

    async def get_wallet_history(self, wallet: str, limit: int = 50) -> list:
        """История транзакций кошелька (для расчёта winrate/свежести)."""
        if not settings.ARKHAM_API_KEY:
            return []
        url = f"{BASE_URL}/transfers"
        params = {"base": wallet, "limit": limit}
        try:
            async with self._session.get(url, headers=self._headers(), params=params, timeout=10) as resp:
                if resp.status != 200:
                    return []
                payload = await resp.json()
                return payload.get("transfers", [])
        except Exception as exc:  # noqa: BLE001
            logger.debug("Ошибка истории Arkham для %s: %s", wallet, exc)
            return []
