"""
Прокси-катализатор без Twitter: DexScreener token/pair info.

Не торговый сигнал сам по себе — обогащает lab_accum features
(есть ли сайт/твиттер у пары, ликвидность на DEX). Rate-limited.
"""
from __future__ import annotations

import logging
import time
from collections import deque
from typing import Any, Optional

import aiohttp

from config import settings

logger = logging.getLogger("sniper.catalyst")

DEX_TOKEN_URL = "https://api.dexscreener.com/latest/dex/tokens/{mint}"


class CatalystProxy:
    def __init__(self, session: aiohttp.ClientSession):
        self._session = session
        self._times: deque[float] = deque()

    def _allow(self) -> bool:
        if not settings.CATALYST_DEXSCREENER_ENABLED:
            return False
        now = time.time()
        while self._times and now - self._times[0] > 60:
            self._times.popleft()
        if len(self._times) >= max(1, int(settings.CATALYST_MAX_PER_MIN)):
            return False
        self._times.append(now)
        return True

    async def enrich(self, mint: str) -> dict[str, Any]:
        """Возвращает компактные features; пустой dict при skip/ошибке."""
        if not self._allow():
            return {"catalyst_skipped": True}

        url = DEX_TOKEN_URL.format(mint=mint)
        try:
            async with self._session.get(url, timeout=8) as resp:
                if resp.status != 200:
                    return {"catalyst_http": resp.status}
                data = await resp.json()
        except Exception as exc:  # noqa: BLE001
            logger.debug("DexScreener catalyst fail %s: %s", mint[:8], exc)
            return {"catalyst_error": type(exc).__name__}

        pairs = data.get("pairs") or []
        if not pairs:
            return {"catalyst_pairs": 0, "catalyst_has_social": False}

        # берём пару с макс. ликвидностью
        def liq(p: dict) -> float:
            try:
                return float((p.get("liquidity") or {}).get("usd") or 0)
            except (TypeError, ValueError):
                return 0.0

        best = max(pairs, key=liq)
        info = best.get("info") or {}
        socials = info.get("socials") or []
        websites = info.get("websites") or []
        has_twitter = any(
            (s.get("type") or "").lower() == "twitter" or "twitter" in str(s.get("url", "")).lower()
            for s in socials if isinstance(s, dict)
        )
        return {
            "catalyst_pairs": len(pairs),
            "catalyst_liq_usd": round(liq(best), 2),
            "catalyst_dex": best.get("dexId") or "",
            "catalyst_has_social": bool(socials or websites),
            "catalyst_has_twitter": has_twitter,
            "catalyst_boosts": int(best.get("boosts", {}).get("active") or 0)
            if isinstance(best.get("boosts"), dict) else 0,
        }
