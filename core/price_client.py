"""
Клиент цен и безопасности токенов: Birdeye (приоритет, если задан API-ключ)
с фолбэком на бесплатный DexScreener API.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Optional

import aiohttp

from config import settings
from core.pump_curve import PumpCurveClient

logger = logging.getLogger("sniper.price_client")

# Цену SOL незачем спрашивать на каждый токен — она меняется медленно,
# а запросов при активном потоке сигналов десятки в минуту.
SOL_PRICE_CACHE_TTL_SEC = 60


@dataclass
class TokenMarketInfo:
    mint: str
    price_usd: float
    liquidity_usd: float
    pool_address: Optional[str] = None
    fdv_usd: Optional[float] = None
    is_honeypot_suspected: bool = False
    source: str = "dex"                 # dex | pump_curve — откуда взята цена
    is_bonding_curve: bool = False      # токен ещё не выпущен на Raydium
    curve_state: object = None          # CurveState, если цена взята с кривой
    # Активность с DexScreener (None = пары/данных нет). Нужна, чтобы
    # отличать «кривая ещё котирует цену» от «рынок жив, можно продать».
    txns_m5_buys: Optional[int] = None
    txns_m5_sells: Optional[int] = None
    txns_h1_buys: Optional[int] = None
    txns_h1_sells: Optional[int] = None
    volume_m5_usd: Optional[float] = None

    @property
    def txns_m5(self) -> Optional[int]:
        if self.txns_m5_buys is None and self.txns_m5_sells is None:
            return None
        return int(self.txns_m5_buys or 0) + int(self.txns_m5_sells or 0)

    @property
    def txns_h1(self) -> Optional[int]:
        if self.txns_h1_buys is None and self.txns_h1_sells is None:
            return None
        return int(self.txns_h1_buys or 0) + int(self.txns_h1_sells or 0)

    def market_alive(self) -> bool:
        """Слабый legacy-критерий: была хоть одна сделка за час.

        Не использовать для honest PnL — пул $18 с одной сделкой проходит,
        а позицию продать нельзя. См. is_exit_tradeable().
        """
        if self.price_usd <= 0:
            return False
        if (self.txns_m5_sells or 0) > 0 or (self.txns_m5_buys or 0) > 0:
            return True
        if (self.txns_h1_sells or 0) > 0 or (self.txns_h1_buys or 0) > 0:
            return True
        return False

    def is_exit_tradeable(
        self,
        position_usd: float,
        *,
        min_txns: int | None = None,
        min_liq_usd: float | None = None,
        exit_multiple: float | None = None,
    ) -> bool:
        """Можно ли выйти размером position_usd без обнуления пула.

        Требует: активность (≥N txns) + ликвидность пула ≥ max(min_liq,
        position × K). Для bonding curve дополнительно смотрит
        exit_capacity_ratio, если curve_state есть.
        """
        from config import settings as _s

        min_txns = int(min_txns if min_txns is not None else _s.OUTCOME_MIN_EXIT_TXNS)
        min_liq_usd = float(min_liq_usd if min_liq_usd is not None else _s.OUTCOME_MIN_EXIT_LIQUIDITY_USD)
        exit_multiple = float(
            exit_multiple if exit_multiple is not None else _s.OUTCOME_EXIT_LIQUIDITY_MULTIPLE
        )

        if self.price_usd <= 0:
            return False

        txns = 0
        if self.txns_m5_buys is not None or self.txns_m5_sells is not None:
            txns = int(self.txns_m5_buys or 0) + int(self.txns_m5_sells or 0)
        elif self.txns_h1_buys is not None or self.txns_h1_sells is not None:
            txns = int(self.txns_h1_buys or 0) + int(self.txns_h1_sells or 0)
        if txns < min_txns:
            return False

        liq = float(self.liquidity_usd or 0.0)
        if liq < min_liq_usd:
            return False
        pos = max(0.0, float(position_usd or 0.0))
        if pos > 0 and liq < pos * exit_multiple:
            return False

        curve = getattr(self, "curve_state", None)
        if curve is not None and pos > 0 and self.price_usd > 0:
            # грубо: position в SOL ≈ position_usd / (price не SOL). Лучше
            # передавать sol снаружи; здесь — только если кривая знает SOL.
            try:
                sol_pos = float(getattr(curve, "_last_position_sol", 0) or 0)
            except Exception:
                sol_pos = 0.0
            if sol_pos > 0:
                ratio = curve.exit_capacity_ratio(sol_pos)
                if ratio < float(_s.MIN_EXIT_LIQUIDITY_MULTIPLE):
                    return False
        return True


class PriceClient:
    def __init__(self, session: aiohttp.ClientSession):
        self._session = session
        self._curve_client = PumpCurveClient(session)
        self._sol_price_cache: tuple[float, float] = (0.0, 0.0)  # (цена, время)

    async def get_market_info(
        self, token_mint: str, *, with_activity: bool = False,
    ) -> Optional[TokenMarketInfo]:
        """Порядок источников: Birdeye -> DexScreener -> бондинг-кривая Pump.fun.

        Кривая идёт последней, но именно она закрывает главную дыру: пока
        токен не выпущен на Raydium (~$69k капитализации), ни Birdeye, ни
        DexScreener о нём не знают — а это ровно те ранние токены, ради
        которых бот и существует. Раньше такие сигналы просто отбрасывались
        с "нет данных о ликвидности".

        with_activity=True (только для resolve outcomes): дотягивает txns
        с DexScreener, даже если цена взята с кривой. На горячем пути не
        включать — лишний HTTP на каждый бай.
        """
        info = await self._from_birdeye(token_mint)
        if info and info.liquidity_usd > 0:
            return await self._maybe_activity(info, token_mint, with_activity)

        info = await self._from_dexscreener(token_mint)
        if info and info.liquidity_usd > 0:
            return info  # уже с txns

        # Важно: DEX-источники нередко отдают пару с нулевой ликвидностью
        # (устаревшая или пустая запись). Раньше такой ответ принимался как
        # валидный и до кривой дело не доходило — сделка отбрасывалась с
        # нулевой ликвидностью, хотя на кривой цена есть.
        curve_info = await self._from_pump_curve(token_mint)
        if curve_info:
            return await self._maybe_activity(curve_info, token_mint, with_activity)

        return await self._maybe_activity(info, token_mint, with_activity)

    async def _maybe_activity(
        self,
        info: Optional[TokenMarketInfo],
        token_mint: str,
        with_activity: bool,
    ) -> Optional[TokenMarketInfo]:
        if not with_activity or info is None:
            return info
        # Уже есть txns (цена с Dex) — не дёргаем второй раз.
        if info.txns_m5_buys is not None or info.txns_h1_buys is not None:
            return info
        dex = await self._from_dexscreener(token_mint)
        return self._merge_activity(info, dex)

    @staticmethod
    def _merge_activity(
        price_info: Optional[TokenMarketInfo],
        dex_info: Optional[TokenMarketInfo],
    ) -> Optional[TokenMarketInfo]:
        if price_info is None:
            return dex_info
        if dex_info is None:
            return price_info
        price_info.txns_m5_buys = dex_info.txns_m5_buys
        price_info.txns_m5_sells = dex_info.txns_m5_sells
        price_info.txns_h1_buys = dex_info.txns_h1_buys
        price_info.txns_h1_sells = dex_info.txns_h1_sells
        price_info.volume_m5_usd = dex_info.volume_m5_usd
        return price_info

    async def _from_pump_curve(self, token_mint: str) -> Optional[TokenMarketInfo]:
        if token_mint == settings.WSOL_MINT:
            return None  # для самого SOL кривой не существует

        sol_price = await self.get_sol_price_usd()
        state = await self._curve_client.get_curve_state(token_mint, sol_price)
        if not state or state.price_usd <= 0:
            return None

        logger.debug(
            "Цена из бондинг-кривой %s: $%.10f, ликвидность %.3f SOL ($%.0f), выпущен=%s",
            token_mint, state.price_usd, state.liquidity_sol, state.liquidity_usd, state.is_complete,
        )
        return TokenMarketInfo(
            mint=token_mint,
            price_usd=state.price_usd,
            liquidity_usd=state.liquidity_usd,
            pool_address=state.curve_address,
            source="pump_curve",
            is_bonding_curve=not state.is_complete,
            curve_state=state,
        )

    async def get_sol_price_usd(self) -> float:
        cached_price, cached_at = self._sol_price_cache
        if cached_price > 0 and (time.time() - cached_at) < SOL_PRICE_CACHE_TTL_SEC:
            return cached_price

        info = await self._from_dexscreener(settings.WSOL_MINT)
        if info and info.price_usd > 0:
            self._sol_price_cache = (info.price_usd, time.time())
            return info.price_usd

        if cached_price > 0:
            return cached_price  # API недоступен — лучше устаревшая цена, чем выдуманная
        return 150.0  # запасное значение на самый первый запрос

    async def _from_birdeye(self, token_mint: str) -> Optional[TokenMarketInfo]:
        if not settings.BIRDEYE_API_KEY:
            return None
        url = "https://public-api.birdeye.so/defi/token_overview"
        headers = {"X-API-KEY": settings.BIRDEYE_API_KEY, "x-chain": "solana"}
        try:
            async with self._session.get(url, params={"address": token_mint}, headers=headers, timeout=10) as resp:
                if resp.status != 200:
                    return None
                payload = (await resp.json()).get("data")
                if not payload:
                    return None
                return TokenMarketInfo(
                    mint=token_mint,
                    price_usd=float(payload.get("price") or 0),
                    liquidity_usd=float(payload.get("liquidity") or 0),
                    fdv_usd=float(payload.get("fdv") or 0) or None,
                )
        except Exception as exc:  # noqa: BLE001
            logger.debug("Birdeye недоступен для %s: %s", token_mint, exc)
            return None

    async def _from_dexscreener(self, token_mint: str) -> Optional[TokenMarketInfo]:
        url = f"{settings.DEXSCREENER_BASE_URL}/tokens/{token_mint}"
        try:
            async with self._session.get(url, timeout=10) as resp:
                if resp.status != 200:
                    return None
                payload = await resp.json()
        except Exception as exc:  # noqa: BLE001
            logger.debug("DexScreener недоступен для %s: %s", token_mint, exc)
            return None

        pairs = payload.get("pairs") or []
        if not pairs:
            return None
        best = max(pairs, key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0))
        liquidity_usd = float((best.get("liquidity") or {}).get("usd") or 0)
        txns = best.get("txns") or {}
        m5 = txns.get("m5") or {}
        h1 = txns.get("h1") or {}
        vol = best.get("volume") or {}
        return TokenMarketInfo(
            mint=token_mint,
            price_usd=float(best.get("priceUsd") or 0),
            liquidity_usd=liquidity_usd,
            pool_address=best.get("pairAddress"),
            fdv_usd=float(best.get("fdv") or 0) or None,
            is_honeypot_suspected=liquidity_usd < 1000,
            txns_m5_buys=int(m5.get("buys") or 0),
            txns_m5_sells=int(m5.get("sells") or 0),
            txns_h1_buys=int(h1.get("buys") or 0),
            txns_h1_sells=int(h1.get("sells") or 0),
            volume_m5_usd=float(vol.get("m5") or 0) or None,
        )
