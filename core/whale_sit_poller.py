"""
Поллер volume-spike по graduates_cohort через DexScreener.

Не «кит сидит», а событие разгона: крупный m5-объём + крупная средняя
сделка на ликвидном graduated токене (часто buys≫sells).

Сигнал (Dex не даёт адрес кошелька и не делит volume на buy/sell):
  liq_usd ≥ WHALE_SIT_MIN_LIQ_USD
  и vol_m5 ≥ WHALE_SIT_MIN_VOL_M5_USD
  и avg_trade = vol_m5 / (buys_m5+sells_m5) ≥ WHALE_SIT_MIN_AVG_TRADE_USD

lab_track=lab_vol_spike. Только H1. n≥30 до отрицательного вердикта.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Callable, Awaitable

import aiohttp

from config import settings
from core.whale_sit import LAB_TRACK

logger = logging.getLogger("sniper.vol_spike_poller")

DEX = "https://api.dexscreener.com/latest/dex/tokens/"
UA = "Mozilla/5.0 (compatible; sniper-vol-spike/1.0)"


class VolSpikePoller:
    def __init__(self, on_signal: Callable[..., Awaitable[None]]):
        self._on_signal = on_signal
        self._prev: dict[str, dict] = {}
        self._emitted_at: dict[str, float] = {}
        self._cohort_mtime = 0.0
        self._feed_mtime = 0.0
        self._mints: list[str] = []

    def _maybe_refresh_cohort_from_feed(self) -> None:
        feed = Path(settings.DATA_DIR) / "graduations_feed.jsonl"
        if not feed.exists():
            return
        mtime = feed.stat().st_mtime
        if mtime == self._feed_mtime:
            return
        try:
            from research.track_graduates import build_cohort
            build_cohort()
            self._feed_mtime = mtime
            self._cohort_mtime = 0.0
            logger.info("VolSpikePoller: cohort rebuilt from graduations_feed")
        except Exception:  # noqa: BLE001
            logger.debug("VolSpikePoller: cohort rebuild failed", exc_info=True)

    def _reload_cohort(self) -> None:
        path = Path(settings.DATA_DIR) / "graduates_cohort.json"
        if not path.exists():
            self._mints = []
            return
        mtime = path.stat().st_mtime
        if mtime == self._cohort_mtime and self._mints:
            return
        data = json.loads(path.read_text(encoding="utf-8"))
        self._mints = list(data.keys()) if isinstance(data, dict) else list(data)
        self._cohort_mtime = mtime
        logger.info("VolSpikePoller: cohort n=%d", len(self._mints))

    async def run_forever(self) -> None:
        interval = max(60, int(settings.WHALE_SIT_POLL_SEC))
        logger.info(
            "VolSpikePoller up — every %ds, min_liq=$%.0f min_vol_m5=$%.0f min_avg=$%.0f track=%s",
            interval,
            settings.WHALE_SIT_MIN_LIQ_USD,
            settings.WHALE_SIT_MIN_VOL_M5_USD,
            settings.WHALE_SIT_MIN_AVG_TRADE_USD,
            LAB_TRACK,
        )
        async with aiohttp.ClientSession(headers={"User-Agent": UA, "Accept": "application/json"}) as session:
            while True:
                try:
                    await self._tick(session)
                except Exception:  # noqa: BLE001
                    logger.exception("VolSpikePoller tick failed")
                await asyncio.sleep(interval)

    async def _tick(self, session: aiohttp.ClientSession) -> None:
        if not settings.WHALE_SIT_SHADOW_ENABLED:
            return
        self._maybe_refresh_cohort_from_feed()
        self._reload_cohort()
        if not self._mints:
            return

        min_liq = float(settings.WHALE_SIT_MIN_LIQ_USD)
        min_vol = float(settings.WHALE_SIT_MIN_VOL_M5_USD)
        min_avg = float(settings.WHALE_SIT_MIN_AVG_TRADE_USD)
        cooldown = max(300, int(settings.WHALE_SIT_COOLDOWN_SEC))
        now = time.time()
        n_sig = 0

        for i in range(0, len(self._mints), 30):
            chunk = self._mints[i : i + 30]
            url = DEX + ",".join(chunk)
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=25)) as resp:
                    if resp.status != 200:
                        logger.debug("Dex batch HTTP %s", resp.status)
                        await asyncio.sleep(2)
                        continue
                    data = await resp.json()
            except Exception as exc:  # noqa: BLE001
                logger.debug("Dex batch err: %s", exc)
                await asyncio.sleep(2)
                continue

            best: dict[str, dict] = {}
            for p in data.get("pairs") or []:
                base = ((p.get("baseToken") or {}).get("address")) or ""
                if not base:
                    continue
                liq = float(((p.get("liquidity") or {}).get("usd")) or 0)
                prev = best.get(base)
                if prev and liq < prev["liq_usd"]:
                    continue
                m5 = (p.get("txns") or {}).get("m5") or {}
                vol_m5 = float(((p.get("volume") or {}).get("m5")) or 0)
                buys = int(m5.get("buys") or 0)
                sells = int(m5.get("sells") or 0)
                txns = buys + sells
                avg = (vol_m5 / txns) if txns > 0 else 0.0
                best[base] = {
                    "liq_usd": liq,
                    "price_usd": float(p.get("priceUsd") or 0),
                    "vol_m5": vol_m5,
                    "buys_m5": buys,
                    "sells_m5": sells,
                    "avg_trade_usd": avg,
                }

            for mint in chunk:
                cur = best.get(mint)
                if not cur:
                    continue
                self._prev[mint] = cur
                if cur["liq_usd"] < min_liq:
                    continue
                if cur["price_usd"] <= 0:
                    continue
                if cur["vol_m5"] < min_vol or cur["avg_trade_usd"] < min_avg:
                    continue
                last = self._emitted_at.get(mint, 0.0)
                if now - last < cooldown:
                    continue
                self._emitted_at[mint] = now
                n_sig += 1
                await self._on_signal(
                    mint=mint,
                    price_usd=cur["price_usd"],
                    liquidity_usd=cur["liq_usd"],
                    vol_m5=cur["vol_m5"],
                    buys_m5=cur["buys_m5"],
                    sells_m5=cur["sells_m5"],
                    avg_trade_usd=cur["avg_trade_usd"],
                    reason="dex_avg_trade",
                )

            await asyncio.sleep(1.2)

        if n_sig:
            logger.info("VolSpikePoller: emitted %d signals this tick", n_sig)


WhaleSitPoller = VolSpikePoller
