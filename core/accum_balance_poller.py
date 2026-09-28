"""
Поллер lab_accum_balance: round-robin по graduates.

Не пытаемся снять всю когорту за один «тик» и уснуть на 6ч —
курсор сохраняется, каждый mint снимается не чаще SNAP_SEC,
за сутки покрываем всех, кто проходит liq-фильтр.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from pathlib import Path
from typing import Awaitable, Callable, Optional

import aiohttp

from config import settings
from core.accum_balance import (
    LAB_TRACK,
    AccumBalanceStore,
    WindowObs,
    compute_conc,
    evaluate_window,
    fetch_largest_accounts,
    fetch_supply,
    resolve_pool_account,
    top_retention,
)
from core.helius_gate import get_helius_gate

logger = logging.getLogger("sniper.accum_balance_poller")

DEX = "https://api.dexscreener.com/latest/dex/tokens/"
UA = "Mozilla/5.0 (compatible; sniper-accum-balance/1.0)"


class AccumBalancePoller:
    def __init__(self, on_window: Callable[[WindowObs], Awaitable[None]]):
        self._on_window = on_window
        self._store = AccumBalanceStore()
        self._cohort_mtime = 0.0
        self._all_mints: list[str] = []
        self._active_mints: list[str] = []
        self._cursor = self._store.load_cursor()
        self._sol_usd = 100.0
        self._sol_checked_at = 0.0
        self._dex_cache: dict[str, dict] = {}
        self._dex_cache_at = 0.0
        self._n_loop = 0
        self._rng = random.Random(42)

    def _reload_cohort(self) -> None:
        path = Path(settings.DATA_DIR) / "graduates_cohort.json"
        if not path.exists():
            self._all_mints = []
            self._active_mints = []
            return
        mtime = path.stat().st_mtime
        if mtime == self._cohort_mtime and self._all_mints and self._active_mints:
            return
        data = json.loads(path.read_text(encoding="utf-8"))
        self._all_mints = list(data.keys()) if isinstance(data, dict) else list(data)
        self._cohort_mtime = mtime
        self._refresh_active_set()
        if self._cursor >= len(self._active_mints):
            self._cursor = 0
        logger.info(
            "AccumBalancePoller: cohort n=%d active=%d cursor=%d",
            len(self._all_mints), len(self._active_mints), self._cursor,
        )

    def _refresh_active_set(self) -> None:
        cohort = set(self._all_mints)
        active_now = {mint for mint, st in self._store.iter_active_mints() if mint in cohort}
        self._active_mints = [m for m in self._all_mints if m in active_now]
        cap = max(1, int(settings.ACCUM_BAL_ACTIVE_CAP))
        free = max(0, cap - len(self._active_mints))
        if free <= 0:
            return
        candidates = []
        for mint in self._all_mints:
            if mint in active_now:
                continue
            st = self._store.peek(mint)
            if st and (st.admitted_ts or st.dropped_ts):
                continue
            candidates.append(mint)
        if not candidates:
            return
        picks = self._rng.sample(candidates, min(free, len(candidates)))
        now = time.time()
        for mint in picks:
            self._store.admit(mint, ts=now, reason="random_admit_free_slot")
            self._active_mints.append(mint)
        self._store.cursor = self._cursor
        self._store.save_meta()
        logger.info(
            "AccumBalancePoller admit: +%d active=%d/%d candidates_left=%d",
            len(picks), len(self._active_mints), cap, max(0, len(candidates) - len(picks)),
        )

    async def run_forever(self) -> None:
        snap_sec = max(300, int(settings.ACCUM_BAL_SNAP_SEC))
        batch = max(1, int(settings.ACCUM_BAL_BATCH_PER_LOOP))
        logger.info(
            "AccumBalancePoller up — round-robin snap≥%ds track=%s window=%d "
            "min_conc_usd=$%.0f d_conc_base_frac=%.2f",
            snap_sec, LAB_TRACK, settings.ACCUM_BAL_WINDOW_SNAPS,
            settings.ACCUM_BAL_MIN_CONC_USD, settings.ACCUM_BAL_MIN_D_CONC_BASE_FRAC,
        )
        async with aiohttp.ClientSession(headers={"User-Agent": UA, "Accept": "application/json"}) as session:
            while True:
                try:
                    if not settings.ACCUM_BAL_ENABLED:
                        await asyncio.sleep(30)
                        continue
                    self._reload_cohort()
                    if not self._active_mints:
                        await asyncio.sleep(60)
                        continue
                    await self._refresh_sol(session)
                    n_done = 0
                    n_skip = 0
                    # Один проход: до batch due-mint'ов, иначе короткий sleep
                    for _ in range(batch):
                        mint = self._next_due(snap_sec)
                        if mint is None:
                            n_skip += 1
                            break
                        wrote = await self._snap_one(session, mint, snap_sec)
                        if wrote is False:
                            # RPC/gate miss — вернуть mint в очередь, не ждать полный круг
                            n = len(self._active_mints)
                            if n:
                                self._cursor = (self._cursor - 1) % n
                            await asyncio.sleep(2)
                            break
                        n_done += 1
                        self._n_loop += 1
                        if self._n_loop % 25 == 0:
                            self._store.cursor = self._cursor
                            self._store.save_meta()
                            logger.info(
                                "AccumBalancePoller progress cursor=%d/%d active=%d unique_snaps≈%d",
                                self._cursor, len(self._active_mints), len(self._active_mints),
                                sum(1 for _, st in self._store.iter_mints() if st.snaps),
                            )
                    if n_done == 0:
                        await asyncio.sleep(15)
                except Exception:  # noqa: BLE001
                    logger.exception("AccumBalancePoller loop failed")
                    await asyncio.sleep(10)

    def _next_due(self, snap_sec: int) -> Optional[str]:
        """Следующий mint, которому пора снимок; двигает cursor по кругу."""
        n = len(self._active_mints)
        if n == 0:
            return None
        now = time.time()
        for _ in range(n):
            idx = self._cursor % n
            self._cursor = (idx + 1) % n
            mint = self._active_mints[idx]
            st = self._store.peek(mint)
            last = float(st.snaps[-1]["ts"]) if st and st.snaps else 0.0
            if last and now - last < snap_sec:
                continue
            return mint
        return None

    async def _refresh_sol(self, session: aiohttp.ClientSession) -> None:
        if time.time() - self._sol_checked_at < 600:
            return
        try:
            async with session.get(
                "https://api.dexscreener.com/latest/dex/tokens/"
                "So11111111111111111111111111111111111111112",
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    pairs = data.get("pairs") or []
                    if pairs:
                        self._sol_usd = float(pairs[0].get("priceUsd") or self._sol_usd)
            self._sol_checked_at = time.time()
        except Exception:  # noqa: BLE001
            pass

    async def _dex_fetch(
        self, session: aiohttp.ClientSession, mint: str,
    ) -> tuple[str, Optional[dict]]:
        """
        Возвращает (status, data):
          ok      — пара есть в этом ответе DexScreener
          missing — HTTP 200, но mint нет в pairs (→ dex_missing)
          error   — сеть/HTTP (→ retry, не подставлять старый кэш)

        Максимум liq берётся только внутри одного ответа; кэш перезаписывается,
        а не растет монотонно между запросами.
        """
        n = len(self._active_mints) or 1
        start = max(0, (self._cursor - 1) % n)
        chunk: list[str] = []
        for i in range(min(30, n)):
            chunk.append(self._active_mints[(start + i) % n])
        if mint not in chunk:
            chunk = [mint] + chunk[:29]
        url = DEX + ",".join(chunk)
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=25)) as resp:
                if resp.status != 200:
                    return "error", None
                data = await resp.json()
        except Exception:  # noqa: BLE001
            return "error", None

        batch_best: dict[str, dict] = {}
        for p in data.get("pairs") or []:
            base = ((p.get("baseToken") or {}).get("address")) or ""
            if not base:
                continue
            liq = float(((p.get("liquidity") or {}).get("usd")) or 0)
            price = float(p.get("priceUsd") or 0)
            prev = batch_best.get(base)
            if prev and liq < float(prev["liq_usd"]):
                continue
            batch_best[base] = {"liq_usd": liq, "price_usd": price}

        # Перезапись кэша значениями этого ответа (без max со старыми).
        self._dex_cache.update(batch_best)
        self._dex_cache_at = time.time()
        if mint in batch_best:
            return "ok", batch_best[mint]
        return "missing", None

    async def _snap_one(
        self, session: aiohttp.ClientSession, mint: str, snap_sec: int,
    ) -> Optional[bool]:
        """True=записали/ок, False=RPC/Dex error (retry), None=пропуск (liq drop)."""
        pos_usd = float(settings.ACCUM_BAL_POSITION_SOL) * self._sol_usd
        min_liq = pos_usd * float(settings.ACCUM_BAL_LIQ_MULTIPLE)
        min_conc_usd = float(settings.ACCUM_BAL_MIN_CONC_USD)
        d_frac = float(settings.ACCUM_BAL_MIN_D_CONC_BASE_FRAC)
        liq_drop_streak = max(1, int(settings.ACCUM_BAL_DROP_LIQ_STREAK))
        max_completed = max(1, int(settings.ACCUM_BAL_MAX_COMPLETED_WINDOWS))
        max_snaps_wo_window = max(1, int(settings.ACCUM_BAL_MAX_SNAPS_WITHOUT_WINDOW))
        need = int(settings.ACCUM_BAL_WINDOW_SNAPS)

        meta = self._store.get(mint)
        dex_status, dex_m = await self._dex_fetch(session, mint)
        if dex_status == "error":
            return False

        dex_missing = dex_status == "missing" or not dex_m
        liq_now = float((dex_m or {}).get("liq_usd") or 0)
        price_now = float((dex_m or {}).get("price_usd") or 0)

        if not dex_missing and liq_now < min_liq:
            meta.liq_below_streak += 1
            if meta.liq_below_streak >= liq_drop_streak and self._store.has_incomplete_window(meta):
                meta.pending_drop_reason = "liq_below_threshold_twice"
            elif meta.liq_below_streak >= liq_drop_streak:
                self._store.drop(mint, ts=time.time(), reason="liq_below_threshold_twice")
                self._active_mints = [m for m in self._active_mints if m != mint]
                self._refresh_active_set()
                self._store.cursor = self._cursor
                self._store.save_meta()
                return None
        elif not dex_missing:
            meta.liq_below_streak = 0

        accounts = await fetch_largest_accounts(session, mint)
        if accounts is None:
            return False

        if meta.supply is None:
            supply = await fetch_supply(session, mint)
            if supply is None:
                return False
            meta.supply = supply

        if not meta.pool_resolved:
            pool = await resolve_pool_account(session, mint, accounts)
            meta.pool_account = pool
            meta.pool_resolved = True

        conc, holders, excluded, top_accounts = compute_conc(
            accounts,
            pool_account=meta.pool_account,
            supply=meta.supply,
            top_n=int(settings.ACCUM_BAL_TOP_N),
            exclude_share=float(settings.ACCUM_BAL_EXCLUDE_SHARE),
        )
        now = time.time()
        prev_top = list(meta.snaps[-1].get("top_accounts") or []) if meta.snaps else []
        retention = top_retention(prev_top, top_accounts) if prev_top else None
        top_floor = min((bal for _, bal in top_accounts), default=0.0)
        snap = {
            "mint": mint,
            "ts": now,
            "conc": conc,
            "price_usd": price_now,
            "liq_usd": liq_now,
            "min_liq_usd": min_liq,
            "min_conc_usd": min_conc_usd,
            "min_d_conc_base_frac": d_frac,
            "top_n_used": holders,
            "holders_count": holders,
            "excluded": excluded,
            "top_accounts": top_accounts,  # [[addr, bal], ...] — полные в jsonl
            "top_floor": top_floor,
            "top_retention": retention,
            "pool_account": meta.pool_account,
            "supply": meta.supply,
            "dex_missing": dex_missing,
        }
        self._store.append_snap(snap)
        meta.snaps_since_admit += 1
        self._store.cursor = self._cursor
        self._store.save_meta()

        if len(meta.snaps) < need:
            return True

        obs = evaluate_window(meta.snaps, meta)
        if obs.void:
            self._store.append_window(obs)
            if meta.completed_windows == 0 and meta.snaps_since_admit >= max_snaps_wo_window:
                meta.pending_drop_reason = "no_window_after_n_snaps"
            if meta.pending_drop_reason:
                self._store.drop(mint, ts=now, reason=meta.pending_drop_reason)
                self._active_mints = [m for m in self._active_mints if m != mint]
                self._refresh_active_set()
            self._store.cursor = self._cursor
            self._store.save_meta()
            return True

        meta.completed_windows += 1
        if meta.completed_windows >= max_completed:
            meta.pending_drop_reason = "max_completed_windows"

        cooldown = max(3600, int(settings.ACCUM_BAL_COOLDOWN_SEC))
        if obs.is_signal:
            if meta.last_signal_at and now - meta.last_signal_at < cooldown:
                obs.is_signal = False
            else:
                meta.last_signal_at = now
                logger.info(
                    "ACCUM_BAL signal %s: d_conc=%+.1f%% d_usd=$%.0f d_price=%+.1f%% liq=$%.0f",
                    mint[:8], obs.d_conc_pct * 100, obs.d_conc_usd,
                    obs.d_price_pct * 100, obs.liq_usd,
                )

        self._store.append_window(obs)
        try:
            await self._on_window(obs)
        except Exception:  # noqa: BLE001
            logger.exception("on_window failed %s", mint[:8])
        if meta.pending_drop_reason:
            self._store.drop(mint, ts=now, reason=meta.pending_drop_reason)
            self._active_mints = [m for m in self._active_mints if m != mint]
            self._refresh_active_set()
        self._store.cursor = self._cursor
        self._store.save_meta()
        return True
