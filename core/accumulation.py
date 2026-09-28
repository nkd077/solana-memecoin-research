"""
Shadow-lab: накопление позиции (accumulation), не ранний снайп.

═══════════════════════════════════════════════════════════════════════
СПЕКА (v1)
═══════════════════════════════════════════════════════════════════════

Зачем
  Ранний вход на кривой требует огромного lift (DEMONSTRATED_LIFT).
  Альтернатива: кит(ы) набирают позицию со временем; вход/наблюдение
  ПОСЛЕ того, как mint уже «пожил», а не в секунду 0.

События (ончейн, из BuyEvent)
  A) repeat_whale — один wallet ≥ ACCUM_REPEAT_WALLET_BUYS покупок
     одного mint в окне, сумма SOL ≥ ACCUM_REPEAT_MIN_SOL_TOTAL
  B) multi_wallet — ≥ ACCUM_CLUSTER_WALLETS разных кошельков,
     каждый ≥ ACCUM_CLUSTER_MIN_SOL_EACH в окне

Общие фильтры
  • возраст mint (с первого увиденного buy) ≥ ACCUM_MIN_MINT_AGE_SEC
  • не creator_buy
  • один shadow-сигнал на mint (пока не истечёт окно)

Измерение (отдельно от lab_early)
  lab_track = "lab_accum"
  • НЕ пишет в baseline early
  • НЕ обновляет winrate кошелька (smart-money early)
  • исход через ACCUM_OUTCOME_DELAY_MIN → outcomes.jsonl + SQLite
  • paper buy только если ACCUM_BUY_ENABLED (по умолчанию false)

Катализатор (прокси, не Twitter)
  При shadow-сигнале опционально DexScreener token info
  (соцссылки / ликвидность пары) → features.catalyst_* .
  Полноценный Twitter API — этап 2, после ончейн lift.

═══════════════════════════════════════════════════════════════════════
"""
from __future__ import annotations

import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

from config import settings

logger = logging.getLogger("sniper.accumulation")


@dataclass
class _WalletAgg:
    buys: int = 0
    sol_total: float = 0.0
    last_ts: float = 0.0


@dataclass
class _MintState:
    first_seen: float
    wallets: dict[str, _WalletAgg] = field(default_factory=dict)
    emitted: bool = False
    pending_reason: Optional[str] = None
    pending_wallet: str = ""
    pending_detail: str = ""


@dataclass
class AccumSignal:
    mint: str
    wallet: str  # триггернувший кошелёк
    reason: str  # repeat_whale | multi_wallet
    mint_age_sec: float
    unique_wallets: int
    trigger_sol_total: float
    cluster_sol_total: float
    detail: str


class AccumulationDetector:
    def __init__(self):
        self._mints: dict[str, _MintState] = {}

    def touch(self, mint: str, ts: Optional[float] = None) -> float:
        """Всегда запоминает first_seen; возвращает возраст mint в секундах."""
        now = float(ts or time.time())
        st = self._mints.get(mint)
        if st is None:
            st = _MintState(first_seen=now)
            self._mints[mint] = st
            return 0.0
        return max(0.0, now - st.first_seen)

    def observe(self, mint: str, wallet: str, sol_amount: float,
                ts: Optional[float] = None) -> Optional[AccumSignal]:
        # Возраст трекаем всегда (нужен insider delayed-entry), даже если shadow выкл.
        age = self.touch(mint, ts)
        if not settings.ACCUM_SHADOW_ENABLED:
            return None
        if sol_amount <= 0:
            return None

        now = float(ts or time.time())
        st = self._mints[mint]

        if st.emitted:
            return None

        window = max(60, int(settings.ACCUM_WINDOW_SEC))

        agg = st.wallets.get(wallet) or _WalletAgg()
        if agg.last_ts and now - agg.last_ts > window:
            agg = _WalletAgg()
        agg.buys += 1
        agg.sol_total += sol_amount
        agg.last_ts = now
        st.wallets[wallet] = agg

        stale = [w for w, a in st.wallets.items() if now - a.last_ts > window]
        for w in stale:
            del st.wallets[w]

        # Условия могут выполниться ДО min age (вспышка на старте) —
        # запоминаем pending и эмитим, когда возраст догонит.
        repeat_ok = (
            agg.buys >= int(settings.ACCUM_REPEAT_WALLET_BUYS)
            and agg.sol_total >= float(settings.ACCUM_REPEAT_MIN_SOL_TOTAL)
        )
        min_each = float(settings.ACCUM_CLUSTER_MIN_SOL_EACH)
        strong = [w for w, a in st.wallets.items() if a.sol_total >= min_each]
        multi_ok = len(strong) >= int(settings.ACCUM_CLUSTER_WALLETS)

        if repeat_ok and not st.pending_reason:
            st.pending_reason = "repeat_whale"
            st.pending_wallet = wallet
            st.pending_detail = (
                f"wallet buys={agg.buys} sol={agg.sol_total:.2f} age={age/60:.1f}m"
            )
        elif multi_ok and not st.pending_reason:
            cluster_sol = sum(st.wallets[w].sol_total for w in strong)
            st.pending_reason = "multi_wallet"
            st.pending_wallet = wallet
            st.pending_detail = (
                f"strong_wallets={len(strong)} sol={cluster_sol:.2f} age={age/60:.1f}m"
            )

        if age < float(settings.ACCUM_MIN_MINT_AGE_SEC):
            return None
        if not st.pending_reason:
            return None

        st.emitted = True
        reason = st.pending_reason
        cluster_sol = sum(a.sol_total for a in st.wallets.values())
        trigger = st.wallets.get(st.pending_wallet) or agg
        sig = AccumSignal(
            mint=mint,
            wallet=st.pending_wallet or wallet,
            reason=reason,
            mint_age_sec=age,
            unique_wallets=len(st.wallets),
            trigger_sol_total=trigger.sol_total,
            cluster_sol_total=cluster_sol,
            detail=st.pending_detail + f" → emit@{age/60:.1f}m",
        )
        logger.info("ACCUM shadow %s: %s | %s", mint[:8], sig.reason, sig.detail)
        return sig

    def prune(self, max_mints: int = 5000):
        if len(self._mints) <= max_mints:
            return
        items = sorted(self._mints.items(), key=lambda kv: kv[1].first_seen)
        for mint, _ in items[: len(self._mints) - max_mints]:
            del self._mints[mint]
