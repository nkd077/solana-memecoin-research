"""
Shadow-lab: volume spike на graduates — не «кит сидит».

Что реально меряем (имя = содержание)
  Вход в момент крупного двустороннего объёма с крупной средней сделкой
  на уже ликвидном graduated токене. Это событие разгона (часто buys≫sells),
  а не состояние «кит держит позицию». Старое имя whale-sit оставлено только
  в legacy lab_track / void-метках.

Гипотеза (зафиксирована до данных)
  H1. Вход по volume spike (liq≥MIN, vol_m5≥MIN, avg_trade≥MIN) даёт
      honest med ≥ 0 на горизонте OUTCOME_DELAY (строгий exit-tradeable).
  Априор слабый/красный (graduates liq≥$5k: med ≈ −95% за ~3 дня;
      MELT: 60% ниже 20% цены миграции за 20 мин). Мерим всё равно.
  Выборка: n≥30 для уверенного отрицательного вывода; n≥100 для
      положительного. Два исхода — не вердикт.

H2 не собираем (контрольной группы нет).

Структурно
  Основной источник — поллер DexScreener по graduates_cohort.
  WS Pump выкл (сделки после кривой там не видны).
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Set

from config import settings

logger = logging.getLogger("sniper.vol_spike")

LAB_TRACK = "lab_vol_spike"
# Старые записи до переименования (не пыль) читаем вместе с LAB_TRACK.
LAB_TRACK_LEGACY = "lab_whale_sit"


@dataclass
class VolSpikeSignal:
    mint: str
    wallet: str
    reason: str  # smart_money | large_buy  (WS-путь, обычно выкл)
    mint_age_sec: float
    liquidity_usd: float
    sol_amount: float
    is_graduated: bool
    detail: str


@dataclass
class _MintState:
    first_seen: float
    emitted_at: float = 0.0


class VolSpikeDetector:
    """WS-опциональный детектор; основной путь — VolSpikePoller."""

    def __init__(self):
        self._mints: dict[str, _MintState] = {}
        self._grads: Set[str] = set()
        self._grads_mtime = 0.0
        self._reload_grads()

    def _reload_grads(self) -> None:
        path = Path(settings.DATA_DIR) / "graduates_cohort.json"
        try:
            if not path.exists():
                return
            mtime = path.stat().st_mtime
            if mtime == self._grads_mtime:
                return
            data = json.loads(path.read_text(encoding="utf-8"))
            self._grads = set(data.keys()) if isinstance(data, dict) else set(data)
            self._grads_mtime = mtime
            logger.info("VolSpike: graduates cohort loaded n=%d", len(self._grads))
        except Exception:  # noqa: BLE001
            logger.debug("VolSpike: не удалось загрузить graduates cohort", exc_info=True)

    def touch(self, mint: str, ts: Optional[float] = None) -> float:
        now = float(ts or time.time())
        st = self._mints.get(mint)
        if st is None:
            st = _MintState(first_seen=now)
            self._mints[mint] = st
            return 0.0
        return max(0.0, now - st.first_seen)

    def is_graduated(self, mint: str) -> bool:
        self._reload_grads()
        return mint in self._grads

    def mark_emitted(self, mint: str, ts: Optional[float] = None) -> None:
        now = float(ts or time.time())
        st = self._mints.get(mint)
        if st is None:
            st = _MintState(first_seen=now)
            self._mints[mint] = st
        st.emitted_at = now

    def observe(
        self,
        mint: str,
        wallet: str,
        sol_amount: float,
        *,
        liquidity_usd: float,
        is_smart_money: bool,
        mint_age_sec: Optional[float] = None,
        ts: Optional[float] = None,
    ) -> Optional[VolSpikeSignal]:
        if not settings.WHALE_SIT_SHADOW_ENABLED:
            return None
        if sol_amount <= 0:
            return None

        self._reload_grads()
        now = float(ts or time.time())
        tracked = self.touch(mint, now)
        age = float(mint_age_sec) if mint_age_sec is not None else tracked
        st = self._mints[mint]

        cooldown = max(300, int(settings.WHALE_SIT_COOLDOWN_SEC))
        if st.emitted_at and now - st.emitted_at < cooldown:
            return None

        is_grad = mint in self._grads
        min_age = float(settings.WHALE_SIT_MIN_AGE_SEC)
        if age < min_age and not is_grad:
            return None

        min_liq = float(settings.WHALE_SIT_MIN_LIQ_USD)
        if float(liquidity_usd or 0) < min_liq:
            return None

        reason = ""
        if is_smart_money:
            reason = "smart_money"
        elif sol_amount >= float(settings.WHALE_SIT_MIN_SOL):
            reason = "large_buy"
        else:
            return None

        st.emitted_at = now
        sig = VolSpikeSignal(
            mint=mint,
            wallet=wallet,
            reason=reason,
            mint_age_sec=age,
            liquidity_usd=float(liquidity_usd or 0),
            sol_amount=float(sol_amount),
            is_graduated=is_grad,
            detail=(
                f"{reason} sol={sol_amount:.2f} liq=${liquidity_usd:.0f} "
                f"age={age/3600:.1f}h grad={is_grad}"
            ),
        )
        logger.info("VOL_SPIKE shadow %s: %s", mint[:8], sig.detail)
        return sig

    def prune(self, max_mints: int = 8000) -> None:
        if len(self._mints) <= max_mints:
            return
        items = sorted(self._mints.items(), key=lambda kv: kv[1].first_seen)
        for mint, _ in items[: len(self._mints) - max_mints]:
            del self._mints[mint]


# Обратная совместимость импортов
WhaleSitDetector = VolSpikeDetector
WhaleSitSignal = VolSpikeSignal
