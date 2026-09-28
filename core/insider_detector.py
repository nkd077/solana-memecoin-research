"""
Детектор инсайдерского паттерна: новый кошелёк + общий funder.

Путь входа в DRY_RUN оставлен для измерения lift. score_boost = 0:
знак признака не зашиваем — литература по координированным когортам
скорее предупреждает о dump, чем о alpha. Исход сам скажет.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from config import settings
from core.funding_graph import FundingGraph, WalletOrigin

logger = logging.getLogger("sniper.insider_detector")


@dataclass
class InsiderVerdict:
    is_insider_cluster: bool
    is_fresh_wallet: bool
    funder: Optional[str]
    cluster_size: int
    wallet_age_hours: Optional[float]
    score_boost: float
    reason: str

    @property
    def as_features(self) -> dict:
        return {
            "insider_cluster": self.is_insider_cluster,
            "insider_funder": self.funder or "",
            "insider_cluster_size": self.cluster_size,
            "wallet_is_new": self.is_fresh_wallet,
            "wallet_age_hours": self.wallet_age_hours,
            "insider_score_boost": self.score_boost,
        }


class InsiderDetector:
    def __init__(self, funding: FundingGraph):
        self._funding = funding

    async def evaluate(self, mint: str, wallet: str, at_ts: Optional[float] = None) -> InsiderVerdict:
        origin: WalletOrigin = await self._funding.register_buy(mint, wallet, at_ts)
        funder, size = self._funding.cluster_for_mint(mint)

        is_fresh = origin.is_fresh
        # Кластер — свойство минта. Сигнал для ЭТОГО buy, если кошелёк
        # из той же когорты (тот же funder), без требования is_fresh на
        # текущем тике (когорта уже собрана из свежих в by_funder).
        is_cluster = (
            size >= settings.INSIDER_MIN_SHARED_FUNDER
            and funder is not None
            and origin.funder == funder
        )

        # Нулевой буст: не предрешаем знак признака до измерения lift.
        boost = 0.0

        if is_cluster:
            reason = (
                f"инсайдер-кластер: {size} свежих кошельков с funder {funder[:8]}…"
            )
        elif is_fresh:
            reason = f"свежий кошелёк ({origin.age_hours:.1f}ч), кластер funder ещё не собран"
        else:
            reason = "паттерн нового кошелька+funder не подтверждён"

        verdict = InsiderVerdict(
            is_insider_cluster=is_cluster,
            is_fresh_wallet=is_fresh,
            funder=funder if is_cluster else origin.funder,
            cluster_size=size if is_cluster else 0,
            wallet_age_hours=origin.age_hours,
            score_boost=boost,
            reason=reason,
        )
        if is_cluster:
            logger.info("Insider signal %s / %s: %s", mint[:8], wallet[:8], reason)
        return verdict
