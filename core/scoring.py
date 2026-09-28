"""
Скоринг-движок: оценивает вероятность успеха сделки кита.

По умолчанию используется прозрачная взвешенная модель с весами из
концепции проекта (см. docs/CONCEPT.md, раздел "Скоринг-движок"). Если
найден обученный файл модели (data/model.pkl, создаётся
backtest/backtest.py), он используется вместо ручных весов.
"""
from __future__ import annotations

import logging
import pickle
from dataclasses import dataclass, field

from config import settings

logger = logging.getLogger("sniper.scoring")

MODEL_PATH = settings.DATA_DIR / "model.pkl"

# Дефолтные веса признаков (сумма максимальных вкладов = 1.0)
# ВАЖНО про wallet_is_fresh: сам по себе этот сигнал НЕОДНОЗНАЧЕН — крупный
# перевод на кошелёк прямо перед покупкой одинаково похож и на реального
# инсайдера, использующего свежий/burner-кошелёк (OPSEC), и на дева,
# финансирующего подставной кошелёк для wash-трейдинга собственного токена.
# Поэтому вес у него низкий — сам по себе он почти не двигает решение,
# вес сдвинут в сторону сигналов, которые тяжело подделать в масштабе:
# независимый кластер из нескольких разных кошельков (cluster_buy) и
# реальный накопленный winrate кошелька (historical_winrate).
# Веса пересобраны 04.09.2026 по итогам измерений. У каждого веса указан
# статус доказательности — это единственный принцип, по которому они
# распределены. Раньше 65% веса лежало на признаках, которые мы затем
# измерили как непредсказывающие.
DEFAULT_WEIGHTS = {
    # ДОКАЗАНО. Единственный признак, переживший проверку: 4577 наблюдений,
    # разделение обучение/проверка по времени, поправка Бонферрони.
    # Верхняя треть по баллу −8,3% против базы −17,5%, p = 0,0018.
    # Механизм объясняется формулой безубыточности: нужная вероятность
    # градации растёт как vSol², поэтому ранний вход дешевле.
    "low_liquidity": 0.45,

    # МЕХАНИЧЕСКИ СВЯЗАН с предыдущим (та же позиция на кривой с другой
    # стороны), отдельно не проверялся.
    "price_impact": 0.15,

    # НЕ ПРОВЕРЕН. У нас кластер вырожден — общих небиржевых спонсоров
    # почти нет, значит покупатели скорее независимы. Но литература
    # (Kamat, 166 098 минтов) показывает, что СОГЛАСОВАННАЯ когорта — это
    # отрицательный признак: в 15 раз чаще ноль органических покупателей.
    # Поэтому вес снижен с 0,30 и требует отдельной проверки.
    "cluster_buy": 0.10,

    # Funding-кластер свежих кошельков (новый сигнал из live funding_graph).
    # Литература трактует согласованную когорту осторожно — вес умеренный,
    # основной вклад идёт через score_boost детектора + INSIDER_FORCE_PASS.
    "insider_cluster": 0.12,

    # НЕ ПРОВЕРЕН.
    "wallet_is_fresh": 0.04,
    "wallet_is_new": 0.04,

    # ИЗМЕРЕНО КАК НЕПРЕДСКАЗЫВАЮЩЕЕ: эффект 0,039 и в обратную сторону
    # на 828 наблюдениях, вырожденное распределение на 4577. Ненулевой вес
    # оставлен только чтобы контур обучения продолжал накапливать данные,
    # а не чтобы влиять на решение.
    "historical_winrate": 0.04,
    "smart_money_flag": 0.04,

    # НИКОГДА НЕ ПРОВЕРЯЛСЯ, внешняя метка.
    "arkham_label": 0.02,
}


@dataclass
class EventFeatures:
    wallet_is_fresh: bool = False          # крупный перевод на кошелёк перед покупкой
    liquidity_usd: float = 0.0             # ликвидность пула в $
    pool_share_pct: float = 0.0            # доля покупки от ликвидности пула, 0..1
    cluster_size: int = 1                  # сколько китов купили токен за последние 30 мин
    is_smart_money: bool = False           # метка GMGN / свой smart-money фид
    historical_winrate: float = 0.5        # winrate кошелька по истории, 0..1
    has_arkham_label: bool = False         # кошелёк размечен в Arkham (whale/insider и т.п.)
    # Live funding-граф / insider detector
    insider_cluster: bool = False
    wallet_is_new: bool = False            # возраст кошелька < INSIDER_MAX_WALLET_AGE_HOURS
    insider_cluster_size: int = 0
    insider_score_boost: float = 0.0       # доп. буст поверх весов


@dataclass
class ScoreResult:
    score: float
    passed_threshold: bool
    breakdown: dict = field(default_factory=dict)


class ScoringEngine:
    def __init__(self):
        self._model = self._try_load_model()

    def _try_load_model(self):
        if MODEL_PATH.exists():
            try:
                with open(MODEL_PATH, "rb") as f:
                    logger.info("Загружена обученная модель скоринга: %s", MODEL_PATH)
                    return pickle.load(f)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Не удалось загрузить модель (%s), используются ручные веса", exc)
        return None

    def score(self, features: EventFeatures) -> ScoreResult:
        if self._model is not None:
            return self._score_with_model(features)
        return self._score_with_weights(features)

    def _score_with_weights(self, f: EventFeatures) -> ScoreResult:
        breakdown = {}

        breakdown["wallet_is_fresh"] = DEFAULT_WEIGHTS["wallet_is_fresh"] if f.wallet_is_fresh else 0.0
        breakdown["wallet_is_new"] = DEFAULT_WEIGHTS["wallet_is_new"] if f.wallet_is_new else 0.0

        low_liquidity = 0 < f.liquidity_usd < 50_000
        breakdown["low_liquidity"] = DEFAULT_WEIGHTS["low_liquidity"] if low_liquidity else 0.0

        impact_ratio = min(f.pool_share_pct / 0.15, 1.0) if f.pool_share_pct > 0.05 else 0.0
        breakdown["price_impact"] = DEFAULT_WEIGHTS["price_impact"] * impact_ratio

        cluster_ratio = max(min((f.cluster_size - 1) / 2, 1.0), 0.0)  # 2+ кита за 30 мин = максимум
        breakdown["cluster_buy"] = DEFAULT_WEIGHTS["cluster_buy"] * cluster_ratio

        if f.insider_cluster:
            ratio = max(min(f.insider_cluster_size / 3.0, 1.0), 0.4)
            breakdown["insider_cluster"] = DEFAULT_WEIGHTS["insider_cluster"] * ratio
        else:
            breakdown["insider_cluster"] = 0.0

        breakdown["smart_money_flag"] = DEFAULT_WEIGHTS["smart_money_flag"] if f.is_smart_money else 0.0

        breakdown["historical_winrate"] = DEFAULT_WEIGHTS["historical_winrate"] * max(min(f.historical_winrate, 1.0), 0.0)

        breakdown["arkham_label"] = DEFAULT_WEIGHTS["arkham_label"] if f.has_arkham_label else 0.0

        total = sum(breakdown.values()) + max(f.insider_score_boost, 0.0)
        if f.insider_score_boost:
            breakdown["insider_boost"] = f.insider_score_boost

        passed = total >= settings.SCORE_THRESHOLD
        if settings.INSIDER_FORCE_PASS and f.insider_cluster:
            passed = True

        return ScoreResult(score=round(total, 4), passed_threshold=passed, breakdown=breakdown)

    def _score_with_model(self, f: EventFeatures) -> ScoreResult:
        import numpy as np  # локальный импорт, чтобы numpy не требовался без модели

        x = np.array([[
            float(f.wallet_is_fresh),
            1.0 if 0 < f.liquidity_usd < 50_000 else 0.0,
            f.pool_share_pct,
            f.cluster_size,
            float(f.is_smart_money),
            f.historical_winrate,
            float(f.has_arkham_label),
        ]])
        proba = float(self._model.predict_proba(x)[0][1])
        return ScoreResult(score=round(proba, 4), passed_threshold=proba >= settings.SCORE_THRESHOLD,
                            breakdown={"model_proba": proba})
