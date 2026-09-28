"""
Бэктест-фреймворк: обучает логистическую регрессию на исторических
сделках китов, чтобы откалибровать веса скоринг-движка вместо ручных
весов по умолчанию (core/scoring.py).

Ожидаемый формат входных данных (CSV):
  timestamp, wallet, token_mint, wallet_is_fresh, liquidity_usd,
  pool_share_pct, cluster_size, is_smart_money, historical_winrate,
  has_arkham_label, outcome_48h

где outcome_48h = 1, если цена токена выросла более чем на X% за 48
часов после покупки кита, иначе 0. Разметку нужно сделать отдельно,
используя историю цен (Birdeye/DexScreener) для каждого события.

Использование:
  python backtest/backtest.py --data data/trades.csv --out data/model.pkl
"""
from __future__ import annotations

import argparse
import pickle
from pathlib import Path

import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.model_selection import TimeSeriesSplit

FEATURE_COLUMNS = [
    "wallet_is_fresh",
    "liquidity_usd",
    "pool_share_pct",
    "cluster_size",
    "is_smart_money",
    "historical_winrate",
    "has_arkham_label",
]
TARGET_COLUMN = "outcome_48h"


def load_dataset(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)
    missing = [c for c in FEATURE_COLUMNS + [TARGET_COLUMN] if c not in df.columns]
    if missing:
        raise ValueError(f"В датасете отсутствуют колонки: {missing}")
    return df


def train_and_evaluate(df: pd.DataFrame, n_splits: int = 5) -> LogisticRegression:
    """Обучает модель с TimeSeriesSplit, чтобы избежать утечки данных —
    модель никогда не видит будущие сделки при валидации на прошлых."""
    X = df[FEATURE_COLUMNS].astype(float).values
    y = df[TARGET_COLUMN].astype(int).values

    n_splits = min(n_splits, max(len(df) // 20, 1))
    tscv = TimeSeriesSplit(n_splits=n_splits) if n_splits > 1 else None
    fold_scores = []

    if tscv:
        for fold, (train_idx, test_idx) in enumerate(tscv.split(X), start=1):
            model = LogisticRegression(max_iter=1000, class_weight="balanced")
            model.fit(X[train_idx], y[train_idx])
            preds = model.predict(X[test_idx])
            proba = model.predict_proba(X[test_idx])[:, 1]
            try:
                auc = roc_auc_score(y[test_idx], proba)
            except ValueError:
                auc = float("nan")
            fold_scores.append(auc)
            print(f"--- Fold {fold} (AUC={auc:.3f}) ---")
            print(classification_report(y[test_idx], preds, zero_division=0))
        print(f"Средний AUC по фолдам: {pd.Series(fold_scores).mean():.3f}")
    else:
        print("Недостаточно данных для кросс-валидации TimeSeriesSplit — обучение без валидации")

    # Финальная модель — на всех доступных данных
    final_model = LogisticRegression(max_iter=1000, class_weight="balanced")
    final_model.fit(X, y)

    print("\nКалиброванные веса признаков (коэффициенты логрегрессии):")
    for name, coef in zip(FEATURE_COLUMNS, final_model.coef_[0]):
        print(f"  {name}: {coef:.4f}")

    return final_model


def main():
    parser = argparse.ArgumentParser(description="Бэктест и калибровка скоринг-модели")
    parser.add_argument("--data", type=Path, default=Path(__file__).resolve().parent.parent / "data" / "trades.csv")
    parser.add_argument("--out", type=Path, default=Path(__file__).resolve().parent.parent / "data" / "model.pkl")
    parser.add_argument("--splits", type=int, default=5)
    args = parser.parse_args()

    if not args.data.exists():
        raise SystemExit(
            f"Файл с данными не найден: {args.data}\n"
            "Соберите историю сделок китов (см. docstring этого файла) перед запуском бэктеста."
        )

    df = load_dataset(args.data)
    model = train_and_evaluate(df, n_splits=args.splits)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "wb") as f:
        pickle.dump(model, f)
    print(f"\nМодель сохранена: {args.out}")


if __name__ == "__main__":
    main()
