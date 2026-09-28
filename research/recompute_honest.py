#!/usr/bin/env python3
"""Пересчёт honest-метрик из уже записанных outcomes (без RPC).

Старый market_alive = «≥1 сделка/час» пропускал пулы на $18.
Здесь: пул ≥ max(OUTCOME_MIN_EXIT_LIQUIDITY_USD, position×K) и ≥N txns.

  python -m research.recompute_honest
  python -m research.recompute_honest --write   # пишет data/outcomes_strict.jsonl
"""
from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path

from config import settings

SOL_USD_FALLBACK = 150.0


def _txns(r: dict) -> int:
    for k in ("exit_txns_h1", "exit_txns_m5"):
        v = r.get(k)
        if v is None:
            continue
        try:
            return int(v)
        except (TypeError, ValueError):
            continue
    return 0


def _pos_usd(r: dict) -> float:
    if r.get("exit_position_usd") is not None:
        try:
            return float(r["exit_position_usd"])
        except (TypeError, ValueError):
            pass
    buy = float(r.get("buy_sol_amount") or settings.MIN_POSITION_SOL or 0.05)
    return buy * SOL_USD_FALLBACK


def is_exit_tradeable_row(r: dict) -> bool:
    liq = r.get("exit_liquidity_usd")
    if liq is None:
        # нет данных о пуле — не считаем продаваемым
        return False
    try:
        liq = float(liq)
    except (TypeError, ValueError):
        return False
    if _txns(r) < int(settings.OUTCOME_MIN_EXIT_TXNS):
        return False
    if liq < float(settings.OUTCOME_MIN_EXIT_LIQUIDITY_USD):
        return False
    pos = _pos_usd(r)
    if pos > 0 and liq < pos * float(settings.OUTCOME_EXIT_LIQUIDITY_MULTIPLE):
        return False
    return True


def honest_growth(r: dict) -> float | None:
    g = r.get("growth")
    if not is_exit_tradeable_row(r):
        return -1.0
    if g is None:
        return -1.0
    try:
        return float(g)
    except (TypeError, ValueError):
        return -1.0


def _med(xs: list[float]) -> float | None:
    return statistics.median(xs) if xs else None


def summarize(rows: list[dict], label: str) -> None:
    naive, honest = [], []
    dead = 0
    for r in rows:
        g = r.get("growth")
        if g is not None:
            try:
                naive.append(float(g))
            except (TypeError, ValueError):
                pass
        h = honest_growth(r)
        if h is not None:
            honest.append(h)
            if h <= -0.999:
                dead += 1
    print(f"\n{label}: n={len(rows)}")
    if naive:
        print(f"  naive  med={_med(naive):+.3f}  avg={statistics.mean(naive):+.3f}")
    if honest:
        print(
            f"  strict med={_med(honest):+.3f}  avg={statistics.mean(honest):+.3f}  "
            f"untradeable={dead}/{len(honest)} ({100*dead/len(honest):.1f}%)"
        )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true", help="писать outcomes_strict.jsonl")
    ap.add_argument("--sol-usd", type=float, default=150.0)
    args = ap.parse_args()
    sol_usd = float(args.sol_usd)

    path = settings.DATA_DIR / "outcomes.jsonl"
    rows = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    # подставляем sol для position_usd фолбэка
    import sys as _sys
    _self = _sys.modules[__name__]
    _self.SOL_USD_FALLBACK = sol_usd

    print(
        f"outcomes={len(rows)} | strict: txns≥{settings.OUTCOME_MIN_EXIT_TXNS} "
        f"liq≥${settings.OUTCOME_MIN_EXIT_LIQUIDITY_USD:g} "
        f"×{settings.OUTCOME_EXIT_LIQUIDITY_MULTIPLE:g} позиции"
    )

    summarize(rows, "все")
    summarize([r for r in rows if not r.get("insider_cluster")], "база (!insider)")
    summarize([r for r in rows if r.get("insider_cluster")], "insider_cluster")
    summarize([r for r in rows if r.get("lab_track") == "lab_early"], "lab_early")
    summarize([r for r in rows if r.get("lab_track") == "lab_accum"], "lab_accum")
    summarize([r for r in rows if r.get("lab_track") == "lab_insider"], "lab_insider")
    # lab_vol_spike = разгон (честное имя); lab_whale_sit = legacy до rename
    vol_rows = [
        r for r in rows
        if r.get("lab_track") in ("lab_vol_spike", "lab_whale_sit")
        and (r.get("whale_sit_reason") or r.get("vol_spike_reason")) != "dex_volume_spike"
    ]
    summarize(vol_rows, "lab_vol_spike")
    ab = [r for r in rows if r.get("lab_track") == "lab_accum_balance"]
    summarize(ab, "lab_accum_balance (all windows)")
    summarize([r for r in ab if r.get("is_signal")], "lab_accum_balance (signals)")
    summarize([r for r in ab if not r.get("is_signal")], "lab_accum_balance (control)")

    if args.write:
        out = settings.DATA_DIR / "outcomes_strict.jsonl"
        with open(out, "w", encoding="utf-8") as f:
            for r in rows:
                r2 = dict(r)
                t = is_exit_tradeable_row(r)
                r2["exit_tradeable"] = t
                r2["market_alive_legacy"] = r.get("market_alive")
                r2["market_alive"] = t
                r2["honest_growth"] = honest_growth(r)
                f.write(json.dumps(r2, ensure_ascii=False) + "\n")
        print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
