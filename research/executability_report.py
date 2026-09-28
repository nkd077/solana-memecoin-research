#!/usr/bin/env python3
"""Пороговая пара: размер позиции × минимальный пул.

Вопрос не «какой размер исполним», а: при каких (size, min_pool, K)
проскальзывание выхода не съедает сделку, и какая доля токенов в
популяции этому удовлетворяет.

Популяции (уже собранные данные, без RPC):
  birth   — outcomes.jsonl с exit_liquidity_usd
  grad    — первый снимок graduates (вся когорта)
  grad5k  — первый снимок с liq≥$5000 (реальный пул)

  python -m research.executability_report
"""
from __future__ import annotations

import json
import statistics
from pathlib import Path

from config import settings

SOL_USD = 150.0  # грубо; для доли токенов важен порядок, не тик
POSITIONS_SOL = (0.005, 0.01, 0.05, 0.1)
MIN_POOLS = (100, 400, 2_000, 5_000, 12_000)
MULTIPLES = (10, 15, 20)


def _ok(liq: float, pos_usd: float, min_pool: float, k: float) -> bool:
    if liq is None or liq <= 0:
        return False
    if liq < min_pool:
        return False
    if pos_usd > 0 and liq < pos_usd * k:
        return False
    return True


def load_birth_liqs() -> list[float]:
    out = []
    path = settings.DATA_DIR / "outcomes.jsonl"
    with open(path, encoding="utf-8") as f:
        for line in f:
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            liq = r.get("exit_liquidity_usd")
            if liq is None:
                continue
            try:
                out.append(float(liq))
            except (TypeError, ValueError):
                continue
    return out


def load_grad_first_liqs(*, min_first: float | None = None) -> list[float]:
    snaps_path = settings.DATA_DIR / "graduates_snapshots.jsonl"
    by: dict[str, list] = {}
    if not snaps_path.exists():
        return []
    with open(snaps_path, encoding="utf-8") as f:
        for line in f:
            try:
                s = json.loads(line)
            except json.JSONDecodeError:
                continue
            m = s.get("mint")
            if not m:
                continue
            by.setdefault(m, []).append(s)
    out = []
    for ss in by.values():
        ss = sorted(ss, key=lambda x: x.get("ts") or 0)
        first = ss[0]
        liq = first.get("liq_usd")
        if liq is None:
            continue
        try:
            lv = float(liq)
        except (TypeError, ValueError):
            continue
        if min_first is not None and lv < min_first:
            continue
        out.append(lv)
    return out


def table(name: str, liqs: list[float]) -> None:
    if not liqs:
        print(f"\n=== {name}: n=0 ===")
        return
    med = statistics.median(liqs)
    print(f"\n=== {name}: n={len(liqs)}  med_liq=${med:,.0f} ===")
    print(
        f"{'pos_SOL':>8} {'pos_$':>8} {'min_pool':>10} {'K':>4} "
        f"{'ok%':>7} {'ok_n':>6}"
    )
    # компактная сетка: для каждого size — ключевые (pool, K)
    grid = [
        (0.005, 400, 20),
        (0.01, 400, 20),
        (0.01, 2_000, 20),
        (0.01, 5_000, 20),
        (0.05, 2_000, 20),
        (0.05, 5_000, 15),
        (0.05, 12_000, 20),
        (0.1, 5_000, 20),
        (0.1, 12_000, 20),
        (0.1, 12_000, 10),
    ]
    for pos_sol, min_pool, k in grid:
        pos_usd = pos_sol * SOL_USD
        ok = sum(1 for L in liqs if _ok(L, pos_usd, min_pool, k))
        pct = 100.0 * ok / len(liqs)
        print(
            f"{pos_sol:8.3f} {pos_usd:8.1f} {min_pool:10.0f} {k:4.0f} "
            f"{pct:6.1f}% {ok:6d}"
        )


def synthesis(birth: list[float], grad: list[float], grad5k: list[float]) -> None:
    print("\n=== синтез ===")
    pos = 0.01 * SOL_USD  # $1.5
    b_ok = sum(1 for L in birth if _ok(L, pos, 400, 20)) / max(len(birth), 1)
    g_ok = sum(1 for L in grad5k if _ok(L, pos, 400, 20)) / max(len(grad5k), 1)
    print(
        f"позиция 0.01 SOL (~${pos:.0f}), порог пул≥$400 и ≥20×:\n"
        f"  birth exit-liq: {100*b_ok:.1f}% токенов исполнима\n"
        f"  grad (liq0≥$5k): {100*g_ok:.1f}% токенов исполнима"
    )
    print(
        "На рождении связывающее ограничение — выход (почти никто не проходит).\n"
        "После градации с реальным пулом выход обычно ок; связывающее — доходность\n"
        "(см. track_graduates --report: med ≈ −95% на confirmed)."
    )
    if grad:
        real = sum(1 for L in grad if L >= 12_000)
        print(
            f"Доля «настоящей» миграции (первый snap liq≥$12k): "
            f"{real}/{len(grad)} = {100*real/len(grad):.1f}%"
        )


def main() -> None:
    print(f"SOL_USD≈{SOL_USD:g} (для перевода SOL→$; доли чувствительны слабо)")
    birth = load_birth_liqs()
    grad = load_grad_first_liqs()
    grad5k = load_grad_first_liqs(min_first=5000.0)
    table("birth (outcomes exit_liquidity)", birth)
    table("grad first snap (all)", grad)
    table("grad first snap (liq≥$5k)", grad5k)
    synthesis(birth, grad, grad5k)


if __name__ == "__main__":
    main()
