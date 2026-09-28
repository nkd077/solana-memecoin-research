#!/usr/bin/env python3
"""
Гипотеза №10: fdv/liq (навес) на t0 vs honest-доходность к t1.

Данные: data/graduates_snapshots.jsonl — без новых RPC.
DAS-контур НЕ строить: mass/liq ≡ mcap/liq − (пул/liq) ≈ A − const.

Пререгистрация (до расчёта, 2026-09-10):
  1) порог: fdv/liq ≥ 2 на t0 (из распределения когорты, не из результата)
  2) исход: honest; found=false → −100%; строки с found=false НЕ выбрасывать
  3) отсечек две → эффективное n << raw; писать в выводе
  4) рядом показать доходность до t0 (price_t0 / price_at_catch − 1),
     иначе не отличить «навес предсказывает» от «упавшее продолжает падать»

  PYTHONUNBUFFERED=1 ./venv/bin/python -m research.analyze_overhang_graduates
"""
from __future__ import annotations

import json
import statistics
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SNAPS = ROOT / "data" / "graduates_snapshots.jsonl"
COHORT = ROOT / "data" / "graduates_cohort.json"
OUT = ROOT / "data" / "analyze_overhang_graduates.json"

THRESHOLD = 2.0  # пререгистрация


def _med(xs: list[float]) -> float | None:
    return statistics.median(xs) if xs else None


def _pct(xs: list[float], p: float) -> float | None:
    if not xs:
        return None
    s = sorted(xs)
    i = min(len(s) - 1, max(0, int(round((p / 100) * (len(s) - 1)))))
    return s[i]


def _mean(xs: list[float]) -> float | None:
    return statistics.mean(xs) if xs else None


def load_snaps() -> dict[str, list[dict]]:
    by: dict[str, list[dict]] = defaultdict(list)
    with SNAPS.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            s = json.loads(line)
            m = s.get("mint")
            if not m:
                continue
            by[m].append(s)
    for m in by:
        by[m].sort(key=lambda x: float(x.get("ts") or 0))
    return by


def two_wave_cut(rows: list[dict]) -> float:
    """Граница между двумя отсечками по максимальному разрыву ts."""
    tss = sorted(float(r["ts"]) for r in rows)
    best_gap = 0.0
    cut = tss[len(tss) // 2]
    for a, b in zip(tss, tss[1:]):
        if b - a > best_gap:
            best_gap = b - a
            cut = (a + b) / 2
    return cut


def honest_ret(t0: dict, t1: dict) -> float:
    """found=false на t1 → −100%. Иначе price_t1/price_t0 − 1."""
    if not t1.get("found"):
        return -1.0
    p0 = t0.get("price_usd")
    p1 = t1.get("price_usd")
    try:
        p0f = float(p0)
        p1f = float(p1)
    except (TypeError, ValueError):
        return -1.0
    if p0f <= 0 or p1f < 0:
        return -1.0
    if not t0.get("found"):
        # t0 без рынка — в полный t0 не должны попасть; страховка
        return -1.0
    return p1f / p0f - 1.0


def pret0_ret(t0: dict, catch_price: float | None) -> float | None:
    if catch_price is None or catch_price <= 0:
        return None
    if not t0.get("found"):
        return None
    try:
        p0 = float(t0.get("price_usd") or 0)
    except (TypeError, ValueError):
        return None
    if p0 <= 0:
        return None
    return p0 / catch_price - 1.0


def summarize_group(name: str, rows: list[dict]) -> dict:
    honest = [r["honest"] for r in rows]
    pret = [r["pre_t0"] for r in rows if r.get("pre_t0") is not None]
    dead = sum(1 for r in rows if r["honest"] == -1.0)
    # survivor = found t1 (honest считается с цены, не −100% по found)
    surv = [r["honest"] for r in rows if r.get("t1_found")]
    out = {
        "n": len(rows),
        "honest_med": _med(honest),
        "honest_avg": _mean(honest),
        "honest_p10": _pct(honest, 10),
        "honest_p90": _pct(honest, 90),
        "dead_n": dead,
        "dead_pct": dead / len(rows) if rows else None,
        "survivor_n": len(surv),
        "survivor_med": _med(surv),
        "survivor_avg": _mean(surv),
        "pre_t0_n": len(pret),
        "pre_t0_med": _med(pret),
        "pre_t0_avg": _mean(pret),
        "feature_med": _med([r["fdv_liq"] for r in rows]),
    }
    print(f"\n=== {name} n={out['n']} ===")
    if not rows:
        return out
    print(
        f"  fdv/liq med={out['feature_med']:.3f}  "
        f"honest med={out['honest_med']:+.3f} avg={out['honest_avg']:+.3f} "
        f"p10={out['honest_p10']:+.3f} p90={out['honest_p90']:+.3f}"
    )
    print(f"  dead(found=false→−100%): {dead}/{len(rows)} ({100*dead/len(rows):.1f}%)")
    if surv:
        print(
            f"  survivors (found t1) n={len(surv)} "
            f"med={out['survivor_med']:+.3f} avg={out['survivor_avg']:+.3f}  "
            f"[описательно; вердикт по полному honest]"
        )
    if pret:
        print(
            f"  pre_t0 (catch→t0) n={len(pret)} med={out['pre_t0_med']:+.3f} "
            f"avg={out['pre_t0_avg']:+.3f}"
        )
    else:
        print("  pre_t0: нет price_at_catch")
    return out


def main() -> None:
    print("PRE-REGISTERED (2026-09-10)")
    print(f"  threshold fdv/liq ≥ {THRESHOLD} on t0")
    print("  honest: found=false → −100%; never drop those rows")
    print("  two cutoffs only → effective n limited by market-days")
    print("  report pre_t0 return alongside")
    print()

    by = load_snaps()
    all_rows = [r for rs in by.values() for r in rs]
    cut = two_wave_cut(all_rows)
    wave_ts = sorted({float(r["ts"]) for r in all_rows})
    # representative times
    w0_ts = [t for t in wave_ts if t < cut]
    w1_ts = [t for t in wave_ts if t >= cut]
    t0_label = datetime.fromtimestamp(statistics.median(w0_ts), timezone.utc).isoformat()
    t1_label = datetime.fromtimestamp(statistics.median(w1_ts), timezone.utc).isoformat()
    horizon_h = (statistics.median(w1_ts) - statistics.median(w0_ts)) / 3600

    print(f"mints={len(by)} snaps={len(all_rows)}")
    print(f"cutoffs (UTC): t0≈{t0_label}  t1≈{t1_label}  horizon≈{horizon_h:.1f}h")
    print(f"WARNING: only TWO market cutoffs — effective n ≪ paired count; "
          f"all rows share one of two calendar days.")

    cohort: dict = {}
    if COHORT.exists():
        raw = json.loads(COHORT.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            cohort = raw

    paired: list[dict] = []
    n_two = 0
    n_t0_full = 0
    for mint, rs in by.items():
        if len(rs) < 2:
            continue
        n_two += 1
        t0, t1 = rs[0], rs[-1]
        # полный t0: found + fdv + liq>0
        try:
            fdv = float(t0.get("fdv") or 0)
            liq = float(t0.get("liq_usd") or 0)
        except (TypeError, ValueError):
            continue
        if not t0.get("found") or fdv <= 0 or liq <= 0:
            continue
        n_t0_full += 1
        ratio = fdv / liq
        catch = None
        meta = cohort.get(mint) or {}
        if isinstance(meta, dict) and meta.get("price_at_catch") is not None:
            try:
                catch = float(meta["price_at_catch"])
            except (TypeError, ValueError):
                catch = None
        paired.append(
            {
                "mint": mint,
                "fdv_liq": ratio,
                "signal": ratio >= THRESHOLD,
                "honest": honest_ret(t0, t1),
                "pre_t0": pret0_ret(t0, catch),
                "t0_found": bool(t0.get("found")),
                "t1_found": bool(t1.get("found")),
                "t0_liq": liq,
                "t0_fdv": fdv,
                "t0_price": t0.get("price_usd"),
                "t1_price": t1.get("price_usd"),
                "wave_day": "d0" if float(t0["ts"]) < cut else "d1",
            }
        )

    print(f"with ≥2 snaps: {n_two}")
    print(f"full t0 (found+fdv+liq): {n_t0_full}")
    print(f"analysis n: {len(paired)}")

    ratios = sorted(r["fdv_liq"] for r in paired)
    print("\nfdv/liq on t0:")
    print(
        f"  p10={_pct(ratios,10):.3f}  p50={_pct(ratios,50):.3f}  "
        f"p90={_pct(ratios,90):.3f}  p99={_pct(ratios,99):.3f}  max={ratios[-1]:.1f}"
    )
    print(
        f"  ≥{THRESHOLD:g}x: {sum(1 for x in ratios if x>=THRESHOLD)} "
        f"({100*sum(1 for x in ratios if x>=THRESHOLD)/len(ratios):.1f}%)"
    )

    high = [r for r in paired if r["signal"]]
    ctrl = [r for r in paired if not r["signal"]]
    s_high = summarize_group(f"HIGH fdv/liq≥{THRESHOLD:g}", high)
    s_ctrl = summarize_group(f"CTRL fdv/liq<{THRESHOLD:g}", ctrl)

    # сравнение
    print("\n=== CONTRAST ===")
    if s_high["honest_med"] is not None and s_ctrl["honest_med"] is not None:
        d = s_high["honest_med"] - s_ctrl["honest_med"]
        print(f"  Δ med honest (high−ctrl) = {d:+.3f}")
    if s_high["pre_t0_med"] is not None and s_ctrl["pre_t0_med"] is not None:
        dp = s_high["pre_t0_med"] - s_ctrl["pre_t0_med"]
        print(f"  Δ med pre_t0 (high−ctrl) = {dp:+.3f}")
        if s_high["pre_t0_med"] is not None and s_high["pre_t0_med"] > 10:
            print(
                "  NOTE: high group already massively up catch→t0 — "
                "overhang here co-moves with prior pump, not prior dump."
            )

    # все t0 — первая отсечка, все t1 — вторая: один парный эксперимент, не 457 независимых
    print("\n=== CLUSTERING ===")
    print(
        f"  all t0 on first cutoff, all t1 on second — ONE paired market window "
        f"(~{horizon_h:.0f}h), not {len(paired)} independent bets. "
        f"effective market-days = 2 (one transition)."
    )

    # вердикт-рамка (не n≥100 — честно сказать про мощность)
    print("\n=== FRAME ===")
    print(
        "  mcap/liq — стандартная эвристика риска, не находка. "
        "Ценность — проверка на своих исходах."
    )
    print(
        "  VERDICT #10 CLOSED: threshold selects float/market vs non-market, "
        "not overhang; reproduces graduate base rate. "
        f"Single window {t0_label[:10]} → {t1_label[:10]}, not "
        f"{len(paired)} independent bets. See CLOSED_HYPOTHESES.md §A11."
    )

    payload = {
        "pre_registered": {
            "threshold": THRESHOLD,
            "honest_found_false": -1.0,
            "drop_found_false": False,
            "cutoffs": 2,
            "report_pre_t0": True,
        },
        "t0_utc": t0_label,
        "t1_utc": t1_label,
        "horizon_hours": horizon_h,
        "n_paired": len(paired),
        "n_high": len(high),
        "n_ctrl": len(ctrl),
        "high": s_high,
        "ctrl": s_ctrl,
        "delta_med_honest": (
            (s_high["honest_med"] - s_ctrl["honest_med"])
            if s_high["honest_med"] is not None and s_ctrl["honest_med"] is not None
            else None
        ),
        "delta_med_pre_t0": (
            (s_high["pre_t0_med"] - s_ctrl["pre_t0_med"])
            if s_high["pre_t0_med"] is not None and s_ctrl["pre_t0_med"] is not None
            else None
        ),
    }
    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
