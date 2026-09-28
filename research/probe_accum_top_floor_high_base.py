#!/usr/bin/env python3
"""
Одноразовый прогон: top_floor у монет с высокой базой (пилот ≥ $20k),
пересечение с текущим active-set.

Цель: отличить пыльную отсечку хвоста от реальной обрезки deep holders.
45-мин прогон по всей пыли НЕ использовать — он измеряет текучку dust.

  ./venv/bin/python -m research.probe_accum_top_floor_high_base
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import aiohttp

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config import settings  # noqa: E402
from core.accum_balance import (  # noqa: E402
    compute_conc,
    fetch_largest_accounts,
    resolve_pool_account,
)
from core.helius_gate import get_helius_gate  # noqa: E402

PILOT_SNAPS = ROOT / "data/pilot_lab_accum_balance_2026-09-09/accum_balance_snaps.jsonl"
META = ROOT / "data/accum_balance_meta.json"
DEX = "https://api.dexscreener.com/latest/dex/tokens/"
MIN_BASE_USD = 20_000.0
FLOOR_USD_CUT = 1.0  # как в спеке: значимая отсечка


def load_high_base_mints(path: Path, min_base: float) -> dict[str, float]:
    """mint → max base_usd = conc * price по пилотным снимкам."""
    best: dict[str, float] = {}
    if not path.exists():
        raise SystemExit(f"missing pilot snaps: {path}")
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            s = json.loads(line)
            mint = s.get("mint")
            if not mint or s.get("dex_missing"):
                continue
            conc = float(s.get("conc") or 0)
            price = float(s.get("price_usd") or 0)
            if conc <= 0 or price <= 0:
                continue
            base = conc * price
            if base >= min_base:
                best[mint] = max(best.get(mint, 0.0), base)
    return best


def load_active() -> set[str]:
    raw = json.loads(META.read_text(encoding="utf-8"))
    mints = raw.get("mints") if isinstance(raw.get("mints"), dict) else {}
    return {m for m, v in mints.items() if isinstance(v, dict) and v.get("active")}


async def dex_price(session: aiohttp.ClientSession, mint: str) -> tuple[float, float]:
    url = DEX + mint
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            if resp.status != 200:
                return 0.0, 0.0
            data = await resp.json()
    except Exception:  # noqa: BLE001
        return 0.0, 0.0
    best_liq, best_price = 0.0, 0.0
    for p in data.get("pairs") or []:
        base = ((p.get("baseToken") or {}).get("address")) or ""
        if base != mint:
            continue
        liq = float(((p.get("liquidity") or {}).get("usd")) or 0)
        price = float(p.get("priceUsd") or 0)
        if liq >= best_liq:
            best_liq, best_price = liq, price
    return best_price, best_liq


async def probe_one(session: aiohttp.ClientSession, mint: str, pilot_base: float) -> dict:
    gate = get_helius_gate()
    await gate.acquire()
    accounts = await fetch_largest_accounts(session, mint)
    if accounts is None:
        return {"mint": mint, "pilot_base": pilot_base, "error": "rpc_largest"}
    pool = await resolve_pool_account(session, mint, accounts)
    # supply не критичен для floor; >50% exclude может убрать пул-дубль уже через pool
    conc, n_used, excluded, top = compute_conc(
        accounts,
        pool_account=pool,
        supply=None,
        top_n=int(settings.ACCUM_BAL_TOP_N),
        exclude_share=float(settings.ACCUM_BAL_EXCLUDE_SHARE),
    )
    price, liq = await dex_price(session, mint)
    floor_tok = min((b for _, b in top), default=0.0)
    floor_usd = floor_tok * price if price > 0 else 0.0
    base_now = conc * price if price > 0 else 0.0
    return {
        "mint": mint,
        "pilot_base": round(pilot_base, 2),
        "base_now": round(base_now, 2),
        "liq_usd": round(liq, 2),
        "price_usd": price,
        "holders_raw": len(accounts),
        "top_n_used": n_used,
        "top_floor_tok": floor_tok,
        "top_floor_usd": floor_usd,
        "significant_floor": floor_usd >= FLOOR_USD_CUT,
        "pool": pool,
        "excluded_n": len(excluded),
    }


async def main() -> None:
    high = load_high_base_mints(PILOT_SNAPS, MIN_BASE_USD)
    active = load_active()
    overlap = sorted(set(high) & active, key=lambda m: -high[m])
    print(
        f"pilot base≥${MIN_BASE_USD:,.0f}: {len(high)} mints | "
        f"active: {len(active)} | overlap: {len(overlap)}"
    )
    if not overlap:
        print("no overlap — try without active filter? abort")
        return

    rows: list[dict] = []
    async with aiohttp.ClientSession(
        headers={"User-Agent": "sniper-accum-floor-probe/1.0", "Accept": "application/json"}
    ) as session:
        for i, mint in enumerate(overlap, 1):
            row = await probe_one(session, mint, high[mint])
            rows.append(row)
            flag = "FLOOR" if row.get("significant_floor") else "dust"
            print(
                f"[{i}/{len(overlap)}] {mint[:8]}… pilot_base=${row['pilot_base']:,.0f} "
                f"floor_usd=${row.get('top_floor_usd', 0):.4f} "
                f"n={row.get('top_n_used')} raw={row.get('holders_raw')} {flag}"
                + (f" err={row['error']}" if row.get("error") else "")
            )
            await asyncio.sleep(0.05)

    ok = [r for r in rows if not r.get("error") and r.get("price_usd", 0) > 0]
    floors = sorted(r["top_floor_usd"] for r in ok)
    print("\n=== top_floor_usd (overlap, priced) ===")
    if floors:
        def pct(p: float) -> float:
            if not floors:
                return 0.0
            i = min(len(floors) - 1, max(0, int(round((p / 100) * (len(floors) - 1)))))
            return floors[i]
        print(
            f"n={len(floors)}  min=${floors[0]:.6f}  p50=${pct(50):.4f}  "
            f"p90=${pct(90):.4f}  max=${floors[-1]:.4f}"
        )
        n_sig = sum(1 for r in ok if r["significant_floor"])
        print(f"top_floor_usd ≥ ${FLOOR_USD_CUT}: {n_sig}/{len(ok)}")
        if n_sig == 0:
            print(
                "→ везде пыльная отсечка и на high-base: обрезка deep holders "
                "не связывает; (b) структурно слабее, остаётся вопрос (a)."
            )
        else:
            print(
                "→ значимая отсечка есть в high-base подгруппе: (b) жив там; "
                "правило (a)/(b) считать по окнам с top_floor_usd≥$1."
            )
    else:
        print("no priced rows")

    out = ROOT / "data" / "probe_top_floor_high_base.jsonl"
    with out.open("w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r) + "\n")
    print(f"wrote {out}")


if __name__ == "__main__":
    asyncio.run(main())
