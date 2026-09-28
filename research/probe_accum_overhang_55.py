#!/usr/bin/env python3
"""
Навес / ликвидность по 55 high-base ∩ active.

Фаза A (дёшево): mcap/liq с Dex — разброс без DAS.
Фаза B (точнее): DAS mass excl AMM / liq, max 10 страниц/mint
         (result.total врёт — счёт по длинам страниц).

  PYTHONUNBUFFERED=1 ./venv/bin/python -m research.probe_accum_overhang_55
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any

import aiohttp

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config import settings  # noqa: E402
from core.accum_balance import (  # noqa: E402
    AMM_PROGRAM_IDS,
    BURN_ADDRESSES,
    resolve_account_programs,
    resolve_token_owners,
)
from core.helius_gate import get_helius_gate  # noqa: E402

FLOOR_PROBE = ROOT / "data" / "probe_top_floor_high_base.jsonl"
DEX = "https://api.dexscreener.com/latest/dex/tokens/"
PAGE_LIMIT = 1000
MAX_PAGES = 10  # safety: 10k accounts; иначе hung на мем-миллионах
CLASSIFY_TOP = 80


def rpc_url() -> str:
    return settings.HELIUS_RPC_URL.format(key=settings.HELIUS_API_KEY)


def pctile(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return float("nan")
    i = min(len(sorted_vals) - 1, max(0, int(round((p / 100) * (len(sorted_vals) - 1)))))
    return sorted_vals[i]


def summarize(name: str, vals: list[float]) -> None:
    s = sorted(vals)
    print(f"\n=== {name} n={len(s)} ===")
    if not s:
        return
    print(
        f"min={s[0]:.2f}x  p10={pctile(s,10):.2f}x  p50={pctile(s,50):.2f}x  "
        f"p90={pctile(s,90):.2f}x  max={s[-1]:.2f}x"
    )
    for lo, hi, lab in [
        (0, 2, "<2x"),
        (2, 10, "2–10x"),
        (10, 50, "10–50x"),
        (50, 200, "50–200x"),
        (200, 1e18, "≥200x"),
    ]:
        print(f"  {lab:8}  {sum(1 for x in s if lo <= x < hi)}")
    spread = s[-1] / max(s[0], 1e-12)
    p_spread = pctile(s, 90) / max(pctile(s, 10), 1e-12)
    if spread < 5 and p_spread < 3:
        print("→ разброс мал: мерить почти нечего")
    else:
        print(f"→ разброс есть (max/min={spread:.1f}, p90/p10={p_spread:.1f})")


async def rpc(session: aiohttp.ClientSession, method: str, params: Any) -> Any:
    await get_helius_gate().acquire()
    payload = {"jsonrpc": "2.0", "id": method, "method": method, "params": params}
    async with session.post(
        rpc_url(), json=payload, timeout=aiohttp.ClientTimeout(total=90)
    ) as resp:
        body = await resp.json()
    if body.get("error"):
        raise RuntimeError(body["error"])
    return body.get("result")


async def dex_meta(session: aiohttp.ClientSession, mint: str) -> dict:
    try:
        async with session.get(DEX + mint, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            if resp.status != 200:
                return {}
            data = await resp.json()
    except Exception:  # noqa: BLE001
        return {}
    best = None
    best_liq = -1.0
    for p in data.get("pairs") or []:
        base = ((p.get("baseToken") or {}).get("address")) or ""
        if base != mint:
            continue
        liq = float(((p.get("liquidity") or {}).get("usd")) or 0)
        if liq >= best_liq:
            best_liq = liq
            best = p
    if not best:
        return {}
    price = float(best.get("priceUsd") or 0)
    liq = float(((best.get("liquidity") or {}).get("usd")) or 0)
    # fdv / mcap с dex если есть
    fdv = float(best.get("fdv") or 0)
    mcap = float(best.get("marketCap") or fdv or 0)
    return {"price_usd": price, "liq_usd": liq, "mcap_usd": mcap, "fdv_usd": fdv}


async def fetch_pages(
    session: aiohttp.ClientSession, mint: str
) -> tuple[list[dict], int, bool]:
    """
    result['total'] ВРЁТ — не использовать.
    truncated=True если упёрлись в MAX_PAGES при полной последней странице.
    """
    all_acc: list[dict] = []
    pages = 0
    for page in range(1, MAX_PAGES + 1):
        result = await rpc(
            session,
            "getTokenAccounts",
            {"mint": mint, "page": page, "limit": PAGE_LIMIT},
        )
        pages += 1
        batch = list((result or {}).get("token_accounts") or [])
        if not batch:
            return all_acc, pages, False
        all_acc.extend(batch)
        print(f"    page {page}: +{len(batch)} (sum={len(all_acc)})", flush=True)
        if len(batch) < PAGE_LIMIT:
            return all_acc, pages, False
    return all_acc, pages, True


async def find_amm(session: aiohttp.ClientSession, ranked: list[dict]) -> set[str]:
    top = ranked[:CLASSIFY_TOP]
    addrs = [r["address"] for r in top if r.get("address")]
    owners = await resolve_token_owners(session, addrs)
    programs = await resolve_account_programs(
        session, list({o for o in owners.values() if o})
    )
    out: set[str] = set()
    for ta, own in owners.items():
        prog = programs.get(own)
        if prog and prog in AMM_PROGRAM_IDS:
            out.add(ta)
    return out


async def phase_a(session: aiohttp.ClientSession, mints: list[tuple[str, float]]) -> list[dict]:
    print("=== PHASE A: mcap/liq (dex only) ===", flush=True)
    rows = []
    for i, (mint, base) in enumerate(mints, 1):
        d = await dex_meta(session, mint)
        liq = float(d.get("liq_usd") or 0)
        mcap = float(d.get("mcap_usd") or 0)
        oh = (mcap / liq) if liq > 0 and mcap > 0 else None
        row = {
            "mint": mint,
            "pilot_base": base,
            "price_usd": d.get("price_usd"),
            "liq_usd": liq,
            "mcap_usd": mcap,
            "overhang_mcap": oh,
        }
        rows.append(row)
        print(
            f"[A {i}/{len(mints)}] {mint[:8]}… "
            f"mcap/liq={oh:.1f}x" if oh else f"[A {i}/{len(mints)}] {mint[:8]}… no mcap/liq",
            flush=True,
        )
        await asyncio.sleep(0.15)
    ok = [r["overhang_mcap"] for r in rows if r.get("overhang_mcap")]
    summarize("overhang ≈ mcap/liq", ok)
    return rows


async def phase_b(session: aiohttp.ClientSession, mints: list[tuple[str, float]]) -> list[dict]:
    print("\n=== PHASE B: DAS mass_excl_amm / liq ===", flush=True)
    rows = []
    for i, (mint, base) in enumerate(mints, 1):
        print(f"[B {i}/{len(mints)}] {mint[:8]}…", flush=True)
        try:
            d = await dex_meta(session, mint)
            price = float(d.get("price_usd") or 0)
            liq = float(d.get("liq_usd") or 0)
            raw, pages, trunc = await fetch_pages(session, mint)
            supply_res = await rpc(
                session, "getTokenSupply", [mint, {"commitment": "confirmed"}]
            )
            val = (supply_res or {}).get("value") or {}
            dec = int(val.get("decimals") or 0)
            supply = float(val.get("uiAmount") or 0)
        except Exception as exc:  # noqa: BLE001
            rows.append({"mint": mint, "pilot_base": base, "error": str(exc)[:200]})
            print(f"  ERR {exc}", flush=True)
            continue

        if price <= 0 or liq <= 0:
            rows.append(
                {
                    "mint": mint,
                    "pilot_base": base,
                    "error": "no_price_or_liq",
                    "n_pages": pages,
                    "truncated": trunc,
                }
            )
            print("  ERR no price/liq", flush=True)
            continue

        scale = 10 ** dec
        ranked = []
        for a in raw:
            amt = int(a.get("amount") or 0)
            if amt > 0:
                ranked.append({"address": a.get("address") or "", "ui": amt / scale})
        ranked.sort(key=lambda r: -r["ui"])
        amm = await find_amm(session, ranked)
        excl_share = float(settings.ACCUM_BAL_EXCLUDE_SHARE)
        mass = amm_ui = 0.0
        for r in ranked:
            share = (r["ui"] / supply) if supply > 0 else 0.0
            if r["address"] in amm:
                amm_ui += r["ui"]
            elif r["address"] in BURN_ADDRESSES:
                continue
            elif share > excl_share:
                continue
            else:
                mass += r["ui"]
        mass_usd = mass * price
        oh = mass_usd / liq
        row = {
            "mint": mint,
            "pilot_base": base,
            "n_pages": pages,
            "truncated": trunc,
            "n_nonzero": len(ranked),
            "n_amm": len(amm),
            "price_usd": price,
            "liq_usd": liq,
            "mcap_usd": d.get("mcap_usd"),
            "mass_usd": mass_usd,
            "amm_usd": amm_ui * price,
            "overhang": oh,
            "overhang_mcap": (
                float(d["mcap_usd"]) / liq if d.get("mcap_usd") and liq else None
            ),
        }
        rows.append(row)
        flag = " TRUNC" if trunc else ""
        print(
            f"  overhang={oh:.1f}x mass=${mass_usd:,.0f} liq=${liq:,.0f} "
            f"amm=${amm_ui*price:,.0f} pages={pages}{flag}",
            flush=True,
        )
    ok = [r["overhang"] for r in rows if "overhang" in r]
    summarize("overhang = mass_excl_amm / liq", ok)
    n_trunc = sum(1 for r in rows if r.get("truncated"))
    if n_trunc:
        print(f"(truncated at {MAX_PAGES} pages: {n_trunc} — mass занижен)")
    return rows


async def main() -> None:
    mints: list[tuple[str, float]] = []
    for line in FLOOR_PROBE.open(encoding="utf-8"):
        r = json.loads(line)
        if r.get("error"):
            continue
        mints.append((r["mint"], float(r.get("pilot_base") or 0)))
    mints.sort(key=lambda x: -x[1])
    print(f"mints={len(mints)}  MAX_PAGES={MAX_PAGES}", flush=True)

    async with aiohttp.ClientSession(
        headers={"User-Agent": "sniper-overhang-55/1.1", "Accept": "application/json"}
    ) as session:
        a_rows = await phase_a(session, mints)
        path_a = ROOT / "data" / "probe_overhang_55_mcap.jsonl"
        with path_a.open("w", encoding="utf-8") as fh:
            for r in a_rows:
                fh.write(json.dumps(r) + "\n")
        print(f"wrote {path_a}", flush=True)

        b_rows = await phase_b(session, mints)
        path_b = ROOT / "data" / "probe_overhang_55.jsonl"
        with path_b.open("w", encoding="utf-8") as fh:
            for r in b_rows:
                fh.write(json.dumps(r) + "\n")
        print(f"wrote {path_b}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
