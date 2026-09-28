#!/usr/bin/env python3
"""
Пробный полный снимок DAS getTokenAccounts vs top-20.

Одна монета, закрывает сразу:
  1) сколько не видели (full non-float conc / top-20 conc)
  2) форма распределения (голова vs плоскость)
  3) кого ещё исключать (программы владельцев крупных аккаунтов)
  4) держатели / масса выше $100 / $1k / $10k

Поле result.total у getTokenAccounts ВРЁТ (часто = len страницы).
Считать только суммой длин страниц до первой пустой.

  ./venv/bin/python -m research.probe_accum_das_full_snap [mint]
"""
from __future__ import annotations

import asyncio
import json
import math
import sys
from pathlib import Path
from typing import Any, Optional

import aiohttp

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config import settings  # noqa: E402
from core.accum_balance import (  # noqa: E402
    AMM_PROGRAM_IDS,
    BURN_ADDRESSES,
    compute_conc,
    fetch_largest_accounts,
    fetch_supply,
    resolve_account_programs,
    resolve_pool_account,
    resolve_token_owners,
)
from core.helius_gate import get_helius_gate  # noqa: E402

DEFAULT_MINT = "2aBJFxNDo4i3cWnknGgPULCUmEck1nKr8RT1fD5Upump"
DEX = "https://api.dexscreener.com/latest/dex/tokens/"
PAGE_LIMIT = 1000
THRESHOLDS_USD = (100.0, 1_000.0, 10_000.0)

# Известные программы владельцев токен-аккаунтов (не System = не обычный кошелёк).
PROGRAM_NAMES: dict[str, str] = {
    **{p: "amm_known" for p in AMM_PROGRAM_IDS},
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc": "orca_whirlpool",
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo": "meteora_dlmm",
    "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB": "meteora_pools",
    "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG": "meteora_Damm_v2",
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA": "token_program",
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb": "token_2022",
    "11111111111111111111111111111111": "system",
    "Stake11111111111111111111111111111111111111": "stake",
    "SPoo1Ku8WFXoNDMHPsrGSTSG1Y6rFun3gUGJC4cdBjx": "stake_pool",
    "MarBmsSgKXdrN1egZf5qy4wLQnhcmS9XTr1tqMRQGnP": "marinade",
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4": "jupiter_v6",
    "Dooar9JkhdZ7J3LHN3A7YCuoGRUggXhQaHYTRGAxQUjr": "dooar",
    "TSWAPaqyCSx2KABk68Shruf4rp7CxcNi8hAsbdwmHbN": "tensor",
    "auth9SigNpDKz4sJJ1DfCTuZrZNSAgh9sFD3rboVmgg": "auth_rules",
    "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s": "metaplex",
    "BGUMAp9Gq7iTEuizy4pqaxsNyMoeanqtZV97RAEP3EZ": "bubblegum",
    "namesLPneVptA9Z5rqUDD9tMTWEJwofgaYwp8cawRkX": "name_service",
}


def rpc_url() -> str:
    return settings.HELIUS_RPC_URL.format(key=settings.HELIUS_API_KEY)


async def rpc(session: aiohttp.ClientSession, method: str, params: Any) -> Any:
    gate = get_helius_gate()
    await gate.acquire()
    payload = {"jsonrpc": "2.0", "id": method, "method": method, "params": params}
    try:
        async with session.post(
            rpc_url(),
            json=payload,
            timeout=aiohttp.ClientTimeout(total=90),
        ) as resp:
            body = await resp.json()
    except Exception as exc:  # noqa: BLE001
        gate.note_error(str(exc)[:120])
        raise
    if body.get("error"):
        raise RuntimeError(body["error"])
    return body.get("result")


async def fetch_all_token_accounts(
    session: aiohttp.ClientSession, mint: str
) -> tuple[list[dict], int]:
    """
    Все токен-аккаунты mint через DAS getTokenAccounts.

    ВАЖНО: result['total'] ВРЁТ — часто равен len текущей страницы, не размеру
    множества. Истинный счёт = сумма len(token_accounts) по страницам до первой
    пустой. Не использовать total для остановки или отчёта.
    """
    pages = 0
    all_acc: list[dict] = []
    page = 1
    while True:
        result = await rpc(
            session,
            "getTokenAccounts",
            {"mint": mint, "page": page, "limit": PAGE_LIMIT},
        )
        pages += 1
        batch = list((result or {}).get("token_accounts") or [])
        # result.get("total") — игнорируем намеренно (врёт).
        if not batch:
            break
        all_acc.extend(batch)
        if len(batch) < PAGE_LIMIT:
            # последняя неполная страница — дальше пусто
            break
        page += 1
        if page > 500:
            raise RuntimeError("pagination safety stop >500 pages")
    return all_acc, pages


async def fetch_decimals(session: aiohttp.ClientSession, mint: str) -> int:
    result = await rpc(session, "getTokenSupply", [mint, {"commitment": "confirmed"}])
    val = (result or {}).get("value") or {}
    return int(val.get("decimals") or 0)


async def dex_price_liq(session: aiohttp.ClientSession, mint: str) -> tuple[float, float]:
    try:
        async with session.get(
            DEX + mint, timeout=aiohttp.ClientTimeout(total=20)
        ) as resp:
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


def pctile(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    i = min(len(sorted_vals) - 1, max(0, int(round((p / 100) * (len(sorted_vals) - 1)))))
    return sorted_vals[i]


def head_break_index(sorted_desc: list[float]) -> Optional[int]:
    """
    Индекс, после которого «голова» обрывается: первое место, где баланс
    падает ниже 10% от предыдущего И ниже 1% от максимума.
    None → явного обрыва нет (плоско / плавно).
    """
    if len(sorted_desc) < 3:
        return None
    mx = sorted_desc[0]
    for i in range(1, len(sorted_desc)):
        prev, cur = sorted_desc[i - 1], sorted_desc[i]
        if prev <= 0:
            continue
        if cur / prev < 0.10 and cur / mx < 0.01:
            return i
    return None


async def classify_large(
    session: aiohttp.ClientSession,
    rows: list[dict],
    *,
    price: float,
    supply_ui: float,
    pool_account: Optional[str],
    top_k: int = 40,
) -> list[dict]:
    """Классификация топ-K по USD: программа владельца, доля supply."""
    ranked = sorted(rows, key=lambda r: -r["ui"])[:top_k]
    addrs = [r["address"] for r in ranked]
    token_owners = await resolve_token_owners(session, addrs)
    unique_owners = list({o for o in token_owners.values() if o})
    programs = await resolve_account_programs(session, unique_owners)

    out: list[dict] = []
    for r in ranked:
        addr = r["address"]
        own = token_owners.get(addr)
        prog = programs.get(own) if own else None
        pname = PROGRAM_NAMES.get(prog or "", "unknown_program" if prog else "no_owner")
        if addr == pool_account:
            tag = "pool_resolved"
        elif addr in BURN_ADDRESSES or (own and own in BURN_ADDRESSES):
            tag = "burn"
        elif prog and prog in AMM_PROGRAM_IDS:
            tag = "amm"
        elif prog and prog != "11111111111111111111111111111111":
            tag = f"service:{pname}"
        else:
            tag = "wallet"
        share = (r["ui"] / supply_ui) if supply_ui > 0 else 0.0
        out.append(
            {
                "address": addr,
                "owner": own,
                "program": prog,
                "program_name": pname,
                "tag": tag,
                "ui": r["ui"],
                "usd": r["ui"] * price,
                "share_supply": share,
                "exclude_by_50pct": bool(supply_ui > 0 and share > 0.50),
            }
        )
    return out


async def main() -> None:
    mint = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_MINT
    print(f"mint={mint}")

    async with aiohttp.ClientSession(
        headers={"User-Agent": "sniper-das-full-snap/1.0", "Accept": "application/json"}
    ) as session:
        print("… DAS pages")
        raw_acc, n_pages = await fetch_all_token_accounts(session, mint)
        print(f"… pages={n_pages} accounts={len(raw_acc)} (counted by page lengths)")

        decimals = await fetch_decimals(session, mint)
        supply_ui = await fetch_supply(session, mint)
        price, liq = await dex_price_liq(session, mint)
        print(f"… decimals={decimals} supply_ui={supply_ui} price={price} liq={liq}")

        scale = 10 ** decimals if decimals >= 0 else 1
        rows: list[dict] = []
        for a in raw_acc:
            amt = int(a.get("amount") or 0)
            if amt <= 0:
                continue
            ui = amt / scale
            rows.append(
                {
                    "address": a.get("address") or "",
                    "owner": a.get("owner"),
                    "ui": ui,
                    "amount_raw": amt,
                }
            )
        rows.sort(key=lambda r: -r["ui"])
        print(f"… nonzero holders={len(rows)}")

        # largest-accounts control (тот же путь, что прибор)
        largest = await fetch_largest_accounts(session, mint)
        if largest is None:
            raise SystemExit("getTokenLargestAccounts failed")
        # подгон под compute_conc: uiAmount
        largest_norm = []
        for a in largest:
            largest_norm.append(
                {
                    "address": a.get("address"),
                    "uiAmount": float(a.get("uiAmount") or 0),
                }
            )
        pool = await resolve_pool_account(session, mint, largest_norm)
        print(f"… pool_account={pool}")

        # --- 1) сколько не видели ---
        conc20, n20, excl20, top20 = compute_conc(
            largest_norm,
            pool_account=pool,
            supply=supply_ui,
            top_n=int(settings.ACCUM_BAL_TOP_N),
            exclude_share=float(settings.ACCUM_BAL_EXCLUDE_SHARE),
        )

        # full non-float: исключить pool + burn + share>50%
        excluded_full: list[dict] = []
        kept_full: list[dict] = []
        for r in rows:
            addr = r["address"]
            share = (r["ui"] / supply_ui) if supply_ui and supply_ui > 0 else 0.0
            reason = None
            if pool and addr == pool:
                reason = "pool"
            elif addr in BURN_ADDRESSES:
                reason = "burn"
            elif share > float(settings.ACCUM_BAL_EXCLUDE_SHARE):
                reason = f"share>{settings.ACCUM_BAL_EXCLUDE_SHARE}"
            if reason:
                excluded_full.append({**r, "reason": reason, "share": share})
            else:
                kept_full.append(r)
        conc_full = sum(r["ui"] for r in kept_full)
        seen_ratio = (conc20 / conc_full) if conc_full > 0 else float("nan")

        print("\n=== 1. СКОЛЬКО НЕ ВИДЕЛИ ===")
        print(f"conc_top20 (ui)     = {conc20:,.6f}  n={n20}  excluded={excl20}")
        print(f"conc_full_nonfloat  = {conc_full:,.6f}  n={len(kept_full)}")
        print(f"excluded_full n={len(excluded_full)}")
        for e in excluded_full[:10]:
            print(
                f"  excl {e['reason']:12} share={e.get('share', 0):.4f} "
                f"ui={e['ui']:,.2f} usd={e['ui']*price:,.0f} {e['address'][:12]}…"
            )
        if price > 0:
            print(
                f"conc_top20_usd      = ${conc20 * price:,.2f}\n"
                f"conc_full_usd       = ${conc_full * price:,.2f}\n"
                f"seen_ratio top20/full = {seen_ratio:.4f}  "
                f"({100 * seen_ratio:.2f}% явления)"
            )
        else:
            print(f"seen_ratio top20/full = {seen_ratio:.4f} (no price)")

        # --- 2) форма ---
        bals_desc = sorted((r["ui"] for r in kept_full), reverse=True)
        br: Optional[int] = None
        print("\n=== 2. ФОРМА РАСПРЕДЕЛЕНИЯ (non-float) ===")
        if bals_desc:
            s = sum(bals_desc)
            print(f"n={len(bals_desc)}  sum={s:,.4f}")
            print(
                f"max={bals_desc[0]:,.4f}  "
                f"p50={pctile(sorted(bals_desc), 50):,.6f}  "
                f"min={bals_desc[-1]:,.8f}"
            )
            # топ-30 рангов
            print("rank  ui_balance           share_of_nonfloat  usd")
            for i, b in enumerate(bals_desc[:30], 1):
                print(
                    f"{i:4d}  {b:18,.4f}  {b / s:10.4%}  ${b * price:14,.2f}"
                )
            # плоскость в окне «топ-20 как у прибора»
            topn = bals_desc[:19]
            if topn:
                mn, sm = min(topn), sum(topn)
                print(
                    f"\ntop-19 min/sum = {mn / sm:.6f}  (равномерно → 1/19={1/19:.6f})"
                )
                print(
                    f"top-19 max/min = {max(topn) / mn if mn else float('inf'):.3f}"
                )
            br = head_break_index(bals_desc)
            if br is None:
                print(
                    "head_break: НЕ найден (нет обрыва <10% prev и <1% max) — "
                    "голова не отделяется; ближе к плоскому/плавному телу."
                )
            else:
                print(
                    f"head_break: после ранга {br} "
                    f"(ранг {br + 1} = {bals_desc[br]:,.4f} ui, "
                    f"${bals_desc[br] * price:,.2f})"
                )
            # лог-корзины по USD
            if price > 0:
                print("\nUSD log-bins (non-float):")
                edges = [0, 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 1e18]
                labels = [
                    "<$1",
                    "$1–10",
                    "$10–100",
                    "$100–1k",
                    "$1k–10k",
                    "$10k–100k",
                    "$100k–1M",
                    "≥$1M",
                ]
                for lo, hi, lab in zip(edges, edges[1:], labels):
                    sel = [b for b in bals_desc if lo <= b * price < hi]
                    mass = sum(sel) * price
                    print(f"  {lab:12}  n={len(sel):5d}  mass=${mass:14,.0f}")

        # --- 3) кого исключать ---
        print("\n=== 3. СЛУЖЕБНЫЕ / КАНДИДАТЫ НА EXCLUDE (топ-40 по ui) ===")
        classified = await classify_large(
            session, kept_full + excluded_full, price=price, supply_ui=supply_ui or 0.0,
            pool_account=pool, top_k=40,
        )
        # пересортировать по usd
        classified.sort(key=lambda x: -x["usd"])
        tags: dict[str, int] = {}
        for c in classified:
            tags[c["tag"]] = tags.get(c["tag"], 0) + 1
            print(
                f"  {c['tag']:18} share={c['share_supply']:.4%}  "
                f"${c['usd']:12,.0f}  prog={c['program_name']:16}  "
                f"excl50={c['exclude_by_50pct']}  "
                f"ta={c['address'][:10]}… own={(c['owner'] or '')[:10]}…"
            )
        print("tag counts (top-40):", tags)
        non_wallet = [c for c in classified if c["tag"] != "wallet"]
        print(
            f"non-wallet in top-40: {len(non_wallet)} — "
            "это кандидаты на явный exclude помимо share>50%."
        )

        # --- 4) пороги ---
        print("\n=== 4. ДЕРЖАТЕЛИ ВЫШЕ ПОРОГОВ (non-float) ===")
        for thr in THRESHOLDS_USD:
            sel = [r for r in kept_full if r["ui"] * price >= thr]
            mass_ui = sum(r["ui"] for r in sel)
            print(
                f"  ≥ ${thr:>7,.0f}:  n={len(sel):5d}  "
                f"mass_ui={mass_ui:,.4f}  mass_usd=${mass_ui * price:,.0f}  "
                f"share_of_nonfloat={(mass_ui / conc_full) if conc_full else 0:.4%}"
            )
        # TOP_HOLDER_RISK-подобное: сколько ≥$10k
        n_10k = sum(1 for r in kept_full if r["ui"] * price >= 10_000)
        print(
            f"\nTOP_HOLDER_RISK analog: wallets ≥$10k = {n_10k} "
            f"(чужой бот смотрел 3 из топ-200; здесь полный список)."
        )

        # --- бюджет ---
        print("\n=== БЮДЖЕТ (оценка, не стройка) ===")
        print(
            f"эта монета: {n_pages} DAS page(s) @ limit={PAGE_LIMIT} "
            f"+ 1 largest + owners/programs ≈ {n_pages + 3} RPC/snap"
        )
        print(
            "dual path: largest всем active (~530×1), DAS только где "
            "top_floor_usd≥$1 (~13/55 high-base ≈ десятки, не ×3 на всех)."
        )

        out = {
            "mint": mint,
            "n_pages": n_pages,
            "n_accounts_raw": len(raw_acc),
            "n_nonzero": len(rows),
            "decimals": decimals,
            "supply_ui": supply_ui,
            "price_usd": price,
            "liq_usd": liq,
            "pool_account": pool,
            "conc_top20_ui": conc20,
            "conc_full_nonfloat_ui": conc_full,
            "seen_ratio": seen_ratio,
            "conc_top20_usd": conc20 * price,
            "conc_full_usd": conc_full * price,
            "excluded_full": [
                {
                    "address": e["address"],
                    "reason": e["reason"],
                    "ui": e["ui"],
                    "usd": e["ui"] * price,
                    "share": e.get("share"),
                }
                for e in excluded_full
            ],
            "top30_ui": bals_desc[:30] if bals_desc else [],
            "top19_min_over_sum": (min(bals_desc[:19]) / sum(bals_desc[:19]))
            if len(bals_desc) >= 19
            else None,
            "head_break_after_rank": br,
            "thresholds": {
                str(int(t)): {
                    "n": sum(1 for r in kept_full if r["ui"] * price >= t),
                    "mass_usd": sum(r["ui"] for r in kept_full if r["ui"] * price >= t)
                    * price,
                }
                for t in THRESHOLDS_USD
            },
            "classified_top40": classified,
        }
        path = ROOT / "data" / "probe_das_full_snap_2aBJFxND.json"
        path.write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
        print(f"\nwrote {path}")


if __name__ == "__main__":
    asyncio.run(main())
