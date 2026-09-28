"""
Диагностика вывода токен-аккаунта бондинг-кривой.

Берём токен, который ТОЧНО на кривой (не выпущен), и сравниваем
выведенный ATA с фактическими адресами держателей + выясняем, кто
владелец крупнейшего аккаунта.

Запуск: python -m tools.verify_ata
"""
import asyncio
import json
from pathlib import Path

import aiohttp

from config import settings
from core.pump_curve import PumpCurveClient, derive_curve_address, derive_curve_token_account
from core.price_client import PriceClient
from core.tx_parser import TxParser

OUT = Path("data/ata_verify.json")


async def main():
    out = {}
    async with aiohttp.ClientSession() as session:
        tx_parser = TxParser(session)
        price_client = PriceClient(session)
        curve_client = PumpCurveClient(session)
        sol_price = await price_client.get_sol_price_usd()

        # Найти токен, который реально на кривой (is_complete=False и есть ликвидность)
        body = {"jsonrpc": "2.0", "id": 1, "method": "getSignaturesForAddress",
                "params": [settings.CHAIN_SCAN_PROGRAM_ID, {"limit": 80}]}
        async with session.post(settings.helius_rpc(), json=body, timeout=20) as resp:
            payload = await resp.json()
        sigs = [i["signature"] for i in (payload.get("result") or []) if not i.get("err")]
        buys, _ = await tx_parser.parse_trades(sigs[:50])

        target = None
        for b in buys:
            st = await curve_client.get_curve_state(b.token_mint, sol_price)
            if st and not st.is_complete and st.liquidity_sol > 1.0:
                target = (b.token_mint, st)
                break

        if not target:
            print("Не нашёл активный токен на кривой — попробуй ещё раз")
            return

        mint, state = target
        print(f"Токен на кривой: {mint}")
        print(f"  ликвидность: {state.liquidity_sol:.2f} SOL, выпущен={state.is_complete}")

        curve = derive_curve_address(mint)
        curve_ata = derive_curve_token_account(mint)
        print(f"  кривая (PDA):        {curve}")
        print(f"  выведенный ATA:      {curve_ata}")
        print()

        body = {"jsonrpc": "2.0", "id": 1, "method": "getTokenLargestAccounts", "params": [mint]}
        async with session.post(settings.helius_rpc(), json=body, timeout=15) as resp:
            data = await resp.json()
        accounts = (data.get("result") or {}).get("value") or []

        print("Крупнейшие держатели (адрес токен-аккаунта -> количество):")
        for i, a in enumerate(accounts[:6], 1):
            mark = "  <-- ЭТО ВЫВЕДЕННЫЙ ATA" if a.get("address") == curve_ata else ""
            print(f"  {i}. {a.get('address')}  {a.get('uiAmount')}{mark}")

        # Кто владелец топ-аккаунта?
        top_addr = accounts[0].get("address") if accounts else None
        owner = None
        if top_addr:
            body = {"jsonrpc": "2.0", "id": 1, "method": "getAccountInfo",
                    "params": [top_addr, {"encoding": "jsonParsed"}]}
            async with session.post(settings.helius_rpc(), json=body, timeout=15) as resp:
                d = await resp.json()
            info = (((d.get("result") or {}).get("value") or {}).get("data") or {}).get("parsed", {}).get("info", {})
            owner = info.get("owner")
            print(f"\nВладелец крупнейшего токен-аккаунта: {owner}")
            print(f"Совпадает с PDA кривой: {owner == curve}")

        # Проверим также mint authority новым методом
        body = {"jsonrpc": "2.0", "id": 1, "method": "getAccountInfo",
                "params": [mint, {"encoding": "jsonParsed"}]}
        async with session.post(settings.helius_rpc(), json=body, timeout=15) as resp:
            d = await resp.json()
        minfo = (((d.get("result") or {}).get("value") or {}).get("data") or {}).get("parsed", {}).get("info", {})
        print(f"\nmintAuthority:   {minfo.get('mintAuthority')}")
        print(f"freezeAuthority: {minfo.get('freezeAuthority')}")
        print(f"supply:          {minfo.get('supply')} (decimals={minfo.get('decimals')})")

        out = {
            "mint": mint, "curve_pda": curve, "derived_ata": curve_ata,
            "top_accounts": [{"address": a.get("address"), "uiAmount": a.get("uiAmount")} for a in accounts[:6]],
            "top_account_owner": owner, "owner_matches_curve": owner == curve,
            "derived_ata_in_holders": curve_ata in [a.get("address") for a in accounts],
            "mint_authority": minfo.get("mintAuthority"),
            "freeze_authority": minfo.get("freezeAuthority"),
            "supply": minfo.get("supply"), "decimals": minfo.get("decimals"),
        }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"\nСохранено в {OUT}")


if __name__ == "__main__":
    asyncio.run(main())
