"""
Сквозная проверка новой цепочки на ЖИВЫХ токенах:
  1. сам находит свежие токены Pump.fun в блокчейне
  2. проверяет, что адрес токен-аккаунта кривой выведен верно
     (он должен реально присутствовать среди крупнейших держателей)
  3. прогоняет rug-check и смотрит, не блокирует ли он всё подряд
  4. прогоняет получение цены через price_client

Запуск:  python -m tools.verify_pipeline
Результат: data/pipeline_verify.json
"""
import asyncio
import json
from pathlib import Path

import aiohttp

from config import settings
from core.price_client import PriceClient
from core.pump_curve import derive_curve_address, derive_curve_token_account
from core.rug_checker import RugChecker
from core.tx_parser import TxParser

OUT = Path("data/pipeline_verify.json")


async def find_live_mints(session, tx_parser, limit=5):
    """Берём свежие сигнатуры Pump.fun и вытаскиваем из них mint'ы покупок."""
    body = {"jsonrpc": "2.0", "id": 1, "method": "getSignaturesForAddress",
            "params": [settings.CHAIN_SCAN_PROGRAM_ID, {"limit": 60}]}
    async with session.post(settings.helius_rpc(), json=body, timeout=20) as resp:
        payload = await resp.json()
    sigs = [i["signature"] for i in (payload.get("result") or []) if not i.get("err")]
    buys, _ = await tx_parser.parse_trades(sigs[:40])
    seen, mints = set(), []
    for b in buys:
        if b.token_mint not in seen:
            seen.add(b.token_mint)
            mints.append((b.token_mint, b.sol_amount))
        if len(mints) >= limit:
            break
    return mints


async def main():
    results = {}
    async with aiohttp.ClientSession() as session:
        tx_parser = TxParser(session)
        price_client = PriceClient(session)
        rug = RugChecker(settings.helius_rpc())

        mints = await find_live_mints(session, tx_parser)
        print(f"Найдено свежих токенов: {len(mints)}\n")
        if not mints:
            print("Покупок сейчас не видно — попробуй запустить ещё раз через минуту")
            return

        for mint, sol_amount in mints:
            print(f"=== {mint} (покупка на {sol_amount:.3f} SOL) ===")
            entry = {"buy_sol_amount": sol_amount}

            curve = derive_curve_address(mint)
            curve_ata = derive_curve_token_account(mint)
            entry["curve_address"] = curve
            entry["curve_token_account"] = curve_ata
            print(f"  кривая:      {curve}")
            print(f"  токен-аккаунт кривой: {curve_ata}")

            # Проверка: реально ли этот ATA среди крупнейших держателей
            body = {"jsonrpc": "2.0", "id": 1, "method": "getTokenLargestAccounts", "params": [mint]}
            async with session.post(settings.helius_rpc(), json=body, timeout=15) as resp:
                data = await resp.json()
            accounts = (data.get("result") or {}).get("value") or []
            addrs = [a.get("address") for a in accounts]
            ata_found = curve_ata in addrs
            ata_rank = addrs.index(curve_ata) + 1 if ata_found else None
            entry["holders_count"] = len(accounts)
            entry["curve_ata_found_in_holders"] = ata_found
            entry["curve_ata_rank"] = ata_rank
            print(f"  держателей в выдаче: {len(accounts)}")
            print(f"  ATA кривой найден среди держателей: {'ДА (место %d)' % ata_rank if ata_found else 'НЕТ'}")

            # Rug-check
            rug_result = await rug.full_check(mint)
            entry["rug_check"] = rug_result
            print(f"  rug-check: {'ПРОШЁЛ' if rug_result['is_safe'] else 'ЗАБЛОКИРОВАН'}")
            if rug_result.get("issues"):
                for i in rug_result["issues"]:
                    print(f"     - {i}")
            print(f"     доля топ-холдера (без кривой): {rug_result.get('top_holder_share')}")

            # Цена
            market = await price_client.get_market_info(mint)
            if market:
                entry["market"] = {"price_usd": market.price_usd, "liquidity_usd": market.liquidity_usd,
                                   "source": market.source, "is_bonding_curve": market.is_bonding_curve}
                passes_liq = market.liquidity_usd >= settings.MIN_LIQUIDITY_USD_TO_BUY
                entry["passes_liquidity_gate"] = passes_liq
                print(f"  цена: ${market.price_usd:.10f} | ликвидность ${market.liquidity_usd:,.0f} "
                      f"| источник={market.source}")
                print(f"  порог ликвидности ${settings.MIN_LIQUIDITY_USD_TO_BUY:,.0f}: "
                      f"{'ПРОЙДЕН' if passes_liq else 'не пройден'}")
            else:
                entry["market"] = None
                print("  цена: НЕ ПОЛУЧЕНА")
            print()
            results[mint] = entry

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"Сохранено в {OUT}")


if __name__ == "__main__":
    asyncio.run(main())
