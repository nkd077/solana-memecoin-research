"""
Диагностика: что реально присылает Helius по транзакциям Pump.fun.

Запусти:  python -m tools.diagnose_parser
Результат: файл data/helius_raw_dump.json — его прочитает Claude.
Никаких сделок не совершает, только читает публичные данные.
"""
import asyncio
import json
from pathlib import Path

import aiohttp

from config import settings

OUT_PATH = Path("data/helius_raw_dump.json")
HELIUS_PARSE_URL = "https://api.helius.xyz/v0/transactions"


async def main():
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    async with aiohttp.ClientSession() as session:
        # 1. Свежие сигнатуры программы Pump.fun
        body = {
            "jsonrpc": "2.0", "id": 1,
            "method": "getSignaturesForAddress",
            "params": [settings.CHAIN_SCAN_PROGRAM_ID, {"limit": 40}],
        }
        async with session.post(settings.helius_rpc(), json=body, timeout=20) as resp:
            payload = await resp.json()

        sigs = [i["signature"] for i in (payload.get("result") or []) if not i.get("err")]
        print(f"Получено сигнатур: {len(sigs)}")
        if not sigs:
            print("Пусто — нечего разбирать")
            return

        # 2. Разбор через Enhanced Transactions API
        url = f"{HELIUS_PARSE_URL}?api-key={settings.HELIUS_API_KEY}"
        async with session.post(url, json={"transactions": sigs[:30]}, timeout=30) as resp:
            print(f"Enhanced API статус: {resp.status}")
            parsed = await resp.json()

    # 3. Сохраняем всё как есть + короткую сводку
    summary = []
    for tx in parsed if isinstance(parsed, list) else []:
        swap = (tx.get("events") or {}).get("swap") or {}
        summary.append({
            "signature": (tx.get("signature") or "")[:20] + "...",
            "type": tx.get("type"),
            "source": tx.get("source"),
            "description": (tx.get("description") or "")[:180],
            "has_swap_event": bool(swap),
            "nativeInput": swap.get("nativeInput"),
            "nativeOutput": swap.get("nativeOutput"),
            "tokenInputs_mints": [t.get("mint") for t in (swap.get("tokenInputs") or [])],
            "tokenOutputs_mints": [t.get("mint") for t in (swap.get("tokenOutputs") or [])],
            "innerSwaps_count": len(swap.get("innerSwaps") or []),
        })

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump({"summary": summary, "raw": parsed}, f, ensure_ascii=False, indent=2)

    print(f"\nСохранено в {OUT_PATH}")
    print(f"Транзакций разобрано: {len(summary)}")
    types = {}
    for s in summary:
        key = f"{s['type']} / {s['source']}"
        types[key] = types.get(key, 0) + 1
    print("\nТипы транзакций:")
    for k, v in sorted(types.items(), key=lambda x: -x[1]):
        print(f"  {v:3d}  {k}")


if __name__ == "__main__":
    asyncio.run(main())
