"""
Проверка: правильно ли читается бондинг-кривая Pump.fun.

Эталон — реальная сделка из дампа: 0.5406 SOL за 4 217 317.6 токенов,
то есть цена ~1.2818e-7 SOL за токен. Если чтение кривой даёт близкое
значение, структура аккаунта разобрана верно.

Запуск:  python -m tools.verify_curve
"""
import asyncio
import json
from pathlib import Path

import aiohttp

from core.pump_curve import PumpCurveClient, derive_curve_address
from core.price_client import PriceClient

# Эталон из data/helius_raw_dump.json
REFERENCE_MINT = "FFou1Hg2KzMdXcB5YLMmp7XSjCFCxnfBRNrVJ78Ppump"
REFERENCE_SOL = 0.540584767
REFERENCE_TOKENS = 4217317.60132

# Свежие токены, замеченные ботом в логе
EXTRA_MINTS = [
    "75mRwR45Wb5UDTgazRYBzzKyF6nFNxVjVHffR8t7pump",
    "Hs7N63fmuC2yBp6LAnoHkFwfHu2n4eCf3AEWDAtnpump",
    "5jNB5YFamqs3iifyhs34ERox2En3EBc1v5CdgHkmpump",
]

OUT = Path("data/curve_verify.json")


async def main():
    results = {}
    async with aiohttp.ClientSession() as session:
        price_client = PriceClient(session)
        curve_client = PumpCurveClient(session)

        sol_price = await price_client.get_sol_price_usd()
        print(f"Цена SOL: ${sol_price:.2f}")
        print()

        implied = REFERENCE_SOL / REFERENCE_TOKENS
        print(f"ЭТАЛОН (из реальной сделки): {implied:.10f} SOL за токен")
        print(f"  адрес кривой: {derive_curve_address(REFERENCE_MINT)}")

        state = await curve_client.get_curve_state(REFERENCE_MINT, sol_price)
        if state:
            ratio = state.price_sol / implied if implied else 0
            print(f"ЧТЕНИЕ КРИВОЙ:               {state.price_sol:.10f} SOL за токен")
            print(f"  отношение к эталону: {ratio:.3f}x  (1.0 = точное совпадение)")
            print(f"  ликвидность: {state.liquidity_sol:.2f} SOL (${state.liquidity_usd:,.0f})")
            print(f"  кривая закрыта (выпущен на Raydium): {state.is_complete}")
            verdict = "СОВПАДАЕТ" if 0.5 < ratio < 2.0 else "РАСХОЖДЕНИЕ"
            print(f"  ВЕРДИКТ: {verdict}")
            results["reference"] = {
                "implied_price_sol": implied, "curve_price_sol": state.price_sol,
                "ratio": ratio, "liquidity_sol": state.liquidity_sol,
                "liquidity_usd": state.liquidity_usd, "is_complete": state.is_complete,
                "verdict": verdict,
            }
        else:
            print("ЧТЕНИЕ КРИВОЙ: не удалось (аккаунт не найден — токен мог быть уже выпущен)")
            results["reference"] = {"error": "curve not found"}

        print()
        print("=== свежие токены из лога ===")
        for mint in EXTRA_MINTS:
            st = await curve_client.get_curve_state(mint, sol_price)
            if st:
                print(f"{mint[:14]}… цена=${st.price_usd:.10f}  ликв={st.liquidity_sol:.2f} SOL "
                      f"(${st.liquidity_usd:,.0f})  выпущен={st.is_complete}")
                results[mint] = {"price_usd": st.price_usd, "liquidity_sol": st.liquidity_sol,
                                 "liquidity_usd": st.liquidity_usd, "is_complete": st.is_complete}
            else:
                print(f"{mint[:14]}… кривая не прочитана")
                results[mint] = {"error": "not found"}

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump({"sol_price_usd": sol_price, "results": results}, f, ensure_ascii=False, indent=2)
    print(f"\nСохранено в {OUT}")


if __name__ == "__main__":
    asyncio.run(main())
