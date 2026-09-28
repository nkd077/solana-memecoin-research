"""Что за программа FAdo9NCw... и что она делает в нашей транзакции."""
import asyncio, base64, json, struct
from pathlib import Path
import aiohttp
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction

from config import settings
from core.pump_executor import PumpExecutor, ALLOWED_PROGRAMS
from core.price_client import PriceClient
from core.pump_curve import PumpCurveClient
from core.tx_parser import TxParser

UNKNOWN = "FAdo9NCw1ssek6Z6yeWzWjhLVsr8uiCwcWNUnKgzTnHe"
OUT = Path("data/unknown_program.json")


async def rpc(s, method, params):
    async with s.post(settings.helius_rpc(), json={"jsonrpc":"2.0","id":1,"method":method,"params":params}, timeout=20) as r:
        return await r.json()


async def main():
    out = {}
    async with aiohttp.ClientSession() as s:
        # 1. Что это за аккаунт
        info = await rpc(s, "getAccountInfo", [UNKNOWN, {"encoding": "jsonParsed"}])
        val = (info.get("result") or {}).get("value") or {}
        print(f"=== {UNKNOWN} ===")
        print(f"  исполняемый (программа): {val.get('executable')}")
        print(f"  владелец (загрузчик):    {val.get('owner')}")
        print(f"  баланс: {val.get('lamports', 0)/1e9:.4f} SOL")
        out["account"] = {"executable": val.get("executable"), "owner": val.get("owner"),
                          "lamports": val.get("lamports")}

        # 2. Собираем транзакцию заново и смотрим ВСЕ инструкции
        # Для разбора структуры инструкции годится любой токен Pump.fun —
        # предпочитаем на кривой, но не настаиваем: цель здесь понять, что
        # делает незнакомая программа, а не оценить конкретный токен.
        parser = TxParser(s); curve = PumpCurveClient(s); price = PriceClient(s)
        sol = await price.get_sol_price_usd()
        body = {"jsonrpc":"2.0","id":1,"method":"getSignaturesForAddress",
                "params":[settings.CHAIN_SCAN_PROGRAM_ID, {"limit": 250}]}
        async with s.post(settings.helius_rpc(), json=body, timeout=25) as r:
            payload = await r.json()
        sigs = [x["signature"] for x in (payload.get("result") or []) if not x.get("err")]
        buys, _ = await parser.parse_trades(sigs[:60])
        print(f"\nПокупок в выборке: {len(buys)}")

        mint = None
        for b in buys:                       # сначала ищем на кривой
            st = await curve.get_curve_state(b.token_mint, sol)
            if st and not st.is_complete and st.liquidity_sol > 1:
                mint = b.token_mint
                print(f"Токен на кривой: {mint} ({st.liquidity_sol:.1f} SOL)")
                break
        if not mint and buys:                # иначе берём любой
            mint = buys[0].token_mint
            print(f"Токен (любой из потока): {mint}")
        if not mint:
            print("покупок в выборке нет, запусти ещё раз"); return

        kp = Keypair()
        ex = PumpExecutor(s, kp)
        raw = await ex.build_trade_tx("buy", mint, 0.01, True, 10, 0.00001)
        if not raw:
            print("PumpPortal не отдал транзакцию"); return

        tx = VersionedTransaction.from_bytes(raw)
        keys = await ex._resolve_all_keys(tx.message)
        print(f"\n=== Все инструкции транзакции (токен {mint[:12]}...) ===")
        ins = []
        for i, ix in enumerate(tx.message.instructions):
            prog = keys[ix.program_id_index] if ix.program_id_index < len(keys) else '<неизвестно>'
            data = bytes(ix.data)
            known = "✅ в белом списке" if prog in ALLOWED_PROGRAMS else "❓ НЕИЗВЕСТНАЯ"
            print(f"\n{i}. {prog}  {known}")
            print(f"   аккаунтов: {len(ix.accounts)}, данных: {len(data)} байт")
            print(f"   data (hex, первые 32): {data[:32].hex()}")
            # System transfer? тогда покажем сумму
            if prog == "11111111111111111111111111111111" and len(data) >= 12 and data[:4] == b"\x02\x00\x00\x00":
                lamports = struct.unpack("<Q", data[4:12])[0]
                print(f"   -> перевод SOL: {lamports/1e9:.6f} SOL")
            accs = [keys[a] if a < len(keys) else f'<индекс {a} вне списка>' for a in ix.accounts]
            for a in accs[:6]:
                print(f"      {a}")
            ins.append({"program": prog, "known": prog in ALLOWED_PROGRAMS,
                        "n_accounts": len(ix.accounts), "data_hex": data[:32].hex(),
                        "accounts": accs})
        out["instructions"] = ins
        out["mint"] = mint

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"\nСохранено в {OUT}")


if __name__ == "__main__":
    import traceback
    try:
        asyncio.run(main())
    except Exception:
        Path("data").mkdir(exist_ok=True)
        Path("data/unknown_error.txt").write_text(traceback.format_exc(), encoding="utf-8")
        print(traceback.format_exc())
