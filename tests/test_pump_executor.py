"""
Проверка исполнителя PumpPortal БЕЗ реального ключа и денег.

Создаём одноразовый кошелёк, запрашиваем настоящую транзакцию покупки
и проверяем, что защита срабатывает: пропускает честную транзакцию и
отклоняет подделанные условия.

Запуск: ./venv/bin/python test_pump_executor.py
"""
import asyncio, json
from pathlib import Path
import aiohttp
from solders.keypair import Keypair

from config import settings
from core.pump_executor import PumpExecutor
from core.price_client import PriceClient
from core.pump_curve import PumpCurveClient
from core.tx_parser import TxParser

OUT = Path("data/pump_executor_test.json")
results = []


def check(name, cond, detail=""):
    mark = "✅" if cond else "❌"
    print(f"{mark} {name}" + (f" — {detail}" if detail else ""))
    results.append({"test": name, "passed": bool(cond), "detail": detail})
    return cond


async def find_live_curve_token(session):
    parser = TxParser(session)
    curve = PumpCurveClient(session)
    price = PriceClient(session)
    sol = await price.get_sol_price_usd()
    body = {"jsonrpc":"2.0","id":1,"method":"getSignaturesForAddress",
            "params":[settings.CHAIN_SCAN_PROGRAM_ID, {"limit": 80}]}
    async with session.post(settings.helius_rpc(), json=body, timeout=20) as r:
        payload = await r.json()
    sigs = [x["signature"] for x in (payload.get("result") or []) if not x.get("err")]
    buys, _ = await parser.parse_trades(sigs[:40])
    for b in buys:
        st = await curve.get_curve_state(b.token_mint, sol)
        if st and not st.is_complete and st.liquidity_sol > 3:
            return b.token_mint, st
    return None, None


async def main():
    # Сверяем свой base58 с эталонной библиотекой — им кодируются адреса,
    # вытащенные из таблиц, ошибка тут means ложные отказы в проверке
    import base58 as _b58lib, os as _os
    from core.pump_executor import _b58encode
    ok58 = all(_b58encode(r := _os.urandom(32)) == _b58lib.b58encode(r).decode() for _ in range(20))
    check("свой base58 совпадает с библиотечным", ok58)

    throwaway = Keypair()
    print(f"Одноразовый кошелёк (без денег): {throwaway.pubkey()}\n")

    async with aiohttp.ClientSession() as session:
        mint, state = await find_live_curve_token(session)
        if not mint:
            print("Не нашёл активный токен на кривой — запусти ещё раз")
            return
        print(f"Токен для теста: {mint}")
        print(f"  на кривой, ликвидность {state.liquidity_sol:.2f} SOL\n")

        ex = PumpExecutor(session, throwaway)
        AMOUNT_SOL = 0.01

        raw = await ex.build_trade_tx("buy", mint, AMOUNT_SOL, True,
                                      slippage_pct=10, priority_fee_sol=0.00001)
        if not check("PumpPortal вернул транзакцию", raw is not None,
                     f"{len(raw)} байт" if raw else "пусто"):
            return

        # 1. Честная транзакция должна пройти проверку
        v = await ex.verify_transaction(raw, expected_mint=mint, max_sol_allowed=AMOUNT_SOL * 1.5)
        check("честная транзакция проходит проверку", v.ok, v.reason)
        if v.max_sol_cost:
            print(f"     предел трат в транзакции: {v.max_sol_cost:.6f} SOL "
                  f"(просили {AMOUNT_SOL}, допуск {AMOUNT_SOL*1.5:.4f})")

        # 2. Слишком низкий лимит — должна отклониться
        v2 = await ex.verify_transaction(raw, expected_mint=mint, max_sol_allowed=0.0001)
        check("превышение лимита трат ловится", not v2.ok, v2.reason)

        # 3. Чужой токен — должна отклониться
        v3 = await ex.verify_transaction(raw, expected_mint="So11111111111111111111111111111111111111112",
                                   max_sol_allowed=AMOUNT_SOL * 1.5)
        check("подмена токена ловится", not v3.ok, v3.reason)

        # 4. Чужой плательщик — должна отклониться
        other = PumpExecutor(session, Keypair())
        v4 = await other.verify_transaction(raw, expected_mint=mint, max_sol_allowed=AMOUNT_SOL * 1.5)
        check("чужой плательщик ловится", not v4.ok, v4.reason)

        # 4а. Транзакция БЕЗ инструкции покупки должна отклоняться.
        # Это проверка на «отказ по умолчанию»: если предел трат прочитать
        # не удалось, подписывать нельзя. Раньше такая транзакция прошла бы,
        # потому что проверка молча ничего не находила.
        import base64 as _b64
        from solders.transaction import VersionedTransaction as _VT
        from solders.message import MessageV0 as _M
        try:
            _tx = _VT.from_bytes(raw)
            # соберём сообщение без инструкций покупки (оставим только первую,
            # это ComputeBudget — денег не двигает)
            _msg = _tx.message
            _stripped = _M(_msg.header, _msg.account_keys, _msg.recent_blockhash,
                           [_msg.instructions[0]], _msg.address_table_lookups)
            _fake = bytes(_VT.populate(_stripped, _tx.signatures))
            v5 = await ex.verify_transaction(_fake, expected_mint=mint, max_sol_allowed=AMOUNT_SOL * 1.5)
            check("транзакция без читаемого предела трат отклоняется", not v5.ok, v5.reason)
        except Exception as _e:
            check("транзакция без читаемого предела трат отклоняется", False,
                  f"не удалось собрать тестовый случай: {type(_e).__name__}: {_e}")

        # 5. Подпись собирается
        signed = ex.sign(raw)
        check("транзакция подписывается", signed is not None,
              f"{len(signed)} символов base64" if signed else "не вышло")

        # 6. Симуляция: на пустом кошельке ДОЛЖНА не пройти (денег нет) —
        #    это подтверждает, что проверка реально исполняется в сети
        if signed:
            ok, msg = await ex.simulate(signed)
            check("симуляция отрабатывает (на пустом кошельке ожидается отказ)",
                  not ok, msg[:160])

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump({"mint": mint, "results": results}, f, ensure_ascii=False, indent=2)
    passed = sum(1 for r in results if r["passed"])
    print(f"\nПройдено {passed} из {len(results)}")
    print("🎉 ВСЁ ХОРОШО" if passed == len(results) else "⚠️ ЕСТЬ ПРОВАЛЫ")


if __name__ == "__main__":
    import traceback
    try:
        asyncio.run(main())
    except Exception:
        OUT.parent.mkdir(parents=True, exist_ok=True)
        OUT.with_name("pump_executor_error.txt").write_text(traceback.format_exc(), encoding="utf-8")
        print(traceback.format_exc())
