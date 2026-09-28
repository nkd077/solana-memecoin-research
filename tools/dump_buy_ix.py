"""
Вытаскивает структуру инструкции buy программы Pump.fun из РЕАЛЬНОЙ
успешной транзакции в блокчейне.

Список аккаунтов у Pump.fun менялся между версиями программы, поэтому
собирать его по памяти нельзя — берём с живой сети.

Запуск:  python -m tools.dump_buy_ix
Результат: data/buy_ix_structure.json
"""
import asyncio
import base64
import json
import struct
from pathlib import Path

import aiohttp
import base58
from solders.pubkey import Pubkey

from config import settings

BUY_DISC = bytes.fromhex("66063d1201daebea")
SELL_DISC = bytes.fromhex("33e685a4017f83ad")
PROGRAM = settings.PUMP_FUN_PROGRAM_ID
OUT = Path("data/buy_ix_structure.json")


def pda(seeds, program=PROGRAM):
    addr, _ = Pubkey.find_program_address(seeds, Pubkey.from_string(program))
    return str(addr)


async def rpc(session, method, params):
    async with session.post(settings.helius_rpc(),
                            json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                            timeout=25) as r:
        return await r.json()


async def main():
    async with aiohttp.ClientSession() as session:
        sigs_resp = await rpc(session, "getSignaturesForAddress", [PROGRAM, {"limit": 200}])
        sigs = [s["signature"] for s in (sigs_resp.get("result") or []) if not s.get("err")]
        print(f"Успешных сигнатур получено: {len(sigs)}")

        found = None
        seen_discs = {}          # какие инструкции программы вообще встречаются
        scanned = 0

        for sig in sigs[:60]:
            tx = await rpc(session, "getTransaction",
                           [sig, {"encoding": "json", "maxSupportedTransactionVersion": 0}])
            result = tx.get("result")
            if not result:
                continue
            scanned += 1
            msg = result["transaction"]["message"]
            keys = list(msg["accountKeys"])
            loaded = (result.get("meta") or {}).get("loadedAddresses") or {}
            keys = keys + loaded.get("writable", []) + loaded.get("readonly", [])

            # Инструкции верхнего уровня И вложенные (CPI): покупки часто
            # идут вложенными, когда торгуют через ботов и роутеры
            candidates = list(msg.get("instructions", []))
            for inner in ((result.get("meta") or {}).get("innerInstructions") or []):
                candidates.extend(inner.get("instructions", []))

            for ix in candidates:
                prog_idx = ix.get("programIdIndex")
                if prog_idx is None or prog_idx >= len(keys) or keys[prog_idx] != PROGRAM:
                    continue
                try:
                    data = base58.b58decode(ix.get("data", ""))
                except Exception:
                    continue
                if len(data) < 8:
                    continue
                disc = data[:8].hex()
                name = {BUY_DISC.hex(): "buy", SELL_DISC.hex(): "sell",
                        "181ec828051c0777": "create"}.get(disc, disc)
                seen_discs[name] = seen_discs.get(name, 0) + 1
                if data[:8] == BUY_DISC and not found:
                    found = (sig, keys, ix, data, result)
            if found:
                break

        print(f"Транзакций просмотрено: {scanned}")
        print("Встреченные инструкции Pump.fun:")
        for k, v in sorted(seen_discs.items(), key=lambda x: -x[1]):
            print(f"   {v:3d}  {k}")
        print()

        if not found:
            print("Инструкция buy не найдена в последних транзакциях — запусти ещё раз")
            return

        sig, keys, ix, data, result = found
        header = result["transaction"]["message"]["header"]
        num_signers = header["numRequiredSignatures"]
        num_ro_signed = header["numReadonlySignedAccounts"]
        num_ro_unsigned = header["numReadonlyUnsignedAccounts"]
        n_static = len(result["transaction"]["message"]["accountKeys"])

        def role(i):
            signer = i < num_signers
            if i < num_signers:
                writable = i < (num_signers - num_ro_signed)
            elif i < n_static:
                writable = i < (n_static - num_ro_unsigned)
            else:
                writable = False
            return signer, writable

        # Разбираем аргументы: discriminator + amount(u64) + max_sol_cost(u64)
        amount, max_sol_cost = struct.unpack("<QQ", data[8:24]) if len(data) >= 24 else (None, None)

        accounts = [keys[i] for i in ix["accounts"]]
        mint = None
        # mint определим по совпадению PDA кривой
        for a in accounts:
            try:
                if pda([b"bonding-curve", bytes(Pubkey.from_string(a))]) in accounts:
                    mint = a
                    break
            except Exception:
                continue

        known = {
            pda([b"global"]): "global (PDA ['global'])",
            pda([b"__event_authority"]): "event_authority (PDA ['__event_authority'])",
            PROGRAM: "program (сама Pump.fun)",
            "11111111111111111111111111111111": "system_program",
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA": "token_program",
            "SysvarRent111111111111111111111111111111111": "rent_sysvar",
            "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL": "associated_token_program",
        }
        if mint:
            known[mint] = "mint"
            known[pda([b"bonding-curve", bytes(Pubkey.from_string(mint))])] = "bonding_curve (PDA ['bonding-curve', mint])"

        print(f"\nТранзакция: {sig}")
        print(f"Аргументы: amount={amount}, max_sol_cost={max_sol_cost} лампортов")
        print(f"\nАккаунты инструкции buy ({len(accounts)} шт.), в порядке:\n")
        rows = []
        for pos, idx in enumerate(ix["accounts"]):
            addr = keys[idx]
            s, w = role(idx)
            label = known.get(addr, "?")
            flags = ("signer " if s else "") + ("writable" if w else "readonly")
            print(f"  {pos:2d}. {addr}  [{flags:16s}] {label}")
            rows.append({"index": pos, "pubkey": addr, "signer": s, "writable": w, "guess": label})

        OUT.parent.mkdir(parents=True, exist_ok=True)
        with open(OUT, "w", encoding="utf-8") as f:
            json.dump({"signature": sig, "mint": mint, "amount": amount,
                       "max_sol_cost": max_sol_cost, "accounts": rows,
                       "data_hex": data.hex(), "data_len": len(data)}, f, ensure_ascii=False, indent=2)
        print(f"\nСохранено в {OUT}")


if __name__ == "__main__":
    import traceback
    try:
        asyncio.run(main())
    except Exception:
        # Пишем ошибку в файл, а не только в терминал — так её видно
        # с той стороны моста, без копирования вручную
        OUT.parent.mkdir(parents=True, exist_ok=True)
        err = traceback.format_exc()
        with open(OUT.with_name("buy_ix_error.txt"), "w", encoding="utf-8") as f:
            f.write(err)
        print(err)
        print(f"\nОшибка записана в {OUT.with_name('buy_ix_error.txt')}")
