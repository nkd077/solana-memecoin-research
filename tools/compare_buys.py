"""
Сравнивает несколько РАЗНЫХ покупок Pump.fun, чтобы понять структуру:
какие аккаунты постоянные (глобальные константы), а какие выводятся
под конкретный токен/пользователя.

Запуск: ./venv/bin/python -m tools.compare_buys   (или через run_compare.sh)
"""
import asyncio, base64, json, struct
from pathlib import Path
import aiohttp, base58
from solders.pubkey import Pubkey
from config import settings

BUY_DISC = bytes.fromhex("66063d1201daebea")
PROGRAM = settings.PUMP_FUN_PROGRAM_ID
TOKEN_2022 = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
TOKEN_CLASSIC = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
ATA_PROGRAM = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"
OUT = Path("data/buy_compare.json")


def pda(seeds, program=PROGRAM):
    a, _ = Pubkey.find_program_address(seeds, Pubkey.from_string(program))
    return str(a)


def ata(owner, mint, token_program):
    a, _ = Pubkey.find_program_address(
        [bytes(Pubkey.from_string(owner)), bytes(Pubkey.from_string(token_program)),
         bytes(Pubkey.from_string(mint))],
        Pubkey.from_string(ATA_PROGRAM))
    return str(a)


async def rpc(s, method, params):
    async with s.post(settings.helius_rpc(), json={"jsonrpc":"2.0","id":1,"method":method,"params":params}, timeout=25) as r:
        return await r.json()


async def main():
    samples = []
    async with aiohttp.ClientSession() as s:
        resp = await rpc(s, "getSignaturesForAddress", [PROGRAM, {"limit": 300}])
        sigs = [x["signature"] for x in (resp.get("result") or []) if not x.get("err")]
        print(f"Успешных сигнатур: {len(sigs)}")

        for sig in sigs[:80]:
            if len(samples) >= 3:
                break
            tx = await rpc(s, "getTransaction", [sig, {"encoding":"json","maxSupportedTransactionVersion":0}])
            res = tx.get("result")
            if not res:
                continue
            msg = res["transaction"]["message"]
            keys = list(msg["accountKeys"])
            la = (res.get("meta") or {}).get("loadedAddresses") or {}
            keys += la.get("writable", []) + la.get("readonly", [])

            cands = list(msg.get("instructions", []))
            for inner in ((res.get("meta") or {}).get("innerInstructions") or []):
                cands += inner.get("instructions", [])

            for ix in cands:
                pi = ix.get("programIdIndex")
                if pi is None or pi >= len(keys) or keys[pi] != PROGRAM:
                    continue
                try:
                    data = base58.b58decode(ix.get("data",""))
                except Exception:
                    continue
                if data[:8] != BUY_DISC:
                    continue
                accs = [keys[i] for i in ix["accounts"]]
                if len(accs) < 12:
                    continue
                amount, max_sol = struct.unpack("<QQ", data[8:24])
                samples.append({"sig": sig, "accounts": accs, "amount": amount,
                                "max_sol_cost": max_sol, "data_len": len(data)})
                break

    if len(samples) < 2:
        print(f"Найдено покупок: {len(samples)} — нужно минимум 2, запусти ещё раз")
        return

    print(f"Собрано покупок для сравнения: {len(samples)}\n")
    n = len(samples[0]["accounts"])
    print(f"Аккаунтов в инструкции: {[len(x['accounts']) for x in samples]}\n")

    report = []
    for i in range(n):
        vals = [x["accounts"][i] if i < len(x["accounts"]) else None for x in samples]
        constant = len(set(vals)) == 1
        first = vals[0]

        # пробуем опознать
        label = "?"
        mint = samples[0]["accounts"][2]
        user = samples[0]["accounts"][6]
        curve = pda([b"bonding-curve", bytes(Pubkey.from_string(mint))])
        checks = {
            pda([b"global"]): "global",
            pda([b"__event_authority"]): "event_authority",
            PROGRAM: "pump_fun_program",
            "11111111111111111111111111111111": "system_program",
            TOKEN_2022: "TOKEN_2022_program",
            TOKEN_CLASSIC: "token_classic_program",
            ATA_PROGRAM: "associated_token_program",
            mint: "mint",
            user: "user (signer)",
            curve: "bonding_curve",
            ata(curve, mint, TOKEN_2022): "associated_bonding_curve (ATA/2022)",
            ata(user, mint, TOKEN_2022): "associated_user (ATA/2022)",
            ata(curve, mint, TOKEN_CLASSIC): "associated_bonding_curve (ATA/classic)",
            ata(user, mint, TOKEN_CLASSIC): "associated_user (ATA/classic)",
            pda([b"creator-vault", bytes(Pubkey.from_string(user))]): "creator_vault(user?)",
            pda([b"global_volume_accumulator"]): "global_volume_accumulator",
            pda([b"user_volume_accumulator", bytes(Pubkey.from_string(user))]): "user_volume_accumulator",
            pda([b"fee_config"]): "fee_config",
        }
        label = checks.get(first, "?")
        kind = "ПОСТОЯННЫЙ" if constant else "меняется"
        print(f"{i:2d}. {kind:11s} {label:38s} {first}")
        report.append({"index": i, "constant": constant, "label": label, "sample_values": vals})

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump({"samples": samples, "analysis": report}, f, ensure_ascii=False, indent=2)
    print(f"\nСохранено в {OUT}")


if __name__ == "__main__":
    import traceback
    try:
        asyncio.run(main())
    except Exception:
        OUT.parent.mkdir(parents=True, exist_ok=True)
        err = traceback.format_exc()
        OUT.with_name("compare_error.txt").write_text(err, encoding="utf-8")
        print(err)
