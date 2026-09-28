"""
Поправка на десятичные разряды токена.

При сборе мы делили выдачу Jupiter на 10^6, считая, что у всех токенов
Pump.fun шесть разрядов. У части токенов это не так, и их цена уехала
на два-три порядка. На медиану это не влияет (выбросов мало), но
средние и хвосты искажает, поэтому чиним перед финальным разбором.

Запуск: python -m research.fix_decimals
Пишет data/graduations_fixed.jsonl, исходник не трогает.
"""
from __future__ import annotations

import json
import os

import urllib.request

from config import settings

SRC = "data/graduations.jsonl"
DST = "data/graduations_fixed.jsonl"
ASSUMED = 6


def decimals_for(mints):
    out = {}
    for i in range(0, len(mints), 100):
        chunk = mints[i:i + 100]
        body = {"jsonrpc": "2.0", "id": 1, "method": "getMultipleAccounts",
                "params": [chunk, {"encoding": "jsonParsed",
                                   "commitment": "confirmed"}]}
        req = urllib.request.Request(
            settings.helius_rpc(),
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = json.loads(resp.read().decode())
        for mint, acc in zip(chunk, payload["result"]["value"]):
            try:
                out[mint] = acc["data"]["parsed"]["info"]["decimals"]
            except Exception:
                out[mint] = None
        print(f"  разрядность получена для {len(out)}/{len(mints)}")
    return out


def main():
    if not os.path.exists(SRC):
        print("нет данных")
        return
    rows = [json.loads(l) for l in open(SRC) if l.strip()]
    mints = sorted({r["mint"] for r in rows})
    print(f"{len(rows)} строк, {len(mints)} токенов")
    dec = decimals_for(mints)

    fixed = 0
    with open(DST, "w") as fh:
        for r in rows:
            d = dec.get(r["mint"])
            r["decimals"] = d
            if d is not None and d != ASSUMED:
                # цена = SOL / (выдача / 10^assumed); реальная выдача
                # делится на 10^d, значит цену надо умножить на 10^(d-assumed)
                k = 10 ** (d - ASSUMED)
                r["prices"] = {cp: (p * k if p else None)
                               for cp, p in (r.get("prices") or {}).items()}
                r["decimals_corrected"] = True
                fixed += 1
            fh.write(json.dumps(r) + "\n")

    unknown = sum(1 for m in mints if dec.get(m) is None)
    print(f"поправлено строк: {fixed}, разрядность неизвестна у {unknown} токенов")
    print(f"готово: {DST}")


if __name__ == "__main__":
    main()
