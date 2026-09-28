"""
Доля токенов, доходящих до выпуска — прямая мера качества рынка.

Считаем по событиям самой фабрики: сколько запусков и сколько выпусков
за одно и то же окно. На Pump.fun до выпуска доходит около 1%.
"""
import json
from collections import Counter
from pathlib import Path
from urllib.request import Request, urlopen

RPC = ["https://rpc.mainnet.chain.robinhood.com",
       "https://robinhoodchain.blockscout.com/api/eth-rpc"]
FACTORY = "0x7ed598bcef8bd9edd8c97a195c6d13f40801ec7e"
LAUNCH = "0x8d4aad4953d0ca700d468f3753aa14432d1b35b43ec6409f051fb6aa43a89607"
H = {"Content-Type":"application/json","Accept":"application/json",
     "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"}

def rpc(m, p, _c={}):
    b = json.dumps({"jsonrpc":"2.0","id":1,"method":m,"params":p}).encode()
    for u in ([_c["ok"]] if "ok" in _c else RPC):
        try:
            with urlopen(Request(u, data=b, headers=H), timeout=30) as r:
                o = json.loads(r.read())
            if "error" not in o: _c["ok"] = u
            return o
        except Exception: _c.pop("ok", None)
    return {"error":{"message":"нет связи"}}

head = int(rpc("eth_blockNumber",[])["result"],16)
print(f"блок {head}, время блока ~0.101 сек\n")

# собираем события фабрики по кускам, чтобы уложиться в лимит окна
total = Counter()
CHUNK, WINDOWS = 2000, 12          # ~40 минут истории
for i in range(WINDOWS):
    frm = head - CHUNK*(i+1)
    r = rpc("eth_getLogs", [{"fromBlock":hex(frm),"toBlock":hex(frm+CHUNK),"address":FACTORY}])
    for l in (r.get("result") or []):
        total[l["topics"][0]] += 1

minutes = CHUNK*WINDOWS*0.101/60
print(f"окно наблюдения: {CHUNK*WINDOWS} блоков ~ {minutes:.0f} минут\n")
print("события фабрики:")
for t,n in total.most_common():
    mark = "  <- ЗАПУСК" if t == LAUNCH else ""
    print(f"  {n:5d}  {t}{mark}")

launches = total.get(LAUNCH, 0)
print(f"\nзапусков: {launches}  ({launches/minutes*60:.0f} в час, {launches/minutes*1440:.0f} в сутки)")
print("\nостальные события — кандидаты на выпуск/завершение кривой:")
for t,n in total.most_common():
    if t == LAUNCH: continue
    print(f"  {n:5d}  ({n/launches*100:5.2f}% от запусков)  {t}")

Path("data").mkdir(exist_ok=True)
Path("data/rhc_grad.json").write_text(json.dumps(dict(total), indent=2), encoding="utf-8")
