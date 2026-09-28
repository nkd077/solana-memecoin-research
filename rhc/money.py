"""
Сколько денег реально заходит в токен — решающая метрика.

Не нужно разбирать события: баланс контракта кривой в ETH и есть то,
что в неё внесли. Смотрим его через 30 минут после запуска.

Сравнение с Pump.fun: там медианный отслеженный токен имел в кривой
около 16.8 SOL (~$1670), а лучший по доходности квинтиль — до $199.
"""
import json
from pathlib import Path
from urllib.request import Request, urlopen

RPC = ["https://rpc.mainnet.chain.robinhood.com",
       "https://robinhoodchain.blockscout.com/api/eth-rpc"]
FACTORY = "0x7ed598bcef8bd9edd8c97a195c6d13f40801ec7e"
LAUNCH = "0x8d4aad4953d0ca700d468f3753aa14432d1b35b43ec6409f051fb6aa43a89607"
H = {"Content-Type":"application/json","Accept":"application/json",
     "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"}
ETH_USD = 3500.0     # порядок величины, для перевода в доллары

def rpc(m,p,_c={}):
    b=json.dumps({"jsonrpc":"2.0","id":1,"method":m,"params":p}).encode()
    for u in ([_c["ok"]] if "ok" in _c else RPC):
        try:
            with urlopen(Request(u,data=b,headers=H),timeout=30) as r: o=json.loads(r.read())
            if "error" not in o: _c["ok"]=u
            return o
        except Exception: _c.pop("ok",None)
    return {"error":1}

head = int(rpc("eth_blockNumber",[])["result"],16)
per30 = int(30*60/0.101)
start = head - per30 - 2000
r = rpc("eth_getLogs",[{"fromBlock":hex(start),"toBlock":hex(start+2000),
                         "address":FACTORY,"topics":[LAUNCH]}])
launches = r.get("result") or []
print(f"запусков в выборке: {len(launches)}  (возраст ~30-35 минут)\n")

vals = []
for lg in launches[:60]:
    # оба кандидата: сам токен и парный контракт — берём тот, где есть эфир
    best = 0.0
    for i in (1,2):
        addr = "0x"+lg["topics"][i][-40:]
        b = rpc("eth_getBalance",[addr,"latest"]).get("result")
        if b:
            v = int(b,16)/1e18
            best = max(best, v)
    vals.append(best)

vals.sort()
n = len(vals)
if not n:
    print("нет данных"); raise SystemExit
print(f"проверено токенов: {n}\n")
print("СКОЛЬКО ETH В КРИВОЙ ЧЕРЕЗ 30 МИНУТ ПОСЛЕ ЗАПУСКА")
for q,name in [(0.1,'10-й процентиль'),(0.25,'25-й'),(0.5,'МЕДИАНА'),
               (0.75,'75-й'),(0.9,'90-й'),(0.99,'99-й')]:
    v = vals[min(int(n*q), n-1)]
    print(f"  {name:16s} {v:>10.5f} ETH   (~${v*ETH_USD:>9,.0f})")
print(f"  {'максимум':16s} {vals[-1]:>10.5f} ETH   (~${vals[-1]*ETH_USD:>9,.0f})")
print()
for thr in (0.001, 0.01, 0.1, 1.0):
    share = sum(1 for v in vals if v >= thr)/n*100
    print(f"  доля с {thr:>5} ETH и больше (~${thr*ETH_USD:>7,.0f}): {share:5.1f}%")
print(f"\n  совсем пустых (0 ETH): {sum(1 for v in vals if v==0)/n*100:.0f}%")

Path("data").mkdir(exist_ok=True)
Path("data/rhc_money.json").write_text(json.dumps({"values":vals},indent=2),encoding="utf-8")
