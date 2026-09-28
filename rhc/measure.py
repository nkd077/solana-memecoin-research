"""
Измерение рынка PONS той же линейкой, что применяли к Pump.fun.

Задача: узнать распределение исходов ранней покупки, не строя бота.
На Solana для этого пришлось сутки собирать данные вживую. Здесь EVM,
история доступна запросом — считаем по уже случившимся запускам.

Ход работы:
  1. берём запуски примерно сорокаминутной давности;
  2. для каждого тянем логи его кривой от запуска и дальше;
  3. восстанавливаем цену по событиям торговли;
  4. считаем, что стало с ценой через 30 минут после первой покупки.

Ничего не покупает, только читает.
"""
import json, time
from collections import Counter, defaultdict
from pathlib import Path
from urllib.request import Request, urlopen

RPC = ["https://rpc.mainnet.chain.robinhood.com",
       "https://robinhoodchain.blockscout.com/api/eth-rpc"]
FACTORY = "0x7ed598bcef8bd9edd8c97a195c6d13f40801ec7e"
LAUNCH_TOPIC = "0x8d4aad4953d0ca700d468f3753aa14432d1b35b43ec6409f051fb6aa43a89607"
H = {"Content-Type": "application/json", "Accept": "application/json",
     "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"}
OUT = Path("data/rhc_measure.json")
BLOCK_SEC = 0.25          # уточним по факту


def rpc(method, params, _c={}):
    body = json.dumps({"jsonrpc":"2.0","id":1,"method":method,"params":params}).encode()
    for url in ([_c["ok"]] if "ok" in _c else RPC):
        try:
            with urlopen(Request(url, data=body, headers=H), timeout=30) as r:
                out = json.loads(r.read())
            if "error" in out:
                return out
            _c["ok"] = url
            return out
        except Exception:
            _c.pop("ok", None)
    return {"error": {"message": "RPC недоступен"}}


def get_logs(frm, to, address=None, topics=None):
    p = {"fromBlock": hex(frm), "toBlock": hex(to)}
    if address: p["address"] = address
    if topics: p["topics"] = topics
    r = rpc("eth_getLogs", [p])
    return r.get("result") or []


def block_time(bn):
    r = rpc("eth_getBlockByNumber", [hex(bn), False])
    res = r.get("result") or {}
    return int(res.get("timestamp", "0x0"), 16)


def main():
    head = int(rpc("eth_blockNumber", [])["result"], 16)
    t_now, t_old = block_time(head), block_time(head - 4000)
    global BLOCK_SEC
    if t_now and t_old:
        BLOCK_SEC = (t_now - t_old) / 4000
    print(f"блок {head}, время блока {BLOCK_SEC:.3f} сек")
    per_30min = int(30 * 60 / BLOCK_SEC)
    print(f"30 минут = {per_30min} блоков\n")

    # запуски, у которых уже прошло 30+ минут
    start = head - per_30min - 2000
    launches = get_logs(start, start + 2000, FACTORY, [LAUNCH_TOPIC])
    print(f"запусков в окне: {len(launches)}")
    if not launches:
        print("пусто — возможно, окно слишком старое для публичного RPC")
        return

    # Разбираем роли адресов. Прошлый заход показал, что у topic1 идут
    # события Transfer (то есть это сам токен), поэтому кривую ищем среди
    # остальных — по событиям, ОТЛИЧНЫМ от стандартного Transfer.
    TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    sample = launches[0]
    bn0 = int(sample["blockNumber"], 16)
    print("роли адресов в событии запуска:")
    curve_idx = None
    for i in (1, 2, 3):
        addr = "0x" + sample["topics"][i][-40:]
        lg = get_logs(bn0, bn0 + 3000, addr)
        kinds = Counter(l["topics"][0] for l in lg if l.get("topics"))
        non_transfer = sum(n for t, n in kinds.items() if t != TRANSFER)
        role = "токен (Transfer)" if kinds.get(TRANSFER) and not non_transfer else \
               ("кривая (свои события)" if non_transfer else "нет логов — вероятно кошелёк")
        print(f"   topic{i} {addr}: логов {len(lg)}, из них не-Transfer {non_transfer} -> {role}")
        if non_transfer and curve_idx is None:
            curve_idx = i
    if curve_idx is None:
        print("\nсобственных событий кривой не найдено — торговля идёт не через отдельный контракт")
        print("значит цену придётся считать из переводов токена и ETH")
        curve_idx = 2
    print(f"=> берём кривой topic{curve_idx}\n")

    results = []
    for lg in launches[:20]:
        bn = int(lg["blockNumber"], 16)
        curve = "0x" + lg["topics"][curve_idx][-40:]
        token = "0x" + lg["topics"][1][-40:]
        trades = get_logs(bn, bn + per_30min, curve)
        kinds = Counter(l["topics"][0] for l in trades if l.get("topics"))
        results.append({"token": token, "curve": curve, "n": len(trades),
                        "kinds": dict(kinds),
                        "sample": trades[0] if trades else None,
                        "last": trades[-1] if trades else None})
        print(f"  {token[:12]}... событий кривой за 30 мин: {len(trades):4d}  типов: {len(kinds)}")

    alive = [r for r in results if r["n"] > 1]
    print(f"\nразобрано {len(results)}, с активностью {len(alive)}")
    if alive:
        allkinds = Counter()
        for r in alive: allkinds.update(r["kinds"])
        print("\nтипы событий кривой (topic0 -> сколько раз):")
        for t, n in allkinds.most_common(6):
            print(f"   {n:5d}  {t}")
        s = alive[0]["sample"]
        d = s.get("data","0x")[2:]
        print(f"\nпример события кривой: тем {len(s['topics'])}, слов данных {len(d)//64}")
        for i in range(0, min(len(d), 64*6), 64):
            v = int(d[i:i+64], 16)
            print(f"   слово {i//64}: {v}   (~{v/1e18:.6f} если 18 знаков)")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps({"head": head, "block_sec": BLOCK_SEC,
                                "curve_idx": curve_idx, "results": results},
                               ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"\nсохранено в {OUT}")


if __name__ == "__main__":
    import traceback
    try: main()
    except Exception:
        Path("data").mkdir(exist_ok=True)
        Path("data/rhc_error.txt").write_text(traceback.format_exc(), encoding="utf-8")
        print(traceback.format_exc())
