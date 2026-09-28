"""
Разведка Robinhood Chain: что за события шлёт фабрика PONS.

Сигнатуры событий не угадываем — вытаскиваем логи фабрики за последние
блоки и смотрим, какие topic0 встречаются и как устроены их данные.
Тот же подход, что сработал с Pump.fun: сначала факты, потом раскладка.

Запуск: ./venv/bin/python rhc/discover.py
"""
import json
from collections import Counter
from pathlib import Path
from urllib.request import Request, urlopen

RPC = "https://rpc.mainnet.chain.robinhood.com"
FACTORY = "0x7ed598bcef8bd9edd8c97a195c6d13f40801ec7e"
OUT = Path("data/rhc_discover.json")


# Публичные RPC часто отклоняют запросы без обычного заголовка браузера —
# urllib по умолчанию представляется как Python-urllib и получает 403.
HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

# Запасные адреса на случай отказа основного
RPC_CANDIDATES = [
    "https://rpc.mainnet.chain.robinhood.com",
    "https://robinhoodchain.blockscout.com/api/eth-rpc",
]


def rpc(method, params, _cache={}):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    last = None
    for url in ([_cache["ok"]] if "ok" in _cache else RPC_CANDIDATES):
        try:
            req = Request(url, data=body, headers=HEADERS)
            with urlopen(req, timeout=30) as r:
                out = json.loads(r.read())
            _cache["ok"] = url
            return out
        except Exception as exc:  # noqa: BLE001
            last = f"{url} -> {type(exc).__name__}: {exc}"
            _cache.pop("ok", None)
    raise RuntimeError(f"ни один RPC не ответил. последняя ошибка: {last}")


def main():
    head = int(rpc("eth_blockNumber", [])["result"], 16)
    print(f"текущий блок: {head}")
    print(f"рабочий RPC: {rpc.__defaults__[0].get('ok')}")

    # пробуем окна разной ширины — публичный RPC ограничен по диапазону
    logs = []
    for span in (2000, 800, 300, 100):
        frm = head - span
        resp = rpc("eth_getLogs", [{
            "fromBlock": hex(frm), "toBlock": hex(head), "address": FACTORY,
        }])
        if "error" in resp:
            print(f"  окно {span} блоков: отказ ({resp['error'].get('message','')[:60]})")
            continue
        logs = resp.get("result") or []
        print(f"  окно {span} блоков: получено {len(logs)} логов")
        if logs:
            break

    if not logs:
        print("\nЛогов нет — либо фабрика другая, либо окно слишком узкое.")
        return

    by_topic = Counter(l["topics"][0] for l in logs if l.get("topics"))
    print(f"\nразных типов событий: {len(by_topic)}")
    for t, n in by_topic.most_common():
        print(f"  {n:5d}  {t}")

    print("\n=== структура самого частого события ===")
    top_topic = by_topic.most_common(1)[0][0]
    sample = next(l for l in logs if l["topics"][0] == top_topic)
    print(f"topic0:  {top_topic}")
    print(f"тем всего: {len(sample['topics'])} (индексированные поля)")
    for i, t in enumerate(sample["topics"][1:], 1):
        print(f"   topic{i}: {t}   -> как адрес: 0x{t[-40:]}")
    data = sample.get("data", "0x")[2:]
    print(f"data: {len(data)//64} слов по 32 байта")
    for i in range(0, min(len(data), 64*8), 64):
        word = data[i:i+64]
        as_int = int(word, 16) if word else 0
        print(f"   слово {i//64}: {as_int}"
              f"{'  (~%.4f если 18 знаков)' % (as_int/1e18) if 0 < as_int < 1e26 else ''}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps({"head": head, "topics": dict(by_topic),
                                "sample": sample, "n_logs": len(logs)},
                               ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nсохранено в {OUT}")


if __name__ == "__main__":
    import traceback
    try:
        main()
    except Exception:
        Path("data").mkdir(exist_ok=True)
        Path("data/rhc_error.txt").write_text(traceback.format_exc(), encoding="utf-8")
        print(traceback.format_exc())
