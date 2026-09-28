"""
Проверка гипотезы о согласованных кошельках.

Для каждого раннего покупателя выясняем: когда кошелёк был создан и кто
его пополнил первым. Кошелёк считается свежим, если создан меньше чем за
сутки до покупки. Кластер — несколько свежих кошельков одной монеты,
пополненных с ОДНОГО адреса.

КРИТЕРИИ ЗАФИКСИРОВАНЫ ДО ПРОСМОТРА ДАННЫХ:
  H1. Медианный рост монет с кластером >= 3 выше, чем у монет без
      кластера (<= 1), минимум на 10 процентных пунктов.
  H2. Перестановочный тест: p < 0.05 на 10 000 перестановок.
  H3. В группе с кластером не меньше 30 монет.
  H4. Знак разницы сохраняется в обеих половинах выборки по времени.
Провал любого из четырёх = гипотеза закрыта.

Разрешение кошельков кэшируется в data/wallet_funders.json — повторный
запуск не тратит запросы заново.

Запуск: python -m research.analyze_clusters
"""
from __future__ import annotations

import asyncio
import collections
import json
import os
import random
import statistics
import sys

import aiohttp

from config import settings

SRC = "data/clusters.jsonl"
CACHE = "data/wallet_funders.json"
FRESH_SEC = 24 * 3600
MIN_BUYERS = 5          # кластер невозможен, если покупателей меньше
# Биржевой горячий кошелёк финансирует десятки тысяч несвязанных адресов.
# Если его не исключить, Binance склеит пол-выборки в один ложный кластер.
# Вместо списка адресов (который устаревает и в котором легко ошибиться)
# используем правило по вееру: измеренный максимальный размер настоящей
# когорты — 12 кошельков (Kamat, 166 098 минтов). Всё, что финансирует
# заметно больше, — сервис, а не организатор.
MAX_FUNDER_FANOUT = 25
MAX_TOKENS = 800        # потолок, чтобы не утопить квоту RPC
SEED = 20260903
OUTCOME_CP = "900"          # 15 минут
CLUSTER_HI = 3
CLUSTER_LO = 1

_sem = None


async def rpc(session, method, params):
    body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    for attempt in range(4):
        async with _sem:
            try:
                async with session.post(settings.helius_rpc(), json=body,
                                        timeout=25) as r:
                    if r.status == 429:
                        await asyncio.sleep(1.5 * (attempt + 1))
                        continue
                    d = await r.json()
                    return d.get("result")
            except Exception:
                await asyncio.sleep(1.0 * (attempt + 1))
    return None


async def resolve_wallet(session, wallet):
    """Возвращает (birth_unix, funder) или (None, None), если кошелёк старый."""
    sigs = await rpc(session, "getSignaturesForAddress",
                     [wallet, {"limit": 1000}])
    if not sigs:
        return None, None
    if len(sigs) >= 1000:
        return None, None                      # активный кошелёк, не свежий
    oldest = sigs[-1]
    birth = oldest.get("blockTime")
    tx = await rpc(session, "getTransaction",
                   [oldest["signature"],
                    {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0,
                     "commitment": "confirmed"}])
    funder = None
    if tx:
        try:
            msg = tx["transaction"]["message"]
            for ix in msg.get("instructions", []):
                info = (ix.get("parsed") or {}).get("info") or {}
                if info.get("destination") == wallet and info.get("source"):
                    funder = info["source"]
                    break
            if funder is None:
                keys = msg.get("accountKeys", [])
                if keys:
                    first = keys[0]
                    pk = first.get("pubkey") if isinstance(first, dict) else first
                    if pk != wallet:
                        funder = pk
        except Exception:
            pass
    return birth, funder


async def resolve_all(wallets):
    global _sem
    _sem = asyncio.Semaphore(5)
    cache = {}
    if os.path.exists(CACHE):
        try:
            cache = json.load(open(CACHE))
        except Exception:
            cache = {}
    todo = [w for w in wallets if w not in cache]
    print(f"кошельков всего {len(wallets)}, в кэше {len(wallets) - len(todo)}, "
          f"запросить {len(todo)}")
    if todo:
        async with aiohttp.ClientSession() as session:
            for i in range(0, len(todo), 50):
                chunk = todo[i:i + 50]
                res = await asyncio.gather(
                    *[resolve_wallet(session, w) for w in chunk])
                for w, (birth, funder) in zip(chunk, res):
                    cache[w] = {"birth": birth, "funder": funder}
                json.dump(cache, open(CACHE, "w"))
                print(f"  разрешено {min(i + 50, len(todo))}/{len(todo)}",
                      flush=True)
    return cache


def growth_of(row):
    p0 = row.get("price_0")
    px = (row.get("prices") or {}).get(OUTCOME_CP) or row.get("last_price")
    if not p0 or not px:
        return None
    return (px - p0) / p0


def permutation_p(a, b, iters=10000):
    obs = statistics.median(a) - statistics.median(b)
    pool = list(a) + list(b)
    n = len(a)
    hits = 0
    for _ in range(iters):
        random.shuffle(pool)
        d = statistics.median(pool[:n]) - statistics.median(pool[n:])
        if abs(d) >= abs(obs):
            hits += 1
    return obs, (hits + 1) / (iters + 1)


def main():
    if not os.path.exists(SRC):
        print("нет данных — сначала собери: ./run_clusters.sh 8")
        return
    allrows = [json.loads(l) for l in open(SRC) if l.strip()]
    rows = [r for r in allrows if len(r.get("buyers") or []) >= MIN_BUYERS]
    print(f"монет собрано {len(allrows)}, с >= {MIN_BUYERS} ранними "
          f"покупателями: {len(rows)}")
    if len(rows) > MAX_TOKENS:
        random.seed(SEED)
        rows = random.sample(rows, MAX_TOKENS)
        rows.sort(key=lambda r: r.get("t0_utc") or 0)
        print(f"взята случайная выборка {MAX_TOKENS} (фиксированное зерно)")
    if not rows:
        return

    wallets = sorted({b[0] for r in rows for b in r["buyers"]})
    cache = asyncio.run(resolve_all(wallets))

    # веер каждого спонсора по всей выборке
    fanout = collections.Counter()
    for r in rows:
        for w, *_ in r["buyers"]:
            f = (cache.get(w) or {}).get("funder")
            if f:
                fanout[f] += 1
    hubs = {f for f, n in fanout.items() if n > MAX_FUNDER_FANOUT}
    if hubs:
        print(f"\nисключено спонсоров-хабов: {len(hubs)} "
              f"(веер > {MAX_FUNDER_FANOUT}), они покрывают "
              f"{sum(fanout[f] for f in hubs)} покупок")
        for f, n in fanout.most_common(5):
            mark = "хаб" if f in hubs else "ок"
            print(f"    {f[:16]}… веер {n:5d}  [{mark}]")

    fresh_n, cluster_n = [], []
    for r in rows:
        t0 = r.get("t0_utc") or 0
        funders = []
        nf = 0
        for w, dt, sol, order in r["buyers"]:
            info = cache.get(w) or {}
            birth, funder = info.get("birth"), info.get("funder")
            if birth and t0 and (t0 - birth) < FRESH_SEC:
                nf += 1
                if funder and funder not in hubs:
                    funders.append(funder)
        top = collections.Counter(funders).most_common(1)
        r["_fresh"] = nf
        r["_cluster"] = top[0][1] if top else 0
        r["_funder"] = top[0][0] if top else None
        fresh_n.append(nf)
        cluster_n.append(r["_cluster"])

    print(f"\nсвежих покупателей на монету: медиана {statistics.median(fresh_n):.0f}, "
          f"макс {max(fresh_n)}")
    print(f"размер кластера по общему спонсору: медиана "
          f"{statistics.median(cluster_n):.0f}, макс {max(cluster_n)}")
    print("распределение кластеров:",
          dict(sorted(collections.Counter(cluster_n).items()))) 

    hi = [r for r in rows if r["_cluster"] >= CLUSTER_HI and growth_of(r) is not None]
    lo = [r for r in rows if r["_cluster"] <= CLUSTER_LO and growth_of(r) is not None]
    ga = [growth_of(r) * 100 for r in hi]
    gb = [growth_of(r) * 100 for r in lo]

    print("\n" + "=" * 68)
    print(f"кластер >= {CLUSTER_HI}: n={len(ga)}"
          + (f"  медиана {statistics.median(ga):+.1f}%" if ga else ""))
    print(f"кластер <= {CLUSTER_LO}: n={len(gb)}"
          + (f"  медиана {statistics.median(gb):+.1f}%" if gb else ""))

    if len(ga) < 5 or len(gb) < 5:
        print("\nданных мало для выводов — продолжай сбор")
        return

    diff, p = permutation_p(ga, gb)
    h1 = diff >= 10.0
    h2 = p < 0.05
    h3 = len(ga) >= 30
    half = len(rows) // 2
    first, second = rows[:half], rows[half:]

    def med_diff(part):
        a = [growth_of(r) * 100 for r in part
             if r["_cluster"] >= CLUSTER_HI and growth_of(r) is not None]
        b = [growth_of(r) * 100 for r in part
             if r["_cluster"] <= CLUSTER_LO and growth_of(r) is not None]
        if len(a) < 3 or len(b) < 3:
            return None
        return statistics.median(a) - statistics.median(b)

    d1, d2 = med_diff(first), med_diff(second)
    h4 = (d1 is not None and d2 is not None and (d1 > 0) == (d2 > 0))

    print("-" * 68)
    print(f"H1  разница медиан >= +10 п.п.   → {'ПРОЙДЕН' if h1 else 'ПРОВАЛЕН'} "
          f"({diff:+.1f} п.п.)")
    print(f"H2  перестановочный тест p<0.05  → {'ПРОЙДЕН' if h2 else 'ПРОВАЛЕН'} "
          f"(p={p:.4f})")
    print(f"H3  n(кластер) >= 30             → {'ПРОЙДЕН' if h3 else 'ПРОВАЛЕН'} "
          f"({len(ga)})")
    print(f"H4  знак устойчив по времени     → {'ПРОЙДЕН' if h4 else 'ПРОВАЛЕН'} "
          + (f"({d1:+.1f} / {d2:+.1f})" if d1 is not None and d2 is not None
             else "(мало данных)"))
    print("-" * 68)
    if h1 and h2 and h3 and h4:
        print("Все критерии пройдены. Признак работает — есть смысл строить дальше.")
    else:
        print("Гипотеза не подтверждена. Код исполнения не пишем.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
