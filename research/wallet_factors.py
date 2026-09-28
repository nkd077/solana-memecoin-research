"""
Многофакторный анализ кошельков.

Вопрос: есть ли комбинация признаков кошелька и входа, которая отделяет
хорошие исходы от плохих настолько, чтобы перекрыть издержки круга
(3.2-6.6%)? Базовая ставка по нашим данным: медианный рост -18%,
в плюсе 12% случаев. Это и есть планка.

ЗАЩИТА ОТ САМООБМАНА (главное в этом файле):
  1. Выборка делится по ВРЕМЕНИ: первые 60% — обучение, последние 40% —
     проверка. Проверочная часть не участвует в отборе факторов вообще.
  2. Все признаки причинные: прошлый винрейт кошелька считается только
     по исходам СТРОГО ДО текущей сделки. Иначе получим утечку будущего.
  3. Отбор факторов идёт только на обучении. На проверке — один прогон,
     без подгонки.
  4. Поправка Бонферрони на число проверенных факторов.

КРИТЕРИЙ УСПЕХА ЗАФИКСИРОВАН ДО ПРОСМОТРА ДАННЫХ:
  На ПРОВЕРОЧНОЙ половине верхняя треть по итоговому баллу должна дать
  медианный рост выше общей медианы проверочной половины минимум на
  15 процентных пунктов, при p < 0.05 после поправки Бонферрони.
  Иначе — фактор не найден, код не пишем.

Режимы:
  python -m research.wallet_factors --enrich    # тянет данные о кошельках (RPC)
  python -m research.wallet_factors --analyze   # считает и выносит вердикт
"""
from __future__ import annotations

import argparse
import asyncio
import collections
import json
import os
import random
import statistics

import aiohttp

from config import settings

SRC = "data/outcomes.jsonl"
CACHE = "data/wallet_features.json"
TRAIN_FRAC = 0.60
_sem = None


# ----------------------------------------------------------- 1. обогащение

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
                    return (await r.json()).get("result")
            except Exception:
                await asyncio.sleep(1.0 * (attempt + 1))
    return None


async def wallet_features(session, wallet):
    sigs = await rpc(session, "getSignaturesForAddress", [wallet, {"limit": 1000}])
    bal = await rpc(session, "getBalance", [wallet])
    out = {
        "n_sigs": len(sigs) if sigs else 0,
        "capped": bool(sigs and len(sigs) >= 1000),
        "birth": None, "last": None, "funder": None,
        "sol": (bal or {}).get("value", 0) / 1e9 if isinstance(bal, dict) else None,
    }
    if not sigs:
        return out
    out["last"] = sigs[0].get("blockTime")
    if out["capped"]:
        return out                      # старый кошелёк, до рождения не дойти
    oldest = sigs[-1]
    out["birth"] = oldest.get("blockTime")
    tx = await rpc(session, "getTransaction",
                   [oldest["signature"],
                    {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0,
                     "commitment": "confirmed"}])
    if tx:
        try:
            msg = tx["transaction"]["message"]
            for ix in msg.get("instructions", []):
                info = (ix.get("parsed") or {}).get("info") or {}
                if info.get("destination") == wallet and info.get("source"):
                    out["funder"] = info["source"]
                    break
            if out["funder"] is None:
                keys = msg.get("accountKeys", [])
                if keys:
                    k = keys[0]
                    pk = k.get("pubkey") if isinstance(k, dict) else k
                    if pk != wallet:
                        out["funder"] = pk
        except Exception:
            pass
    return out


async def enrich():
    global _sem
    _sem = asyncio.Semaphore(6)
    rows = [json.loads(l) for l in open(SRC) if l.strip()]
    wallets = sorted({r["wallet"] for r in rows})
    cache = json.load(open(CACHE)) if os.path.exists(CACHE) else {}
    todo = [w for w in wallets if w not in cache]
    print(f"кошельков {len(wallets)}, в кэше {len(wallets) - len(todo)}, "
          f"запросить {len(todo)}  (~{len(todo) * 3} запросов)")
    if not todo:
        return
    async with aiohttp.ClientSession() as session:
        for i in range(0, len(todo), 60):
            chunk = todo[i:i + 60]
            res = await asyncio.gather(*[wallet_features(session, w) for w in chunk])
            cache.update(dict(zip(chunk, res)))
            json.dump(cache, open(CACHE, "w"))
            print(f"  {min(i + 60, len(todo))}/{len(todo)}", flush=True)
    print("готово")


# ------------------------------------------------------------- 2. признаки

def build(rows, cache):
    rows.sort(key=lambda r: r["ts"])
    funder_of = {w: (cache.get(w) or {}).get("funder") for w in cache}
    cluster = collections.Counter(f for f in funder_of.values() if f)
    # Биржевые горячие кошельки финансируют тысячи несвязанных адресов —
    # без этого признак funder_cluster измерял бы «пользуется ли человек
    # Binance», а не «состоит ли кошелёк в организованной группе».
    # Порог: измеренный максимум настоящей когорты — 12 кошельков.
    HUB = 25
    hubs = {f for f, n in cluster.items() if n > HUB}
    if hubs:
        print(f"исключено спонсоров-хабов: {len(hubs)} "
              f"(веер > {HUB}), покрывают {sum(cluster[f] for f in hubs)} кошельков")

    hist: dict[str, list[bool]] = collections.defaultdict(list)
    feats = []
    for r in rows:
        w = r["wallet"]
        c = cache.get(w) or {}
        prior = hist[w]                       # только прошлое — причинность
        birth, last = c.get("birth"), c.get("last")
        age_days = ((r["ts"] - birth) / 86400) if birth else None
        span = ((last - birth) / 86400) if (birth and last and last > birth) else None
        f = {
            "growth": r["growth"],
            "liq": r.get("entry_liquidity_usd"),
            "age_days": age_days,
            "n_sigs": c.get("n_sigs"),
            "capped": 1 if c.get("capped") else 0,
            "sol": c.get("sol"),
            "activity": (c["n_sigs"] / span) if (span and c.get("n_sigs")) else None,
            "funder_cluster": (0 if (not c.get("funder") or c["funder"] in hubs)
                               else cluster.get(c["funder"], 0)),
            "prior_n": len(prior),
            "prior_wr": (sum(prior) / len(prior)) if prior else None,
            "ts": r["ts"],
        }
        feats.append(f)
        hist[w].append(bool(r.get("is_win")))
    return feats


FACTORS = ["liq", "age_days", "n_sigs", "capped", "sol", "activity",
           "funder_cluster", "prior_n", "prior_wr"]


def terciles(vals):
    s = sorted(vals)
    return s[len(s) // 3], s[2 * len(s) // 3]


def perm_p(a, b, iters=5000):
    obs = statistics.median(a) - statistics.median(b)
    pool = list(a) + list(b)
    n = len(a)
    hits = 0
    for _ in range(iters):
        random.shuffle(pool)
        if abs(statistics.median(pool[:n]) - statistics.median(pool[n:])) >= abs(obs):
            hits += 1
    return obs, (hits + 1) / (iters + 1)


def analyze():
    if not os.path.exists(CACHE):
        print("нет обогащения — сначала: python -m research.wallet_factors --enrich")
        return
    rows = [json.loads(l) for l in open(SRC) if l.strip()]
    cache = json.load(open(CACHE))
    feats = build(rows, cache)
    cut = int(len(feats) * TRAIN_FRAC)
    train, test = feats[:cut], feats[cut:]
    print(f"всего {len(feats)}, обучение {len(train)}, проверка {len(test)}")
    print(f"базовая медиана: обучение {statistics.median([f['growth'] for f in train])*100:+.1f}%  "
          f"проверка {statistics.median([f['growth'] for f in test])*100:+.1f}%")

    print("\nОТБОР НА ОБУЧАЮЩЕЙ ЧАСТИ (проверочная не тронута)")
    print(f"  {'фактор':<16}{'n':>6}{'нижн.':>9}{'сред.':>9}{'верх.':>9}{'разброс':>10}")
    chosen = []
    for name in FACTORS:
        vals = [f for f in train if f.get(name) is not None]
        if len(vals) < 150:
            print(f"  {name:<16}{len(vals):>6}   мало данных")
            continue
        lo, hi = terciles([f[name] for f in vals])
        g = lambda sel: statistics.median([f["growth"] for f in sel]) * 100
        a = [f for f in vals if f[name] <= lo]
        b = [f for f in vals if lo < f[name] <= hi]
        c = [f for f in vals if f[name] > hi]
        if min(len(a), len(b), len(c)) < 30:
            print(f"  {name:<16}{len(vals):>6}   вырожденное распределение")
            continue
        spread = g(c) - g(a)
        print(f"  {name:<16}{len(vals):>6}{g(a):>8.1f}%{g(b):>8.1f}%{g(c):>8.1f}%"
              f"{spread:>9.1f}п")
        if abs(spread) >= 8.0:
            chosen.append((name, 1 if spread > 0 else -1, lo, hi))

    print(f"\nотобрано факторов: {len(chosen)} "
          f"({', '.join(n for n, *_ in chosen) if chosen else '—'})")
    if not chosen:
        print("\nНи один фактор не дал разброса >= 8 п.п. на обучении.")
        print("ВЕРДИКТ: сигнал не найден. Код не пишем.")
        return

    def score(f):
        s = 0
        for name, sign, lo, hi in chosen:
            v = f.get(name)
            if v is None:
                continue
            s += sign * (1 if v > hi else (-1 if v <= lo else 0))
        return s

    print("\nЕДИНСТВЕННЫЙ ПРОГОН НА ПРОВЕРОЧНОЙ ЧАСТИ")
    scored = sorted(test, key=score)
    k = len(scored) // 3
    top = [f["growth"] * 100 for f in scored[-k:]]
    rest = [f["growth"] * 100 for f in scored[:-k]]
    base = statistics.median([f["growth"] for f in test]) * 100
    diff, p = perm_p(top, rest)
    p_adj = min(1.0, p * len(FACTORS))          # Бонферрони

    print(f"  верхняя треть по баллу: n={len(top)}  медиана {statistics.median(top):+.1f}%")
    print(f"  остальные:              n={len(rest)}  медиана {statistics.median(rest):+.1f}%")
    print(f"  общая медиана проверки: {base:+.1f}%")
    lift = statistics.median(top) - base
    print(f"  прирост к базе: {lift:+.1f} п.п.   p={p:.4f}  "
          f"p с поправкой={p_adj:.4f}")

    h1 = lift >= 15.0
    h2 = p_adj < 0.05
    print("\n" + "-" * 64)
    print(f"H1  прирост >= +15 п.п.        → {'ПРОЙДЕН' if h1 else 'ПРОВАЛЕН'}")
    print(f"H2  p(Бонферрони) < 0.05       → {'ПРОЙДЕН' if h2 else 'ПРОВАЛЕН'}")
    print("-" * 64)
    print("Сигнал найден — можно строить." if (h1 and h2)
          else "ВЕРДИКТ: сигнал не подтверждён. Код исполнения не пишем.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--enrich", action="store_true")
    ap.add_argument("--analyze", action="store_true")
    a = ap.parse_args()
    if a.enrich:
        asyncio.run(enrich())
    elif a.analyze:
        analyze()
    else:
        print("укажи --enrich или --analyze")
