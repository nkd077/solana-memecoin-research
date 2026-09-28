"""
Долгий горизонт: что происходит с токеном ПОСЛЕ градации.

Всё, что мы измеряли до сих пор, жило в пределах получаса. Стратегия
«отобрать 5-6 монет и сидеть 2-3 недели» — другой вопрос, и данных по
нему у нас ноль. Этот скрипт их набирает.

ПОЧЕМУ ВПЕРЁД, А НЕ РЕТРОСПЕКТИВНО
Взять список мигрировавших месяц назад проще, но любой такой список
смещён выживанием: умершие токены выпадают из индексов, и выборка
состоит из тех, кто дожил. Мы фиксируем токен В МОМЕНТ ГРАДАЦИИ, когда
исход ещё неизвестен, и потом смотрим, что с ним стало. Цена — время.

ЧТО ЭТО ПРОВЕРЯЕТ
Концентрация не меняет матожидание, она меняет только разброс. Поэтому
вопрос ровно один: положительно ли матожидание сделки «купил на
градации — держал N недель». Если да, стратегия из 5-6 позиций имеет
смысл при готовности к большому разбросу. Если нет — никакая
концентрация этого не исправит.

КРИТЕРИИ ЗАФИКСИРОВАНЫ ДО ДАННЫХ
  H1. Медианная доходность через 7 дней среди градуировавших > 0.
  H2. Хотя бы один признак, наблюдаемый В МОМЕНТ градации, разделяет
      выборку так, что верхняя треть обгоняет нижнюю на 50+ п.п.
  H3. Разделение держится на отложенной по времени половине.
  H4. Не меньше 100 градуировавших токенов в выборке.
Провал H1 закрывает вопрос независимо от остального.

Запуск: python -m research.track_graduates --snapshot   (раз в сутки)
Отчёт:  python -m research.track_graduates --report

Источник когорты: data/graduations_feed.jsonl (чистый инвариант)
+ обогащение из data/clusters.jsonl (buyers/trades).
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import time
from datetime import datetime, timezone

import urllib.error
import urllib.request

SRC = "data/clusters.jsonl"
FEED = "data/graduations_feed.jsonl"   # чистая лента (инвариант → liq≈85 SOL)
COHORT = "data/graduates_cohort.json"
SNAPS = "data/graduates_snapshots.jsonl"
DEX = "https://api.dexscreener.com/latest/dex/tokens/"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"


def http_json(url, timeout=25):
    """GET с разбором JSON. Возвращает (данные, код) — без внешних пакетов."""
    req = urllib.request.Request(url, headers={"User-Agent": UA,
                                               "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8")), r.status
    except urllib.error.HTTPError as e:
        return None, e.code
    except Exception:
        return None, 0


def build_cohort():
    """Собирает градуировавшие токены: сначала чистая лента, потом clusters.

    graduations_feed.jsonl — события, где сошёлся инвариант кривой
    (liq≈85 SOL). Это основной источник. clusters.jsonl с max_liq_std≥84 —
    запасной/обогащающий (buyers, trades), если минт уже в ленте или ещё нет.
    """
    cohort = json.load(open(COHORT)) if os.path.exists(COHORT) else {}
    added = 0

    # 1) Чистая лента градаций
    if os.path.exists(FEED):
        for line in open(FEED):
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except Exception:
                continue
            liq = float(r.get("liq_sol") or 0)
            if liq < 84:
                continue
            m = r.get("mint")
            if not m or m in cohort:
                continue
            cohort[m] = {
                "t_grad_utc": r.get("t_utc"),
                "grad_at_sec": None,
                "price_at_catch": r.get("price_sol"),
                "peak_liq_sol": round(liq, 3),
                "n_buyers_60s": None,
                "sol_bought_60s": float(r.get("sol_amount") or 0) or None,
                "deployer": r.get("wallet"),
                "trades_30m": None,
                "legacy_record": False,
                "source": "graduations_feed",
                "sig": r.get("sig"),
            }
            added += 1

    # 2) clusters.jsonl — доп. минты + обогащение признаков
    if os.path.exists(SRC):
        for line in open(SRC):
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except Exception:
                continue
            std = r.get("max_liq_std")
            if std is None or std < 84:
                continue
            m = r["mint"]
            buyers = r.get("buyers") or []
            enrich = {
                "n_buyers_60s": len(buyers),
                "sol_bought_60s": round(sum(b[2] for b in buyers), 4) if buyers else None,
                "deployer": (buyers[0][0] if buyers else None),
                "trades_30m": r.get("trades"),
                "grad_at_sec": r.get("grad_at_sec"),
                "nonstd_events": r.get("nonstd_events"),
            }
            if m in cohort:
                # не затираем t_grad/price с ленты; дополняем пустые поля
                for k, v in enrich.items():
                    if cohort[m].get(k) is None and v is not None:
                        cohort[m][k] = v
                if not cohort[m].get("t_grad_utc"):
                    cohort[m]["t_grad_utc"] = r.get("t0_utc")
                if not cohort[m].get("price_at_catch"):
                    cohort[m]["price_at_catch"] = r.get("price_0")
                continue
            cohort[m] = {
                "t_grad_utc": r.get("t0_utc"),
                "grad_at_sec": r.get("grad_at_sec"),
                "price_at_catch": r.get("price_0"),
                "peak_liq_sol": round(float(std), 3),
                "n_buyers_60s": enrich["n_buyers_60s"],
                "sol_bought_60s": enrich["sol_bought_60s"],
                "deployer": enrich["deployer"],
                "trades_30m": enrich["trades_30m"],
                "legacy_record": False,
                "nonstd_events": enrich["nonstd_events"],
                "source": "clusters",
            }
            added += 1

    json.dump(cohort, open(COHORT, "w"), indent=1)
    from_feed = sum(1 for v in cohort.values() if v.get("source") == "graduations_feed")
    print(
        f"когорта: {len(cohort)} токенов (+{added} новых; "
        f"из ленты={from_feed}, остальное clusters/прошлые)"
    )
    return cohort


def snapshot(cohort):
    """Снимает текущую цену и ликвидность по всей когорте."""
    mints = list(cohort)
    now = time.time()
    got = 0
    with open(SNAPS, "a") as fh:
        for i in range(0, len(mints), 30):        # DexScreener принимает пачками
            chunk = mints[i:i + 30]
            data, code = http_json(DEX + ",".join(chunk))
            if data is None:
                print(f"  ! пачка {i // 30 + 1}: HTTP {code}, пропускаю")
                time.sleep(2)
                continue
            pairs = (data.get("pairs") or [])
            best = {}
            for p in pairs:
                base = (p.get("baseToken") or {}).get("address")
                liq = ((p.get("liquidity") or {}).get("usd")) or 0
                if base and liq >= (best.get(base, {}).get("liq", -1)):
                    best[base] = {
                        "liq": liq,
                        "price_usd": float(p.get("priceUsd") or 0),
                        "fdv": p.get("fdv"),
                        "vol24": ((p.get("volume") or {}).get("h24")),
                    }
            for m in chunk:
                b = best.get(m)
                fh.write(json.dumps({
                    "ts": now, "mint": m,
                    "price_usd": b["price_usd"] if b else None,
                    "liq_usd": b["liq"] if b else None,
                    "fdv": b["fdv"] if b else None,
                    "vol24": b["vol24"] if b else None,
                    "found": bool(b),
                }) + "\n")
                got += bool(b)
            time.sleep(1.2)
    print(f"снимок {datetime.now(timezone.utc):%Y-%m-%d %H:%M}: "
          f"найдено {got} из {len(mints)}")


def report():
    if not os.path.exists(SNAPS):
        print("снимков ещё нет")
        return
    cohort = json.load(open(COHORT))
    snaps = {}
    for line in open(SNAPS):
        try:
            s = json.loads(line)
        except Exception:
            continue
        snaps.setdefault(s["mint"], []).append(s)

    print(f"когорта {len(cohort)}, со снимками {len(snaps)}")
    first_ts = min((s[0]["ts"] for s in snaps.values()), default=None)
    if not first_ts:
        return
    age_d = (time.time() - first_ts) / 86400
    print(f"возраст наблюдения: {age_d:.1f} суток\n")

    # Настоящая миграция создаёт пул с реальной ликвидностью. Токен,
    # у которого в первом же снимке пул пустой, градацию не проходил —
    # это остаток ошибки разбора, и в статистику доходности он не идёт.
    confirmed = {m for m, ss in snaps.items()
                 if ss and ss[0].get("liq_usd") and ss[0]["liq_usd"] >= 5000}
    print(f"подтверждённых пулом (ликвидность >= $5000 в первом снимке): "
          f"{len(confirmed)} из {len(snaps)}")
    snaps = {m: ss for m, ss in snaps.items() if m in confirmed} or snaps
    alive = [m for m, ss in snaps.items() if ss[-1]["found"]]
    print(f"ещё торгуются: {len(alive)} из {len(snaps)} "
          f"({100 * len(alive) / len(snaps):.0f}%)")

    rets = []
    for m, ss in snaps.items():
        a = next((s for s in ss if s["found"] and s["price_usd"]), None)
        b = ss[-1]
        if not a:
            continue
        if not b["found"] or not b["price_usd"]:
            rets.append(-100.0)          # исчез с бирж — считаем полной потерей
        else:
            rets.append((b["price_usd"] / a["price_usd"] - 1) * 100)
    if len(rets) < 5:
        print("\nмало данных для выводов — снимай раз в сутки")
        return
    s = sorted(rets)
    q = lambda p: s[min(len(s) - 1, int(p * len(s)))]
    print(f"\nдоходность от первого снимка (n={len(rets)}):")
    print(f"  медиана {statistics.median(rets):+.1f}%   среднее {statistics.mean(rets):+.1f}%")
    print(f"  p10 {q(.1):+.1f}%  p25 {q(.25):+.1f}%  p75 {q(.75):+.1f}%  p90 {q(.90):+.1f}%")
    print(f"  в плюсе {100 * sum(1 for x in rets if x > 0) / len(rets):.0f}%   "
          f"лучший {max(rets):+.0f}%")
    print(f"\n  H1 медиана > 0 → {'ПОКА ПРОХОДИТ' if statistics.median(rets) > 0 else 'ПОКА ПРОВАЛЕН'}")
    print(f"  H4 n >= 100    → {'ПРОЙДЕН' if len(rets) >= 100 else f'ещё нет ({len(rets)})'}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", action="store_true")
    ap.add_argument("--report", action="store_true")
    a = ap.parse_args()
    os.makedirs("data", exist_ok=True)
    if a.report:
        report()
    else:
        snapshot(build_cohort())
