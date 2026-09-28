"""
То же самое, но по непрерывной доходности вместо бинарной метки.

Бинарное «победа/поражение» выбрасывает почти всю информацию: рост +29%
и падение -80% для него одинаковы. Средняя доходность несёт кратно больше
сигнала на то же число наблюдений, поэтому этот тест способен различить
навык там, где бинарный ещё слеп.
"""
import json, random
from collections import defaultdict
from pathlib import Path
from statistics import mean, median

rows = []
for line in Path("data/outcomes.jsonl").open(encoding="utf-8"):
    try:
        r = json.loads(line)
        if r.get("wallet") and r.get("growth") is not None:
            rows.append(r)
    except Exception:
        pass
rows.sort(key=lambda r: r.get("ts", 0))

print(f"Наблюдений с известной доходностью: {len(rows)}")
g = [r["growth"] for r in rows]
print(f"средняя доходность: {mean(g)*100:+.1f}%   медиана: {median(g)*100:+.1f}%")

wallets = defaultdict(list)
for r in rows:
    wallets[r["wallet"]].append(r)
multi = {w: rs for w, rs in wallets.items() if len(rs) >= 2}
print(f"кошельков: {len(wallets)}, с 2+ наблюдениями: {len(multi)}\n")

print("=" * 62)
print("ПРЕДСКАЗЫВАЕТ ЛИ СРЕДНЯЯ ДОХОДНОСТЬ ПРОШЛОГО — БУДУЩУЮ")
print("=" * 62)
if len(multi) < 30:
    print(f"кошельков с 2+ наблюдениями {len(multi)} — мало")
    raise SystemExit

pairs = []          # (доходность в прошлом, доходность в будущем)
for rs in multi.values():
    half = max(1, len(rs) // 2)
    first, second = rs[:half], rs[half:]
    if not second:
        continue
    pairs.append((mean(x["growth"] for x in first), mean(x["growth"] for x in second)))

overall = mean(g)
good = [b for a, b in pairs if a > overall]
bad = [b for a, b in pairs if a <= overall]
print(f"кошельков с прошлым ВЫШЕ среднего: {len(good)}")
print(f"кошельков с прошлым НИЖЕ среднего: {len(bad)}")
if good and bad:
    mg, mb = mean(good), mean(bad)
    print(f"\nих будущая доходность:")
    print(f"  бывшие лучше среднего: {mg*100:+.1f}%")
    print(f"  бывшие хуже среднего:  {mb*100:+.1f}%")
    print(f"  разница:               {(mg-mb)*100:+.1f} п.п.")

    allv = good + bad
    k = len(good)
    diff = mg - mb
    cnt = 0
    for _ in range(5000):
        random.shuffle(allv)
        if mean(allv[:k]) - mean(allv[k:]) >= diff:
            cnt += 1
    pv = cnt / 5000
    print(f"  случайность даёт такую разницу в {pv*100:.1f}% случаев (p = {pv:.3f})")
    print()
    if pv < 0.05 and diff > 0:
        print("-> СИГНАЛ ЕСТЬ: доходность кошелька в прошлом предсказывает будущую.")
    elif diff > 0:
        print("-> Направление верное, но неотличимо от шума. Нужно больше данных.")
    else:
        print("-> Связи нет: прошлая доходность не говорит о будущей.")

# Сколько данных нужно, чтобы различить эффект
print("\n" + "=" * 62)
print("СКОЛЬКО ЕЩЁ СОБИРАТЬ")
print("=" * 62)
if good and bad:
    import statistics as st
    pooled = st.pstdev(good + bad) or 1e-9
    d = abs(mean(good) - mean(bad)) / pooled     # величина эффекта
    print(f"наблюдаемая величина эффекта: {d:.3f}")
    if d > 0.01:
        n_need = int(16 / (d ** 2))
        print(f"для надёжного вывода нужно примерно {n_need} кошельков в каждой группе")
        print(f"сейчас: {len(good)} и {len(bad)}")
        rate = len(multi) / (len(rows) / 100)     # кошельков с 2+ на 100 наблюдений
        if rate > 0:
            need_obs = max(0, (n_need * 2 - len(pairs))) / rate * 100
            print(f"это ещё примерно {need_obs:.0f} наблюдений ≈ {need_obs/100:.0f} часов работы")
    else:
        print("эффект настолько мал, что различить его нереально")
