"""
Эмпирическая калибровка порога «победы».

Берём токены, которые бот недавно взял под наблюдение (в кэше лежат их
цены входа), смотрим текущую цену и считаем РЕАЛЬНОЕ распределение роста.
Это отвечает на два вопроса:
  1. какая доля исходов вообще не имеет цены (то есть засчитывается
     поражением не по факту падения, а из-за отсутствия данных);
  2. где разумно ставить планку, чтобы сигнал различал кошельки.

Ничего не меняет, только читает. Запуск через run_calib.sh
"""
import asyncio, json, time
from pathlib import Path
import aiohttp
from config import settings
from core.price_client import PriceClient

OUT = Path("data/calibration.json")


async def main():
    d = json.load(open("data/cache_state.json"))
    pending = []
    for k, v in d.items():
        if not k.startswith("outcome:"):
            continue
        try:
            rec = json.loads(v[0])
            if rec.get("entry_price_usd", 0) > 0:
                pending.append(rec)
        except Exception:
            pass

    print(f"Записей под наблюдением с ценой входа: {len(pending)}")
    if not pending:
        print("Пока нечего калибровать — подожди, пока накопятся наблюдения")
        return

    now = time.time()
    rows, no_price = [], 0
    async with aiohttp.ClientSession() as s:
        pc = PriceClient(s)
        for i, rec in enumerate(pending[:120], 1):
            mint = rec["token_mint"]
            entry = rec["entry_price_usd"]
            age_min = (now - (rec.get("due_at", now) - settings.OUTCOME_CHECK_DELAY_MIN * 60)) / 60
            market = await pc.get_market_info(mint)
            if not market or market.price_usd <= 0:
                no_price += 1
                rows.append({"mint": mint, "growth": None, "age_min": round(age_min, 1)})
                continue
            growth = (market.price_usd - entry) / entry
            rows.append({"mint": mint, "growth": growth, "age_min": round(age_min, 1),
                         "source": market.source})
            if i % 25 == 0:
                print(f"  обработано {i}...")

    have = [r["growth"] for r in rows if r["growth"] is not None]
    print(f"\nПроверено токенов: {len(rows)}")
    print(f"Без данных о цене: {no_price} ({no_price/len(rows)*100:.0f}%) — сейчас все они идут в поражения")
    if not have:
        print("Ни по одному токену цены нет — это само по себе диагноз")
        return

    have.sort()
    n = len(have)
    print(f"\n=== распределение роста по {n} токенам с ценой ===")
    for q, name in [(0.1,'10-й процентиль'),(0.25,'25-й'),(0.5,'медиана'),(0.75,'75-й'),(0.9,'90-й'),(0.95,'95-й'),(0.99,'99-й')]:
        v = have[min(int(n*q), n-1)]
        print(f"  {name:16s} {v*100:+8.1f}%")
    print(f"  максимум         {have[-1]*100:+8.1f}%")

    print(f"\n=== какая доля берёт планку ===")
    for thr in [0.2, 0.3, 0.5, 1.0, 2.0, 5.0]:
        share = sum(1 for g in have if g >= thr) / n
        share_all = sum(1 for g in have if g >= thr) / len(rows)
        mark = "  <-- текущий порог" if abs(thr - settings.OUTCOME_WIN_THRESHOLD_PCT) < 0.01 else ""
        print(f"  +{thr*100:>5.0f}% : {share*100:5.1f}% от токенов с ценой, "
              f"{share_all*100:5.1f}% от всех{mark}")

    with open(OUT, "w", encoding="utf-8") as f:
        json.dump({"checked": len(rows), "no_price": no_price, "rows": rows}, f, ensure_ascii=False, indent=2)
    print(f"\nСохранено в {OUT}")


if __name__ == "__main__":
    import traceback
    try:
        asyncio.run(main())
    except Exception:
        Path("data").mkdir(exist_ok=True)
        Path("data/calib_error.txt").write_text(traceback.format_exc(), encoding="utf-8")
        print(traceback.format_exc())
