"""
Измерение разрыва на градации Pump.fun.

Гипотеза: в момент, когда токен закрывает бондинг-кривую (~85 SOL) и
мигрирует на PumpSwap, возникает механический разрыв — цена на кривой
известна точно, а цена в новом пуле формируется заново. Если разрыв
систематически положительный и больше стоимости круга, это структурная
неэффективность, а не ставка на направление.

Почему это отличается от нашей прошлой (провалившейся) работы: там мы
предсказывали будущее по репутации кошельков. Здесь предсказывать не
нужно — событие видно заранее (ликвидность растёт к 85 SOL на глазах),
а измеряем мы механику, а не намерение.

КРИТЕРИИ ЗАФИКСИРОВАНЫ ДО ПРОСМОТРА ДАННЫХ:
  H1. Медианный доход от цены на кривой до цены через 30 с > 2%
      (2% — консервативная оценка круга: комиссии пула, проскальзывание,
      приоритетная комиссия).
  H2. Эффект сохраняется при делении выборки пополам по времени
      (первая половина против второй), знак не меняется.
  H3. Наблюдений не меньше 30 градаций.
Провал любого из трёх = гипотеза закрыта, код не пишем.

Всё read-only. Ничего не покупается.

Запуск:   python -m research.measure_graduation --hours 6
Отчёт:    python -m research.measure_graduation --report
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import statistics
import time
from datetime import datetime, timezone

import aiohttp

from config import settings
from core.ws_scanner import decode_trade_event

OUT = "data/graduations.jsonl"

# Кривая закрывается около 85 SOL реальных резервов. Взводим раньше,
# чтобы не пропустить токен, проскочивший порог одной крупной покупкой.
ARM_SOL = 75.0
TRIGGER_SOL = 84.0

# Моменты замера цены после градации, секунды
CHECKPOINTS = (1, 2, 5, 15, 30, 60, 180, 300)

JUP_QUOTE = "https://lite-api.jup.ag/swap/v1/quote"
SOL_MINT = "So11111111111111111111111111111111111111112"
PROBE_LAMPORTS = 100_000_000        # 0.1 SOL — типичный размер пробы
TOKEN_UNITS = 1_000_000

_jup_gate = None
_last_call = [0.0]


async def jupiter_price(session, mint):
    """Цена токена в SOL по маршруту Jupiter. None, если маршрута нет."""
    params = {
        "inputMint": SOL_MINT, "outputMint": mint,
        "amount": str(PROBE_LAMPORTS), "slippageBps": "500",
    }
    async with _jup_gate:
        wait = 1.1 - (time.monotonic() - _last_call[0])
        if wait > 0:
            await asyncio.sleep(wait)
        _last_call[0] = time.monotonic()
        try:
            async with session.get(JUP_QUOTE, params=params, timeout=10) as r:
                if r.status != 200:
                    return None
                d = await r.json()
        except Exception:
            return None
    try:
        out = int(d["outAmount"]) / TOKEN_UNITS
        if out <= 0:
            return None
        return (PROBE_LAMPORTS / 1e9) / out
    except Exception:
        return None


async def track(session, mint, p0, armed_at, state, liq=None):
    """Ведёт один токен после срабатывания порога."""
    t0 = time.monotonic()
    record = {
        "mint": mint,
        "t_utc": datetime.now(timezone.utc).isoformat(),
        "price_curve": p0,
        "sec_from_arm": round(time.monotonic() - armed_at, 1),
        "liq_at_trigger": liq,
        "routable_after_sec": None,
        "prices": {},
    }
    first_seen = None

    for cp in CHECKPOINTS:
        delay = cp - (time.monotonic() - t0)
        if delay > 0:
            await asyncio.sleep(delay)
        px = await jupiter_price(session, mint)
        record["prices"][str(cp)] = px
        if px is not None and first_seen is None:
            first_seen = round(time.monotonic() - t0, 1)
            record["routable_after_sec"] = first_seen

    with open(OUT, "a") as fh:
        fh.write(json.dumps(record) + "\n")
    state["done"] += 1
    if record["routable_after_sec"] is None:
        state["never_routable"] += 1
    print(f"  [{state['done']}] {mint[:12]}… кривая={p0:.3e} "
          f"маршрут через {record['routable_after_sec']} с "
          f"цены={ {k: (f'{v:.3e}' if v else None) for k, v in record['prices'].items()} }",
          flush=True)


async def run(hours):
    global _jup_gate
    _jup_gate = asyncio.Semaphore(2)
    os.makedirs("data", exist_ok=True)
    deadline = time.monotonic() + hours * 3600
    seen: dict[str, float] = {}
    last_seen: dict[str, float] = {}
    fired: set[str] = set()
    armed: dict[str, float] = {}
    state = {"done": 0, "never_routable": 0}
    tasks: set[asyncio.Task] = set()
    reconnects = 0

    print(f"Слушаю поток. Порог взведения {ARM_SOL} SOL, "
          f"срабатывание {TRIGGER_SOL} SOL. Работаю {hours} ч.")
    print("Переподключение при обрыве включено.\n", flush=True)

    async with aiohttp.ClientSession() as session:
        last_note = time.monotonic()
        while time.monotonic() < deadline:
            try:
                async with session.ws_connect(settings.helius_ws(),
                                              heartbeat=30) as ws:
                    await ws.send_json({
                        "jsonrpc": "2.0", "id": 1, "method": "logsSubscribe",
                        "params": [{"mentions": [settings.PUMP_FUN_PROGRAM_ID]},
                                   {"commitment": "processed"}],
                    })

                    async for msg in ws:
                        now = time.monotonic()
                        if now > deadline:
                            break
                        if msg.type is not aiohttp.WSMsgType.TEXT:
                            continue
                        try:
                            d = json.loads(msg.data)
                        except Exception:
                            continue
                        if d.get("method") != "logsNotification":
                            continue
                        value = d["params"]["result"].get("value") or {}
                        if value.get("err") is not None:
                            continue

                        for line in value.get("logs") or []:
                            if not line.startswith("Program data: "):
                                continue
                            try:
                                ev = decode_trade_event(base64.b64decode(line[14:]))
                            except Exception:
                                continue
                            if not ev:
                                continue
                            mint, liq = ev["mint"], ev["liquidity_sol"]
                            seen[mint] = liq
                            last_seen[mint] = now

                            if liq >= ARM_SOL and mint not in armed:
                                armed[mint] = now
                            if liq >= TRIGGER_SOL and mint not in fired:
                                fired.add(mint)
                                t = asyncio.create_task(
                                    track(session, mint, ev["price_sol"],
                                          armed.get(mint, now), state, liq))
                                tasks.add(t)
                                t.add_done_callback(tasks.discard)

                        if now - last_note > 300:
                            last_note = now
                            for m in [k for k, v in last_seen.items()
                                      if now - v > 900]:
                                last_seen.pop(m, None)
                                seen.pop(m, None)
                                armed.pop(m, None)
                            left = max(0.0, (deadline - now) / 3600)
                            print(f"  ... взведено {len(armed)}, сработало "
                                  f"{len(fired)}, записано {state['done']}, "
                                  f"обрывов {reconnects}, осталось {left:.1f} ч",
                                  flush=True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"  ! связь оборвалась: {type(exc).__name__}: {exc} — "
                      f"переподключаюсь", flush=True)
            if time.monotonic() >= deadline:
                break
            reconnects += 1
            await asyncio.sleep(min(2 * reconnects, 20))

        if tasks:
            print(f"\nДожидаюсь {len(tasks)} незавершённых наблюдений...",
                  flush=True)
            await asyncio.gather(*tasks, return_exceptions=True)

    print(f"\nГотово. Записано {state['done']}, без маршрута "
          f"{state['never_routable']}, обрывов связи {reconnects}. "
          f"Отчёт: python -m research.measure_graduation --report")


# ------------------------------------------------------------------- отчёт

def report():
    if not os.path.exists(OUT):
        print("Нет данных. Сначала запусти сбор.")
        return
    rows = []
    for line in open(OUT):
        line = line.strip()
        if line:
            try:
                rows.append(json.loads(line))
            except Exception:
                pass
    if not rows:
        print("Файл пуст.")
        return

    print("=" * 72)
    print(f"ГРАДАЦИИ: {len(rows)} наблюдений")
    print("=" * 72)

    never = [r for r in rows if r.get("routable_after_sec") is None]
    print(f"\nБез маршрута Jupiter за 300 с: {len(never)} "
          f"({100 * len(never) / len(rows):.0f}%)")
    lat = [r["routable_after_sec"] for r in rows if r.get("routable_after_sec")]
    if lat:
        print(f"Появление маршрута: медиана {statistics.median(lat):.0f} с, "
              f"мин {min(lat):.0f} с, макс {max(lat):.0f} с")

    # Популяция оказалась неоднородной: часть срабатываний приходит с
    # ликвидностью много выше 85 SOL, что на стандартной кривой Pump.fun
    # невозможно. Значит, в потоке есть запуски с другими параметрами
    # кривой. Мешать их в одну выборку нельзя — считаем отдельно.
    clean = [r for r in rows
             if r.get("liq_at_trigger") is not None and 84 <= r["liq_at_trigger"] < 90]
    dirty = [r for r in rows
             if r.get("liq_at_trigger") is not None and r["liq_at_trigger"] >= 90]
    unknown = [r for r in rows if r.get("liq_at_trigger") is None]
    print(f"\nСЕГМЕНТЫ: стандартная кривая (84-90 SOL) {len(clean)}, "
          f"нестандартная (>=90) {len(dirty)}, без метки {len(unknown)}")

    def seg(name, part):
        if len(part) < 10:
            print(f"  {name}: {len(part)} — мало для выводов")
            return
        line = []
        for cp in ("5", "30", "60", "300"):
            rr = [(r["prices"][cp] - r["price_curve"]) / r["price_curve"] * 100
                  for r in part
                  if r.get("price_curve") and (r.get("prices") or {}).get(cp)]
            line.append(f"{cp}с {statistics.median(rr):+6.1f}%" if rr else f"{cp}с   —")
        print(f"  {name} (n={len(part)}): " + "  ".join(line))

    seg("стандартная кривая", clean)
    seg("нестандартная    ", dirty)
    seg("без метки        ", unknown)

    print("\nДоход от цены на кривой до цены в момент (ВСЯ выборка вместе):")
    print(f"  {'момент':>8} {'n':>5} {'медиана':>10} {'среднее':>10} "
          f"{'>0':>6} {'>2%':>6}")
    per_cp = {}
    for cp in CHECKPOINTS:
        rets = []
        for r in rows:
            p0 = r.get("price_curve")
            px = (r.get("prices") or {}).get(str(cp))
            if p0 and px:
                rets.append((px - p0) / p0 * 100)
        per_cp[cp] = rets
        if not rets:
            print(f"  {cp:>6} с {0:>5}          —")
            continue
        pos = 100 * sum(1 for x in rets if x > 0) / len(rets)
        gt2 = 100 * sum(1 for x in rets if x > 2) / len(rets)
        print(f"  {cp:>6} с {len(rets):>5} {statistics.median(rets):>9.1f}% "
              f"{statistics.mean(rets):>9.1f}% {pos:>5.0f}% {gt2:>5.0f}%")

    # --- проверка критериев ---
    print("\n" + "-" * 72)
    key = 30
    rets = per_cp.get(key, [])
    h3 = len(rows) >= 30
    h1 = bool(rets) and statistics.median(rets) > 2.0
    h2 = None
    if len(rets) >= 10:
        half = len(rets) // 2
        a, b = rets[:half], rets[half:]
        ma, mb = statistics.median(a), statistics.median(b)
        h2 = (ma > 0) == (mb > 0)
        print(f"H2  первая половина {ma:+.1f}%  вторая {mb:+.1f}%  "
              f"→ {'знак совпал' if h2 else 'ЗНАК РАЗОШЁЛСЯ'}")
    print(f"H1  медиана на {key} с > 2%      → "
          f"{'ПРОЙДЕН' if h1 else 'ПРОВАЛЕН'}"
          + (f" ({statistics.median(rets):+.1f}%)" if rets else ""))
    print(f"H3  наблюдений >= 30            → "
          f"{'ПРОЙДЕН' if h3 else f'ПРОВАЛЕН ({len(rows)})'}")
    print("-" * 72)
    if h1 and h2 and h3:
        print("Все критерии пройдены. Есть основание писать код исполнения.")
    elif not h3:
        print("Данных мало — нужно продолжить сбор, выводов пока нет.")
    else:
        print("Гипотеза не подтверждена. Код исполнения не пишем.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=float, default=6)
    ap.add_argument("--report", action="store_true")
    a = ap.parse_args()
    if a.report:
        report()
    else:
        try:
            asyncio.run(run(a.hours))
        except KeyboardInterrupt:
            print("\nОстановлено. Отчёт: python -m research.measure_graduation --report")
