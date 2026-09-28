"""
Сбор ранних покупателей для проверки гипотезы о согласованных кошельках.

Гипотеза (взята из гайда, не наша): если несколько кошельков, купивших
токен в первые секунды, были созданы недавно и пополнены с ОДНОГО адреса
незадолго до запуска — это след организованной группы, и исход такого
токена отличается от базового.

Это принципиально не то, что мы проверяли раньше. Прежде мы оценивали
кошелёк по его прошлым результатам — и это не сработало. Здесь оценка
структурная: кто кого финансировал и когда. Прошлое кошелька не важно.

Что делает скрипт: слушает поток Pump.fun, ловит монеты с самого начала
кривой, записывает всех, кто купил в первые 60 секунд, и следит за ценой
30 минут. Цена берётся из самого потока — запросов к API ноль.

Разрешение источников финансирования — отдельным шагом, в analyze_clusters.py.

Запуск: python -m research.collect_clusters --hours 8
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import time

import aiohttp

from config import settings
from core.ws_scanner import decode_trade_event

CREATE_MARKER = "Instruction: Create"

# Квота Helius кончается быстро: непрерывная подписка съедает её за часы.
# Публичные узлы бесплатны и ключа не требуют — для чтения потока этого
# достаточно. Пробуем по очереди, первый ответивший выигрывает.
WS_ENDPOINTS = [
    "wss://api.mainnet-beta.solana.com",
    "wss://solana-rpc.publicnode.com",
    "wss://rpc.ankr.com/solana/ws",
]

OUT = "data/clusters.jsonl"

BIRTH_LIQ_SOL = 3.0      # ловим монету, только если застали её в начале кривой
BUYER_WINDOW_SEC = 60    # окно записи ранних покупателей
MAX_BUYERS = 40
TRACK_SEC = 1800         # 30 минут наблюдения
CHECKPOINTS = (60, 300, 900, 1800)


class Token:
    __slots__ = ("mint", "t0", "t0_utc", "p0", "buyers", "seen", "prices",
                 "last_price", "last_trade_t", "trades", "gap",
                 "first_liq", "max_liq", "grad_at", "create_blobs", "sig",
                 "std_curve", "nonstd_seen", "max_liq_std", "last_std_liq")

    def __init__(self, mint, t0, p0):
        self.mint = mint
        self.t0 = t0
        self.t0_utc = time.time()
        self.p0 = p0
        self.buyers = []          # (wallet, dt, sol, order)
        self.seen = set()
        self.prices = {}
        self.last_price = p0
        self.last_trade_t = t0
        self.trades = 0
        self.gap = 0.0          # секунд без связи за время наблюдения
        self.first_liq = None   # ликвидность в момент, когда поймали
        self.max_liq = 0.0      # максимум ликвидности за наблюдение
        self.grad_at = None     # через сколько секунд достигли 84 SOL
        self.create_blobs = []  # сырые Program data из транзакции создания
        self.sig = None         # подпись транзакции первого события
        self.std_curve = None   # сходится ли инвариант кривой
        self.nonstd_seen = 0    # сколько событий не сошлось с эталоном
        self.max_liq_std = 0.0  # максимум ликвидности ТОЛЬКО по событиям,
                                # где инвариант кривой сходится — это
                                # единственная величина, которую можно
                                # сравнивать с порогом градации 85 SOL
        self.last_std_liq = None  # последняя ликвидность на стандартной кривой

    def record(self, ev, now):
        dt = now - self.t0
        std = ev.get("standard_curve", True)
        if self.std_curve is None:
            self.std_curve = bool(std)
        if not std:
            self.nonstd_seen += 1
        liq = ev.get("liquidity_sol") or 0.0
        if self.first_liq is None:
            self.first_liq = liq
        if liq > self.max_liq:
            self.max_liq = liq
        if std:
            self.last_std_liq = liq
            if liq > self.max_liq_std:
                self.max_liq_std = liq
        # Градация наблюдается напрямую, а не выводится из цены —
        # вывод из цены зависел от предположения о разрядности токена,
        # на котором мы уже один раз обожглись.
        if self.grad_at is None and std and liq >= 84.0:
            self.grad_at = round(dt, 1)
        self.last_price = ev["price_sol"]
        self.last_trade_t = now
        self.trades += 1
        if ev["is_buy"] and dt <= BUYER_WINDOW_SEC and len(self.buyers) < MAX_BUYERS:
            w = ev["wallet"]
            if w not in self.seen:
                self.seen.add(w)
                self.buyers.append([w, round(dt, 2), round(ev["sol_amount"], 6),
                                    len(self.buyers)])
        for cp in CHECKPOINTS:
            if str(cp) not in self.prices and dt >= cp:
                self.prices[str(cp)] = ev["price_sol"]

    def finish(self, now, partial=False):
        for cp in CHECKPOINTS:
            self.prices.setdefault(str(cp), None)
        return {
            "partial": partial,
            "gap_sec": round(self.gap, 1),
            "first_liq": self.first_liq,
            "max_liq": round(self.max_liq, 3),
            "std_curve": self.std_curve,
            "nonstd_events": self.nonstd_seen,
            "max_liq_std": round(self.max_liq_std, 3),
            "last_std_liq": self.last_std_liq,
            "graduated": self.grad_at is not None,
            "grad_at_sec": self.grad_at,
            "sig": self.sig,
            "create_blobs": self.create_blobs,
            "mint": self.mint,
            "t0_utc": self.t0_utc,
            "price_0": self.p0,
            "prices": self.prices,
            "last_price": self.last_price,
            "silent_sec": round(now - self.last_trade_t, 1),
            "trades": self.trades,
            "buyers": self.buyers,
        }


async def run(hours, use_helius=True):
    os.makedirs("data", exist_ok=True)
    deadline = time.monotonic() + hours * 3600
    live: dict[str, Token] = {}
    skip: set[str] = set()
    state = {"written": 0, "reconnects": 0, "ep": 0, "last_url": None}
    endpoints = ([settings.helius_ws()] if use_helius else []) + WS_ENDPOINTS
    fh = open(OUT, "a")

    print(f"Слушаю поток. Ловлю монеты с ликвидностью < {BIRTH_LIQ_SOL} SOL, "
          f"пишу покупателей первых {BUYER_WINDOW_SEC} с, "
          f"слежу {TRACK_SEC // 60} мин. Работаю {hours} ч.", flush=True)
    print("Переподключение при обрыве включено — наблюдения не теряются.\n",
          flush=True)

    def drain(now, force=False):
        """Записывает дозревшие монеты. force — дописать всё пригодное."""
        limit = 300 if force else TRACK_SEC
        done = [m for m, t in live.items() if now - t.t0 >= limit]
        for m in done:
            tok = live.pop(m)
            fh.write(json.dumps(tok.finish(now, partial=force)) + "\n")
            state["written"] += 1
        if done:
            fh.flush()

    last_note = time.monotonic()
    async with aiohttp.ClientSession() as session:
        while True:
            now = time.monotonic()
            if now > deadline and not live:
                break
            gap_start = now
            url = endpoints[state["ep"] % len(endpoints)]
            try:
                async with session.ws_connect(url, heartbeat=30) as ws:
                    if state["reconnects"] and state.get("last_url") != url:
                        print(f"  подключаюсь к {url.split('//')[1][:34]}", flush=True)
                    state["last_url"] = url
                    await ws.send_json({
                        "jsonrpc": "2.0", "id": 1, "method": "logsSubscribe",
                        "params": [{"mentions": [settings.PUMP_FUN_PROGRAM_ID]},
                                   {"commitment": "processed"}],
                    })
                    gap = time.monotonic() - gap_start
                    if state["reconnects"]:
                        for t in live.values():
                            t.gap += gap

                    async for msg in ws:
                        now = time.monotonic()
                        if now > deadline and not live:
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
                            mint = ev["mint"]
                            tok = live.get(mint)
                            if tok is None:
                                if mint in skip or now > deadline:
                                    continue
                                if ev["liquidity_sol"] > BIRTH_LIQ_SOL:
                                    skip.add(mint)
                                    continue
                                tok = Token(mint, now, ev["price_sol"])
                                tok.sig = value.get("signature")
                                # Дев обычно покупает в той же транзакции,
                                # что и минт, поэтому событие создания часто
                                # лежит в этих же логах. Сохраняем сырьём —
                                # раскладку разберём офлайн, как с трейдами.
                                logs_all = value.get("logs") or []
                                if any(CREATE_MARKER in x for x in logs_all):
                                    tok.create_blobs = [
                                        x[14:] for x in logs_all
                                        if x.startswith("Program data: ")][:3]
                                live[mint] = tok
                            tok.record(ev, now)

                        drain(now)

                        if now - last_note > 300:
                            last_note = now
                            if len(skip) > 20000:
                                skip.clear()
                            left = max(0.0, (deadline - now) / 3600)
                            print(f"  ... в работе {len(live)}, записано "
                                  f"{state['written']}, обрывов "
                                  f"{state['reconnects']}, осталось {left:.1f} ч",
                                  flush=True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                short = str(exc)[:60]
                print(f"  ! обрыв на {url.split('//')[1][:28]}: "
                      f"{type(exc).__name__}: {short} — меняю узел", flush=True)
                state["ep"] += 1

            if time.monotonic() > deadline and not live:
                break
            state["reconnects"] += 1
            await asyncio.sleep(min(2 * state["reconnects"], 20))

    drain(time.monotonic(), force=True)
    fh.close()
    print(f"\nГотово. Записано {state['written']} монет в {OUT} "
          f"(обрывов связи: {state['reconnects']})")
    print("Дальше: python -m research.analyze_clusters")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=float, default=8)
    ap.add_argument("--public-only", action="store_true",
                    help="не трогать Helius, только бесплатные публичные узлы")
    a = ap.parse_args()
    try:
        asyncio.run(run(a.hours, use_helius=not a.public_only))
    except KeyboardInterrupt:
        print("\nОстановлено.")
