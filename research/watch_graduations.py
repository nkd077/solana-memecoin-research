"""
Лента градаций: ловим ВСЕ токены, дошедшие до конца кривой.

Зачем отдельно от collect_clusters: тот следит только за монетами,
которые застал с рождения (ликвидность < 3 SOL), и потому видит лишь
часть градаций. Здесь мы держим по каждому мяту в потоке одну цифру —
максимум ликвидности на событиях, где сошёлся инвариант кривой, — и
пишем запись в момент пересечения 85 SOL. Никакой памяти на покупателей,
поэтому можно вести десятки тысяч токенов одновременно.

Почему именно градация: измерение 04.09.2026 показало, что при покупке
на рождении продать можно лишь 8,2% купленного — у 43,5% токенов после
нашей сделки не происходит ни одной другой. Матожидание такой покупки
−91,6%. У градуировавших токенов есть пул на десятки тысяч долларов,
то есть выход физически существует. Это единственная популяция, где
вопрос отбора ещё открыт.

Поток градаций — порядка 800 токенов в сутки против 200 000 запусков.
Именно это сужение и было целью: искать среди немногих, а не перебирать
сотни тысяч.

Запуск: python -m research.watch_graduations --hours 24
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import sys
import time

# ВАЖНО: detach ДО import aiohttp/config. После fork DNS resolver aiohttp
# на macOS часто остаётся сломанным (ClientConnectorDNSError навсегда).
PID_FILE = "data/grads_feed.pid"


def _early_daemonize() -> None:
    if "--daemon" not in sys.argv:
        return
    os.makedirs("data", exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    logf = open("logs/grads_feed.log", "a", buffering=1)
    os.dup2(logf.fileno(), 1)
    os.dup2(logf.fileno(), 2)
    if os.fork() > 0:
        raise SystemExit(0)
    os.setsid()
    if os.fork() > 0:
        raise SystemExit(0)
    with open(PID_FILE, "w") as f:
        f.write(str(os.getpid()))


_early_daemonize()

import aiohttp

from config import settings
from core.ws_scanner import decode_trade_event

OUT = "data/graduations_feed.jsonl"
GRAD_SOL = 84.0          # кривая закрывается на 85, берём с запасом
FORGET_SEC = 3600        # мят, молчавший час, забываем
MAX_TRACKED = 120_000

WS_ENDPOINTS = [
    "wss://api.mainnet-beta.solana.com",
    "wss://solana-rpc.publicnode.com",
    "wss://rpc.ankr.com/solana/ws",
]





async def run(hours, use_helius=False):
    os.makedirs("data", exist_ok=True)
    deadline = time.monotonic() + hours * 3600
    peak: dict[str, float] = {}      # мят -> максимум ликвидности
    seen: dict[str, float] = {}      # мят -> когда видели последний раз
    done: set[str] = set()           # уже записанные градации
    ep = 0
    stats = {"events": 0, "mints": 0, "grads": 0, "reconnects": 0}
    endpoints = ([settings.helius_ws()] if use_helius else []) + WS_ENDPOINTS
    fh = open(OUT, "a")

    print(f"Лента градаций. Порог {GRAD_SOL} SOL, работаю {hours} ч.")
    print("Считаем только события, где сошёлся инвариант кривой.\n", flush=True)

    async with aiohttp.ClientSession() as session:
        last_note = time.monotonic()
        while time.monotonic() < deadline:
            url = endpoints[ep % len(endpoints)]
            try:
                async with session.ws_connect(url, heartbeat=30) as ws:
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
                            # Инвариант обязателен: без него ликвидность
                            # ничего не значит — на этом мы уже трижды
                            # получили ложный результат.
                            if not ev or not ev.get("standard_curve"):
                                continue
                            stats["events"] += 1
                            m = ev["mint"]
                            liq = ev["liquidity_sol"]
                            seen[m] = now
                            if m not in peak:
                                stats["mints"] += 1
                            if liq > peak.get(m, -1):
                                peak[m] = liq
                            if liq >= GRAD_SOL and m not in done:
                                done.add(m)
                                stats["grads"] += 1
                                fh.write(json.dumps({
                                    "mint": m,
                                    "t_utc": time.time(),
                                    "liq_sol": round(liq, 3),
                                    "price_sol": ev["price_sol"],
                                    "wallet": ev["wallet"],
                                    "is_buy": ev["is_buy"],
                                    "sol_amount": round(ev["sol_amount"], 6),
                                    "sig": value.get("signature"),
                                }) + "\n")
                                fh.flush()
                                print(f"  ГРАДАЦИЯ #{stats['grads']}: {m[:16]}… "
                                      f"{liq:.1f} SOL", flush=True)

                        if now - last_note > 600:
                            last_note = now
                            for k in [k for k, v in seen.items()
                                      if now - v > FORGET_SEC]:
                                seen.pop(k, None); peak.pop(k, None)
                            if len(seen) > MAX_TRACKED:
                                seen.clear(); peak.clear()
                            left = (deadline - now) / 3600
                            print(f"  ... в памяти {len(peak)} мятов, всего "
                                  f"видели {stats['mints']}, градаций "
                                  f"{stats['grads']}, осталось {left:.1f} ч",
                                  flush=True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"  ! обрыв на {url.split('//')[1][:26]}: "
                      f"{type(exc).__name__} — меняю узел", flush=True)
                ep += 1
            if time.monotonic() >= deadline:
                break
            stats["reconnects"] += 1
            await asyncio.sleep(min(2 * stats["reconnects"], 20))

    fh.close()
    print(f"\nГотово. Градаций записано {stats['grads']}, "
          f"мятов просмотрено {stats['mints']}, обрывов {stats['reconnects']}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=float, default=24)
    ap.add_argument("--helius", action="store_true")
    ap.add_argument("--daemon", action="store_true",
                    help="detach (setsid); лог — logs/grads_feed.log")
    a = ap.parse_args()
    try:
        asyncio.run(run(a.hours, use_helius=a.helius))
    except KeyboardInterrupt:
        print("\nОстановлено.")
    finally:
        try:
            if os.path.exists(PID_FILE) and open(PID_FILE).read().strip() == str(os.getpid()):
                os.unlink(PID_FILE)
        except OSError:
            pass
