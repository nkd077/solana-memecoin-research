"""
Измерение собственной задержки — верхняя граница для любой стратегии,
основанной на скорости.

Зачем это раньше кода стратегии: и кросс-DEX арбитраж, и разрыв на
градации, и листинги упираются в один и тот же вопрос — успеваем ли мы
среагировать раньше, чем неэффективность закроется. Если наш путь от
события до подписанной транзакции длиннее одного слота (~400 мс), то
реактивные стратегии закрыты для нас независимо от качества сигнала,
и мерить сами неэффективности бессмысленно.

Что меряем (всё read-only, деньги не участвуют, приватный ключ не
покидает машину и используется только для получения публичного адреса):

  1. RPC round-trip      — время до узла Helius и обратно
  2. Задержка WS         — от анонса слота до доставки логов этого слота
  3. Отставание от tip   — на сколько слотов мы позади вершины цепи
  4. Разбор события      — наш собственный CPU-путь
  5. Сборка транзакции   — POST в PumpPortal (транзакция НЕ подписывается
                           и НЕ отправляется, это чистое измерение)
  6. Подпись             — одноразовый ключ, не боевой

Не меряется здесь: время от отправки транзакции до попадания в блок.
Его нельзя измерить, не заплатив комиссию. Это отдельный шаг, и он
имеет смысл только если сумма пунктов 2-6 окажется приемлемой.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import json
import statistics
import sys
import time

import aiohttp

from config import settings

PUMP = settings.PUMP_FUN_PROGRAM_ID
TRADE_EVENT_DISC = bytes.fromhex("bddb7fd34ee661ee")
OFF_MINT = 8
OFF_SOL = 40
OFF_TOKENS = 48
OFF_IS_BUY = 56

PUMPPORTAL_URL = "https://pumpportal.fun/api/trade-local"
SLOT_MS = 400.0  # номинальная длительность слота Solana


def pct(values, p):
    if not values:
        return float("nan")
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(round((p / 100.0) * (len(s) - 1)))))
    return s[k]


def describe(name, values, unit="мс"):
    if not values:
        print(f"  {name:<28} нет данных")
        return
    print(
        f"  {name:<28} p50={statistics.median(values):7.1f}  "
        f"p90={pct(values, 90):7.1f}  p99={pct(values, 99):7.1f}  "
        f"max={max(values):7.1f} {unit}  (n={len(values)})"
    )


# ---------------------------------------------------------------- 1. RPC

async def measure_rpc(session, n):
    body = {
        "jsonrpc": "2.0", "id": 1, "method": "getLatestBlockhash",
        "params": [{"commitment": "processed"}],
    }
    lat = []
    for _ in range(n):
        t = time.perf_counter()
        try:
            async with session.post(settings.helius_rpc(), json=body, timeout=10) as r:
                await r.json()
        except Exception as exc:
            print(f"  ! RPC ошибка: {exc}")
            continue
        lat.append((time.perf_counter() - t) * 1000)
        await asyncio.sleep(0.05)
    return lat


# ----------------------------------------------------------------- 2-4. WS

def decode_trade(logs):
    """Наш боевой путь разбора — ровно то, что делает ws_scanner."""
    for line in logs:
        if not line.startswith("Program data: "):
            continue
        try:
            raw = base64.b64decode(line[14:])
        except Exception:
            continue
        if len(raw) < 96 or raw[:8] != TRADE_EVENT_DISC:
            continue
        mint = raw[OFF_MINT:OFF_MINT + 32]
        sol = int.from_bytes(raw[OFF_SOL:OFF_SOL + 8], "little")
        tokens = int.from_bytes(raw[OFF_TOKENS:OFF_TOKENS + 8], "little")
        is_buy = bool(raw[OFF_IS_BUY])
        return mint, sol, tokens, is_buy
    return None


async def measure_ws(session, seconds):
    slot_seen: dict[int, float] = {}
    tip = 0
    lags, gaps, decodes = [], [], []
    early = 0          # логи пришли раньше анонса слота
    events = 0
    sample_mint = None

    async with session.ws_connect(settings.helius_ws(), heartbeat=30) as ws:
        await ws.send_json({"jsonrpc": "2.0", "id": 1, "method": "slotSubscribe"})
        await ws.send_json({
            "jsonrpc": "2.0", "id": 2, "method": "logsSubscribe",
            "params": [{"mentions": [PUMP]}, {"commitment": "processed"}],
        })

        deadline = time.perf_counter() + seconds
        last_report = time.perf_counter()

        async for msg in ws:
            now = time.perf_counter()
            if now > deadline:
                break
            if msg.type is not aiohttp.WSMsgType.TEXT:
                continue
            try:
                d = json.loads(msg.data)
            except Exception:
                continue

            method = d.get("method")
            if method == "slotNotification":
                s = d["params"]["result"]["slot"]
                slot_seen[s] = now
                if s > tip:
                    tip = s
                if len(slot_seen) > 400:                     # держим окно коротким
                    for old in sorted(slot_seen)[:200]:
                        slot_seen.pop(old, None)

            elif method == "logsNotification":
                res = d["params"]["result"]
                slot = res.get("context", {}).get("slot")
                value = res.get("value") or {}
                if value.get("err") is not None:
                    continue
                logs = value.get("logs") or []

                t0 = time.perf_counter()
                trade = decode_trade(logs)
                decodes.append((time.perf_counter() - t0) * 1000)

                if trade is None:
                    continue
                events += 1
                if sample_mint is None:
                    import base58 as _b58
                    sample_mint = _b58.b58encode(trade[0]).decode()

                if slot in slot_seen:
                    lags.append((now - slot_seen[slot]) * 1000)
                elif slot is not None and slot > tip:
                    early += 1
                if slot is not None and tip:
                    gaps.append(tip - slot)

            if now - last_report > 20:
                last_report = now
                left = int(deadline - now)
                print(f"    ... событий {events}, осталось {left} с", flush=True)

    return {
        "lags": lags, "gaps": gaps, "decodes": decodes,
        "early": early, "events": events, "sample_mint": sample_mint,
    }


# ------------------------------------------------------- 5. сборка транзакции

def wallet_pubkey():
    """Публичный адрес для сборки транзакции.

    Если боевой ключ задан — берём адрес из него (сам ключ никуда не
    отправляется, только производный публичный адрес). Если не задан —
    генерируем одноразовый. Для измерения времени сборки это
    безразлично: PumpPortal собирает транзакцию под любой адрес, а мы
    её всё равно не подписываем и не отправляем.
    """
    try:
        from solders.keypair import Keypair
    except Exception:
        return None, "solders недоступен"
    if settings.PRIVATE_KEY:
        try:
            import base58
            kp = Keypair.from_bytes(base58.b58decode(settings.PRIVATE_KEY))
            return str(kp.pubkey()), "боевой адрес"
        except Exception as exc:
            return None, f"ключ не разобран: {exc}"
    return str(Keypair().pubkey()), "одноразовый адрес (боевой ключ не задан)"


async def measure_build(session, mint, pubkey, n):
    if not mint or not pubkey:
        return [], "нет мяты или адреса — шаг пропущен"
    payload = {
        "publicKey": pubkey, "action": "buy", "mint": mint,
        "denominatedInSol": "true", "amount": 0.01,
        "slippage": 10, "priorityFee": 0.0005, "pool": "auto",
    }
    lat, note = [], ""
    for _ in range(n):
        t = time.perf_counter()
        try:
            async with session.post(PUMPPORTAL_URL, json=payload, timeout=15) as r:
                raw = await r.read()
                if r.status != 200:
                    note = f"HTTP {r.status}: {raw[:120]!r}"
                    break
        except Exception as exc:
            note = f"ошибка: {exc}"
            break
        lat.append((time.perf_counter() - t) * 1000)
        await asyncio.sleep(0.3)
    return lat, note


# ------------------------------------------------------------------ 6. подпись

def measure_sign(n=200):
    try:
        from solders.keypair import Keypair
        from solders.message import Message
        from solders.instruction import Instruction, AccountMeta
        from solders.hash import Hash
        from solders.transaction import Transaction
    except Exception as exc:
        return [], f"solders недоступен: {exc}"

    try:
        kp = Keypair()                   # одноразовый, не боевой
        ix = Instruction(kp.pubkey(), b"\x00" * 64,
                         [AccountMeta(kp.pubkey(), True, True)])
        msg = Message([ix], kp.pubkey())
        Transaction([kp], msg, Hash.default())      # пробный вызов
    except Exception as exc:
        return [], f"подпись не собралась: {exc}"

    lat = []
    for _ in range(n):
        t = time.perf_counter()
        Transaction([kp], msg, Hash.default())
        lat.append((time.perf_counter() - t) * 1000)
    return lat, ""


# --------------------------------------------------------------------- main

async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ws-seconds", type=int, default=120)
    ap.add_argument("--rpc-samples", type=int, default=40)
    ap.add_argument("--build-samples", type=int, default=8)
    args = ap.parse_args()

    print("=" * 72)
    print("ИЗМЕРЕНИЕ СОБСТВЕННОЙ ЗАДЕРЖКИ")
    print("Транзакции не подписываются и не отправляются. Деньги не тратятся.")
    print("=" * 72)

    async with aiohttp.ClientSession() as session:
        print("\n[1/5] RPC round-trip до Helius...", flush=True)
        rpc = await measure_rpc(session, args.rpc_samples)
        describe("RPC round-trip", rpc)

        print(f"\n[2/5] Поток Pump.fun, {args.ws_seconds} с...", flush=True)
        ws = await measure_ws(session, args.ws_seconds)
        describe("WS: анонс слота -> логи", ws["lags"])
        describe("WS: разбор события", ws["decodes"])
        if ws["gaps"]:
            g = ws["gaps"]
            print(f"  {'Отставание от tip':<28} p50={statistics.median(g):.0f}  "
                  f"p90={pct(g, 90):.0f}  слотов  "
                  f"(~{statistics.median(g) * SLOT_MS:.0f} мс)")
        print(f"  {'Логи раньше анонса слота':<28} {ws['early']} раз")
        print(f"  {'Событий сделок разобрано':<28} {ws['events']}")

        pk, pk_note = wallet_pubkey()
        print(f"\n[3/5] Сборка транзакции в PumpPortal ({pk_note})...", flush=True)
        build, note = await measure_build(session, ws["sample_mint"], pk,
                                          args.build_samples)
        describe("PumpPortal trade-local", build)
        if note:
            print(f"  ! {note}")

        print("\n[4/5] Локальная подпись...", flush=True)
        sign, snote = measure_sign()
        describe("Подпись транзакции", sign)
        if snote:
            print(f"  ! {snote}")

        # ------------------------------------------------------------ итог
        print("\n[5/5] ИТОГ")
        print("-" * 72)

        def med(v):
            return statistics.median(v) if v else 0.0

        ws_lag = med(ws["lags"])
        dec = med(ws["decodes"])
        bld = med(build)
        sgn = med(sign)
        floor = ws_lag + dec + bld + sgn

        print(f"  доставка события     {ws_lag:8.1f} мс")
        print(f"  разбор               {dec:8.1f} мс")
        print(f"  сборка транзакции    {bld:8.1f} мс")
        print(f"  подпись              {sgn:8.1f} мс")
        print(f"  {'-' * 34}")
        print(f"  ДО ПОДПИСАННОЙ TX    {floor:8.1f} мс  = {floor / SLOT_MS:.1f} слота")
        print(f"  + отправка и попадание в блок: минимум 1-2 слота "
              f"({SLOT_MS:.0f}-{2 * SLOT_MS:.0f} мс), не измерено здесь")
        print()

        total_optimistic = floor + SLOT_MS
        print(f"  Реалистичная нижняя граница реакции: ~{total_optimistic:.0f} мс")
        print()
        if total_optimistic <= 500:
            verdict = ("Быстро. Реактивные стратегии стоит мерить дальше.")
        elif total_optimistic <= 1500:
            verdict = ("Пограничная зона. Кросс-DEX арбитраж почти наверняка "
                       "закрыт, но предсказуемые события (градация) —\n"
                       "  ещё нет, потому что там можно готовиться заранее.")
        else:
            verdict = ("Медленно. Всё реактивное закрыто. Смысл имеют только "
                       "стратегии, где событие известно заранее.")
        print(f"  ВЫВОД: {verdict}")
        print("-" * 72)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(130)
