#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Приёмник материалов из внешнего канала.

Ничего не советует и не торгует. Задача одна: зафиксировать материал
в момент прихода, ДО того как исход известен, и потом честно замерить,
что из него вышло.

Команды
-------
  python3 -m research.ext_signals add   [файл|-]     сигнал бота: парсит и снимает t0
  python3 -m research.ext_signals save  [файл|-] --kind voice|post|other --note "..."
  python3 -m research.ext_signals track                доснять открытые сигналы
  python3 -m research.ext_signals report               воспроизведённый hit rate и EV
  python3 -m research.ext_signals ls                   что лежит в архиве

Раскладка
---------
  channel/raw/<ts>_<kind>[_<sig>].txt   оригинал дословно
  channel/index.jsonl                   по записи на материал
  data/ext_signals.jsonl                разобранные сигналы (t0)
  data/ext_signal_track.jsonl           снимки цены по открытым сигналам
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHANNEL = os.path.join(ROOT, "channel")
RAW = os.path.join(CHANNEL, "raw")
INDEX = os.path.join(CHANNEL, "index.jsonl")
DATA = os.path.join(ROOT, "data")
SIGNALS = os.path.join(DATA, "ext_signals.jsonl")
TRACK = os.path.join(DATA, "ext_signal_track.jsonl")

DEX = "https://api.dexscreener.com/latest/dex/tokens/"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"

# Сетка досъёма: первые 2 часа часто (часовой барьер надо ловить честно),
# дальше редко. Значения в секундах от t0.
GRID_FINE_SEC = 180
GRID_FINE_UNTIL = 2 * 3600
GRID_COARSE_SEC = 3600
TRACK_HORIZON_SEC = 72 * 3600

# Издержки на круг (вход+выход): комиссия, проскальзывание, спред.
# Осознанно пессимистично; меняется одним местом.
ROUNDTRIP_COST = 0.03


# --------------------------------------------------------------------------
# утилиты
# --------------------------------------------------------------------------
def _norm(text: str) -> str:
    """Убрать неразрывные пробелы и прочую типографику, ломающую regex."""
    for ch in ("\u00a0", "\u202f", "\u2009", "\u2007"):
        text = text.replace(ch, " ")
    return text.replace("\u2011", "-").replace("\u2212", "-")


def _money(s):
    if s is None:
        return None
    s = _norm(str(s)).replace("$", "").replace(",", "").replace(" ", "")
    try:
        return float(s)
    except ValueError:
        return None


def _pct(s):
    if s is None:
        return None
    try:
        return float(_norm(str(s)).replace("%", "").replace(",", ".").strip()) / 100.0
    except ValueError:
        return None


def _now():
    return time.time()


def _stamp(ts=None):
    dt = datetime.fromtimestamp(ts or _now(), tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H-%M-%SZ")


def _append(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def _read_jsonl(path):
    if not os.path.exists(path):
        return []
    out = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except ValueError:
                continue
    return out


def _read_input(src):
    if src in (None, "-"):
        sys.stderr.write("Вставь текст, затем Ctrl-D:\n")
        return sys.stdin.read()
    with open(src, encoding="utf-8") as f:
        return f.read()


# --------------------------------------------------------------------------
# разбор сигнала
# --------------------------------------------------------------------------
RE_TOKEN = re.compile(r"/tokens?/([a-z0-9_-]+)/(0x[0-9a-fA-F]{40})")
RE_TOKEN_SOL = re.compile(r"/token/([1-9A-HJ-NP-Za-km-z]{32,44})")
RE_MCAP = re.compile(r"MCAP\s*\$?\s*([\d\s,]+)", re.I)
RE_VOL = re.compile(r"оборот/ч\s*\$?\s*([\d\s,]+)", re.I)
RE_HOLDERS = re.compile(r"холдеров\s*([\d\s,]+)", re.I)
RE_CONC = re.compile(r"концентрация\s+top\s*(\d+)\s*([\d.,]+)\s*%", re.I)
RE_TAX = re.compile(r"налог\s+покупки\s*([\d.,]+)\s*%\s*/\s*продажи\s*([\d.,]+)\s*%", re.I)
RE_MODEL = re.compile(r"модель\s*`?([\w.\-]+)`?", re.I)
RE_STATUS = re.compile(r"статус\s*`?([A-Z_]+)`?")
RE_TAG = re.compile(r"`?([A-Z][A-Z_]{6,})`?\s*(?:·|\u00b7)\s*модель")
RE_SIG = re.compile(r"`?(sig_[0-9a-f]{8,})`?")
RE_MSK = re.compile(r"(\d{2})\.(\d{2})(?:\.(\d{2,4}))?\s+(\d{2}):(\d{2}):(\d{2})\s*МСК")

RE_WALLET = re.compile(r"`?(0x[0-9a-fA-F]{40})`?")
RE_WALLET_LINE = re.compile(
    r"купил\s*\$?\s*([\d\s,]+).*?(\d+)\s+созревш\w*\s+исход\w*.*?"
    r"попадан\w*\s*([\d.,]+)\s*%"
    r"(?:.*?сжатием\s+к\s+базе\s*([\d.,]+)\s*%)?"
    r"(?:.*?база\s*([\d.,]+)\s*%)?"
    r"(?:.*?отрыв\s*[x\u00d7]\s*([\d.,]+))?",
    re.I | re.S,
)
# «рост цены на 25% раньше падения на 20% за час»
RE_BARRIER = re.compile(
    r"рост\s+цены\s+на\s*([\d.,]+)\s*%\s*раньше\s+падени\w*\s+на\s*([\d.,]+)\s*%"
    r"\s*за\s*(час|\d+\s*(?:ч|час\w*|мин\w*))",
    re.I,
)


def _horizon_sec(s):
    s = (s or "").strip().lower()
    if not s or s.startswith("час"):
        return 3600
    m = re.match(r"(\d+)\s*(ч|час|мин)", s)
    if not m:
        return 3600
    n = int(m.group(1))
    return n * 60 if m.group(2).startswith("мин") else n * 3600


def parse_signal(text: str) -> dict:
    t = _norm(text)
    rec = {"raw": text}

    m = RE_TOKEN.search(t)
    if m:
        rec["chain"], rec["token"] = m.group(1), m.group(2).lower()
    else:
        m = RE_TOKEN_SOL.search(t)
        if m:
            rec["chain"], rec["token"] = "solana", m.group(1)

    m = RE_MCAP.search(t)
    rec["mcap_usd"] = _money(m.group(1)) if m else None
    m = RE_VOL.search(t)
    rec["vol_1h_usd"] = _money(m.group(1)) if m else None
    m = RE_HOLDERS.search(t)
    rec["holders"] = int(_money(m.group(1)) or 0) if m else None
    m = RE_CONC.search(t)
    if m:
        rec["conc_top_n"] = int(m.group(1))
        rec["conc_top_share"] = _pct(m.group(2))
    m = RE_TAX.search(t)
    if m:
        rec["buy_tax"] = _pct(m.group(1))
        rec["sell_tax"] = _pct(m.group(2))

    m = RE_MODEL.search(t)
    rec["model"] = m.group(1) if m else None
    m = RE_STATUS.search(t)
    rec["status"] = m.group(1) if m else None
    m = RE_TAG.search(t)
    rec["tag"] = m.group(1) if m else None
    m = RE_SIG.search(t)
    rec["sig_id"] = m.group(1) if m else None

    m = RE_MSK.search(t)
    if m:
        dd, mm, yy, hh, mi, ss = m.groups()
        year = int(yy) + 2000 if yy and len(yy) == 2 else int(yy or datetime.now().year)
        rec["sent_msk"] = "%04d-%02d-%02dT%s:%s:%s+03:00" % (year, int(mm), int(dd), hh, mi, ss)

    m = RE_BARRIER.search(t)
    if m:
        rec["barrier_up"] = _pct(m.group(1))
        rec["barrier_down"] = _pct(m.group(2))
        rec["barrier_horizon_sec"] = _horizon_sec(m.group(3))
    else:
        rec["barrier_up"], rec["barrier_down"], rec["barrier_horizon_sec"] = None, None, None

    # кошельки: адрес, затем строка с числами
    wallets = []
    lines = [ln for ln in t.splitlines()]
    for i, ln in enumerate(lines):
        mw = RE_WALLET.search(ln)
        if not mw:
            continue
        addr = mw.group(1).lower()
        if addr == rec.get("token"):
            continue
        blob = " ".join(lines[i:i + 3])
        mv = RE_WALLET_LINE.search(blob)
        if not mv:
            continue
        wallets.append({
            "address": addr,
            "bought_usd": _money(mv.group(1)),
            "n_outcomes": int(mv.group(2)),
            "hit_rate": _pct(mv.group(3)),
            "hit_shrunk": _pct(mv.group(4)),
            "base_rate": _pct(mv.group(5)),
            "lift_claimed": float((mv.group(6) or "0").replace(",", ".")) or None,
            "buy_verified_onchain": None,   # заполняется отдельной проверкой
        })
    rec["wallets"] = wallets
    rec["base_rate"] = next((w["base_rate"] for w in wallets if w.get("base_rate")), None)
    return rec


# --------------------------------------------------------------------------
# снимок рынка
# --------------------------------------------------------------------------
def dex_snapshot(token: str):
    if not token:
        return None
    req = urllib.request.Request(DEX + token, headers={"User-Agent": UA, "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            data = json.loads(r.read().decode("utf-8"))
    except (urllib.error.URLError, ValueError, OSError) as e:
        return {"error": str(e)}
    best = None
    for p in data.get("pairs") or []:
        liq = float(((p.get("liquidity") or {}).get("usd")) or 0)
        if best is None or liq > best["liq_usd"]:
            fdv = float(p.get("fdv") or 0)
            mcap = float(p.get("marketCap") or fdv or 0)
            vol = p.get("volume") or {}
            vol24 = float(vol.get("h24") or 0)
            best = {
                "liq_usd": liq,
                "price_usd": float(p.get("priceUsd") or 0),
                "fdv": fdv,
                "mcap_usd": mcap,
                "vol24": vol24,  # бесплатно из того же ответа; различает пул vs не-рынок
                "pair": p.get("pairAddress"),
                "dex": p.get("dexId"),
                "chain": p.get("chainId"),
            }
    if not best:
        return {"error": "no_pairs"}
    # Третий счётчик форварда (пререгистрация 2026-09-10): отношение навеса.
    # Не выводить из исхода — писать на t0 всегда.
    liq = float(best.get("liq_usd") or 0)
    if liq > 0:
        if best.get("fdv"):
            best["fdv_liq"] = float(best["fdv"]) / liq
        if best.get("mcap_usd"):
            best["mcap_liq"] = float(best["mcap_usd"]) / liq
    return best


def _track_key(sig_or_snap: dict) -> str:
    """Ключ досъема: token (sig_id у TOP_HOLDER_RISK всегда null — иначе все сигналы
    делят одну кучу снимков и сетка схлопывается)."""
    tok = (sig_or_snap.get("token") or "").lower()
    if tok:
        return "tok:" + tok
    sid = sig_or_snap.get("sig_id")
    return "sid:" + sid if sid else "unknown"


# --------------------------------------------------------------------------
# команды
# --------------------------------------------------------------------------
def _save_raw(text, kind, sig_id=None, ts=None):
    os.makedirs(RAW, exist_ok=True)
    name = "%s_%s%s.txt" % (_stamp(ts), kind, ("_" + sig_id) if sig_id else "")
    path = os.path.join(RAW, name)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    return os.path.relpath(path, ROOT)


def cmd_add(args):
    text = _read_input(args.src)
    rec = parse_signal(text)
    ts = _now()
    rec["ts_received"] = ts
    rec["received_utc"] = _stamp(ts)
    rec["source"] = args.source
    rec["note"] = args.note
    rec["raw_path"] = _save_raw(text, "signal", rec.get("sig_id"), ts)
    rec.pop("raw", None)

    snap = dex_snapshot(rec.get("token")) if not args.no_fetch else {"skipped": True}
    # claimed mcap из текста алерта / liq с dex — третий счётчик рядом с t0
    if isinstance(snap, dict) and not snap.get("error") and not snap.get("skipped"):
        liq = float(snap.get("liq_usd") or 0)
        claimed = rec.get("mcap_usd")
        if liq > 0 and claimed:
            try:
                snap["mcap_liq_claimed"] = float(claimed) / liq
            except (TypeError, ValueError):
                pass
    rec["t0"] = snap
    _append(SIGNALS, rec)
    _append(INDEX, {
        "ts": ts, "utc": rec["received_utc"], "kind": "signal",
        "source": args.source, "sig_id": rec.get("sig_id"),
        "token": rec.get("token"), "chain": rec.get("chain"),
        "path": rec["raw_path"], "note": args.note,
    })

    print("сохранено: %s" % rec["raw_path"])
    print("  sig      : %s  модель %s  статус %s" % (rec.get("sig_id"), rec.get("model"), rec.get("status")))
    print("  токен    : %s  (%s)" % (rec.get("token"), rec.get("chain")))
    print("  заявлено : mcap $%s · оборот/ч $%s · холдеров %s"
          % (_fmt(rec.get("mcap_usd")), _fmt(rec.get("vol_1h_usd")), rec.get("holders")))
    if rec.get("barrier_up"):
        be = rec["barrier_down"] / (rec["barrier_up"] + rec["barrier_down"])
        print("  барьеры  : +%.0f%% / -%.0f%% за %dс → безубыток при hit %.1f%%"
              % (rec["barrier_up"] * 100, rec["barrier_down"] * 100,
                 rec["barrier_horizon_sec"], be * 100))
    for w in rec["wallets"]:
        print("  кошелёк  : %s купил $%s · n=%d · hit %.1f%% (сжато %s)"
              % (w["address"][:10], _fmt(w["bought_usd"]), w["n_outcomes"],
                 (w["hit_rate"] or 0) * 100,
                 ("%.1f%%" % (w["hit_shrunk"] * 100)) if w.get("hit_shrunk") else "—"))
    if isinstance(snap, dict) and snap.get("error"):
        print("  t0       : снимок не получен (%s) — токен отследим только вручную" % snap["error"])
    elif isinstance(snap, dict) and snap.get("price_usd"):
        print("  t0       : цена %.3g · ликвидность $%s · %s"
              % (snap["price_usd"], _fmt(snap["liq_usd"]), snap.get("dex")))
        if snap.get("fdv_liq") is not None or snap.get("mcap_liq_claimed") is not None:
            print(
                "  навес t0 : fdv/liq=%.1fx  claimed_mcap/liq=%s"
                % (
                    snap.get("fdv_liq") or 0,
                    ("%.1fx" % snap["mcap_liq_claimed"])
                    if snap.get("mcap_liq_claimed") is not None
                    else "—",
                )
            )
        if snap.get("vol24") is not None:
            print("  vol24    : $%s" % _fmt(snap.get("vol24")))


def cmd_save(args):
    text = _read_input(args.src)
    ts = _now()
    path = _save_raw(text, args.kind, None, ts)
    _append(INDEX, {
        "ts": ts, "utc": _stamp(ts), "kind": args.kind,
        "source": args.source, "path": path, "note": args.note,
        "chars": len(text),
    })
    print("сохранено: %s  (%d символов, kind=%s)" % (path, len(text), args.kind))


def _due(sig, snaps_for_sig):
    """Пора ли снимать этот сигнал."""
    age = _now() - sig["ts_received"]
    if age > TRACK_HORIZON_SEC:
        return False
    last = max([s["ts"] for s in snaps_for_sig], default=sig["ts_received"])
    step = GRID_FINE_SEC if age <= GRID_FINE_UNTIL else GRID_COARSE_SEC
    return (_now() - last) >= step


def cmd_track(args):
    sigs = _read_jsonl(SIGNALS)
    snaps = _read_jsonl(TRACK)
    by_key: dict = {}
    for s in snaps:
        by_key.setdefault(_track_key(s), []).append(s)
    # один token — одна активная запись (последняя с ценой t0)
    latest_by_tok: dict = {}
    for sig in sigs:
        tok = (sig.get("token") or "").lower()
        if not tok or not (sig.get("t0") or {}).get("price_usd"):
            continue
        prev = latest_by_tok.get(tok)
        if prev is None or float(sig.get("ts_received") or 0) >= float(prev.get("ts_received") or 0):
            latest_by_tok[tok] = sig

    n = 0
    for sig in latest_by_tok.values():
        key = _track_key(sig)
        if not _due(sig, by_key.get(key, [])):
            continue
        snap = dex_snapshot(sig["token"])
        if not isinstance(snap, dict) or snap.get("error"):
            continue
        snap.update({
            "sig_id": sig.get("sig_id"),
            "token": sig["token"],
            "ts": _now(),
            "track_key": key,
        })
        _append(TRACK, snap)
        by_key.setdefault(key, []).append(snap)
        n += 1
        time.sleep(0.4)
    print("доснято: %d" % n)


def _barrier_label(sig, snaps):
    """Метка исхода.

    Классические алерты: +up раньше −down в горизонте.
    TOP_HOLDER_RISK: касание −50% за 24ч (утверждение канала «сложился вдвое»).
    """
    p0 = (sig.get("t0") or {}).get("price_usd")
    if not p0:
        return None, "нет базовой цены"

    model = (sig.get("model") or "")
    if model == "top_holder_risk_v1" or (
        not sig.get("barrier_up") and (sig.get("tag") or "") == "TOP_HOLDER_RISK"
    ):
        hor = 24 * 3600
        inside = sorted(
            [s for s in snaps if 0 <= s["ts"] - sig["ts_received"] <= hor],
            key=lambda s: s["ts"],
        )
        if not inside:
            return None, "нет снимков"
        for s in inside:
            if s["price_usd"] / p0 - 1.0 <= -0.50:
                return True, "касание −50% (сложился вдвое)"
        if _now() - sig["ts_received"] < hor:
            return None, "горизонт 24ч не истёк"
        return False, "24ч без касания −50%"

    up, down = sig.get("barrier_up"), sig.get("barrier_down")
    hor = sig.get("barrier_horizon_sec") or 3600
    if not (up and down):
        return None, "нет базовой цены или барьеров"
    inside = sorted([s for s in snaps if s["ts"] - sig["ts_received"] <= hor], key=lambda s: s["ts"])
    if not inside:
        return None, "нет снимков внутри горизонта"
    for s in inside:
        r = s["price_usd"] / p0 - 1.0
        if r >= up:
            return True, "верх пробит"
        if r <= -down:
            return False, "низ пробит"
    if _now() - sig["ts_received"] < hor:
        return None, "горизонт не истёк"
    return False, "горизонт истёк без пробоя"


def _ret_at(sig, snaps, sec):
    p0 = (sig.get("t0") or {}).get("price_usd")
    if not p0:
        return None
    cand = [s for s in snaps if s["ts"] - sig["ts_received"] >= sec]
    if not cand:
        return None
    s = min(cand, key=lambda x: x["ts"])
    return s["price_usd"] / p0 - 1.0


def cmd_report(args):
    sigs = _read_jsonl(SIGNALS)
    snaps = _read_jsonl(TRACK)
    by_key: dict = {}
    for s in snaps:
        by_key.setdefault(_track_key(s), []).append(s)

    # лаг алерт→t0 (зафиксировано 2026-09-10; не пересматривать постфактум)
    print("=== лаг алерт → t0 (пререгистрация / факт) ===")
    print("  измерен по 4 в зачёте: 0.67–0.85 ч (медиана ~0.73 ч).")
    print("  окно почти не сдвинуто; горизонт 24ч от ts_received допустим.")
    print()

    hits = misses = pending = 0
    rets = {3600: [], 4 * 3600: [], 24 * 3600: [], 72 * 3600: []}
    # дедуп по token — одна строка на монету (последняя с t0)
    latest = {}
    for sig in sigs:
        tok = (sig.get("token") or "").lower()
        t0 = sig.get("t0") or {}
        if not tok or not t0.get("price_usd"):
            continue
        note = sig.get("note") or ""
        if note.startswith("БРАК") or note.startswith("НЕ по"):
            continue
        prev = latest.get(tok)
        if prev is None or float(sig.get("ts_received") or 0) >= float(prev.get("ts_received") or 0):
            latest[tok] = sig

    print("%-14s %-8s %7s %8s %8s %8s %8s %s" % (
        "token", "метка", "m/liq", "vol24", "+1ч", "+10ч", "+24ч", "why"))
    for sig in sorted(latest.values(), key=lambda s: s.get("ts_received") or 0):
        tok = sig["token"]
        ss = by_key.get(_track_key(sig), [])
        lab, why = _barrier_label(sig, ss)
        if lab is True:
            hits += 1
            lab_s = "HALF"
        elif lab is False:
            misses += 1
            lab_s = "clean"
        else:
            pending += 1
            lab_s = "wait"
        t0 = sig.get("t0") or {}
        ml = t0.get("mcap_liq_claimed") or t0.get("fdv_liq")
        # vol24: t0 or latest track
        vol = t0.get("vol24")
        if ss:
            vol = ss[-1].get("vol24", vol)
        row = []
        for h in (3600, 10 * 3600, 24 * 3600):
            r = _ret_at(sig, ss, h)
            row.append(("%+.1f%%" % (r * 100)) if r is not None else "—")
        for h in (3600, 4 * 3600, 24 * 3600, 72 * 3600):
            r = _ret_at(sig, ss, h)
            if r is not None:
                rets[h].append(r)
        print("%-14s %-8s %6s %8s %8s %8s %8s %s" % (
            tok[:14],
            lab_s,
            ("%.1fx" % ml) if ml else "—",
            ("$%s" % _fmt(vol)) if vol else "—",
            row[0], row[1], row[2],
            why,
        ))

    matured = hits + misses
    print("\nв зачёте: %d   half(−50%%): %d   clean: %d   ждём: %d"
          % (len(latest), hits, misses, pending))
    print("кластер: первые 4 алерта за ~18 мин на одном чейне — ближе к 1 наблюдению,")
    print("  чем к 4; p-значения «0 из 4» завышают силу против канала.")
    if matured:
        p = hits / matured
        print("доля сложившихся вдвое: %.1f%%  (n=%d)" % (p * 100, matured))
        print("канал заявляет ~81%% (ведро 1–2/3+); база «ноль» 41.4%%.")
    for h, name in ((3600, "+1ч"), (4 * 3600, "+4ч"), (24 * 3600, "+24ч"), (72 * 3600, "+72ч")):
        v = sorted(rets[h])
        if v:
            print("медиана %s: %+.1f%%  (n=%d)" % (name, v[len(v) // 2] * 100, len(v)))


def _fmt(x):
    return "—" if x is None else format(int(x), ",").replace(",", " ")


def cmd_ls(args):
    idx = _read_jsonl(INDEX)
    if not idx:
        print("архив пуст")
        return
    print("%-22s %-8s %-14s %s" % ("получено (UTC)", "тип", "источник", "файл"))
    for r in idx:
        print("%-22s %-8s %-14s %s" % (r.get("utc", "—"), r.get("kind", "—"),
                                       (r.get("source") or "—")[:14], r.get("path", "—")))
    print("\nвсего: %d   сигналов: %d" % (len(idx), sum(1 for r in idx if r.get("kind") == "signal")))


def main():
    ap = argparse.ArgumentParser(description="приёмник материалов внешнего канала")
    sub = ap.add_subparsers(dest="cmd", required=True)

    a = sub.add_parser("add", help="сигнал бота")
    a.add_argument("src", nargs="?", default="-")
    a.add_argument("--source", default="channel")
    a.add_argument("--note", default="")
    a.add_argument("--no-fetch", action="store_true")
    a.set_defaults(func=cmd_add)

    s = sub.add_parser("save", help="любой другой материал")
    s.add_argument("src", nargs="?", default="-")
    s.add_argument("--kind", default="post", choices=["voice", "post", "other"])
    s.add_argument("--source", default="channel")
    s.add_argument("--note", default="")
    s.set_defaults(func=cmd_save)

    t = sub.add_parser("track", help="доснять открытые сигналы")
    t.set_defaults(func=cmd_track)

    r = sub.add_parser("report", help="воспроизведённый hit rate и EV")
    r.set_defaults(func=cmd_report)

    l = sub.add_parser("ls", help="что лежит в архиве")
    l.set_defaults(func=cmd_ls)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
