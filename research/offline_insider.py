"""
Офлайн-проверка insider-гипотезы (общий funder у свежих кошельков).

Живой бот при бюджете Helius разрешает funder у единиц процентов покупателей —
кластер из двух на одном минте почти не ловится. Гипотеза при этом
проверяется ПОСТФАКТУМ: покупатели уже в outcomes/clusters, спонсоров
резолвим пакетом без гонки с WS.

  python -m research.offline_insider --resolve          # добрать funder'ов в SQLite
  python -m research.offline_insider --report           # lift по outcomes
  python -m research.offline_insider --resolve --report

Квота офлайн отдельная и медленная (по умолчанию ~20 req/min, 1 воркер),
чтобы не ронять live-бот в 429. Кэш — data/sniper.db::wallet_funders
(+ data/wallet_funders.json для совместимости с analyze_clusters).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Optional

import aiohttp

from config import settings
from core.db import get_db
from core.funding_graph import _SYSTEM_OR_PROGRAM_FUNDERS, _sanitize_funder

OUTCOMES = Path("data/outcomes.jsonl")
CLUSTERS = Path("data/clusters.jsonl")
MINTS_OUT = Path("data/insider_offline_mints.json")
JSON_CACHE = Path("data/wallet_funders.json")

FRESH_HOURS = float(settings.INSIDER_MAX_WALLET_AGE_HOURS)
MIN_SHARED = int(settings.INSIDER_MIN_SHARED_FUNDER)
MAX_FANOUT = int(settings.INSIDER_MAX_FUNDER_FANOUT)
MIN_BUYERS = 3          # минты с <3 покупателями кластер не дадут — не тратим RPC
PAGE = 100
MAX_PAGES = max(1, int(settings.INSIDER_MAX_SIG_PAGES))


# ---------------------------------------------------------------- resolve RPC

class OfflineRpc:
    """Свой темп, не общий HeliusGate бота — офлайн не должен глушить live."""

    def __init__(self, session: aiohttp.ClientSession, per_min: int = 20):
        self._session = session
        self._sem = asyncio.Semaphore(1)
        self._per_min = max(1, per_min)
        self._times: list[float] = []
        self._n429 = 0
        self._ok = 0

    async def call(self, method: str, params: list) -> Optional[object]:
        body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        async with self._sem:
            while True:
                now = time.time()
                self._times = [t for t in self._times if now - t < 60]
                if len(self._times) >= self._per_min:
                    await asyncio.sleep(60 / self._per_min)
                    continue
                self._times.append(time.time())
                break
            for attempt in range(5):
                try:
                    async with self._session.post(
                        settings.helius_rpc(), json=body, timeout=25,
                    ) as resp:
                        if resp.status == 429:
                            self._n429 += 1
                            wait = 2.0 * (attempt + 1)
                            print(f"  429 → sleep {wait:.0f}s", flush=True)
                            await asyncio.sleep(wait)
                            continue
                        if resp.status != 200:
                            await asyncio.sleep(0.5)
                            continue
                        data = await resp.json()
                        self._ok += 1
                        return data.get("result")
                except Exception:
                    await asyncio.sleep(0.5 * (attempt + 1))
        return None


async def resolve_wallet(rpc: OfflineRpc, wallet: str, now: float) -> dict:
    """Как live FundingGraph._fetch_origin, но без gate бота."""
    sigs = await rpc.call("getSignaturesForAddress", [wallet, {"limit": PAGE}])
    if not sigs:
        return {"birth": None, "funder": None, "n_sigs": 0, "capped": False}

    n_total = len(sigs)
    oldest = sigs[-1]
    pages = 1

    while len(sigs) >= PAGE and pages < MAX_PAGES:
        bt = oldest.get("blockTime")
        if bt is not None and (now - bt) / 3600 > FRESH_HOURS:
            return {"birth": None, "funder": None, "n_sigs": n_total, "capped": True}
        more = await rpc.call(
            "getSignaturesForAddress",
            [wallet, {"limit": PAGE, "before": oldest["signature"]}],
        )
        if not more:
            return {"birth": None, "funder": None, "n_sigs": n_total, "capped": True}
        n_total += len(more)
        oldest = more[-1]
        sigs = more
        pages += 1
        if len(more) < PAGE:
            break
    else:
        if len(sigs) >= PAGE:
            return {"birth": None, "funder": None, "n_sigs": n_total, "capped": True}

    birth = oldest.get("blockTime")
    age_h = ((now - birth) / 3600) if birth else None
    if age_h is not None and age_h > FRESH_HOURS:
        return {"birth": birth, "funder": None, "n_sigs": n_total, "capped": True,
                "age_hours": age_h}

    funder = None
    tx = await rpc.call(
        "getTransaction",
        [oldest["signature"], {
            "encoding": "jsonParsed",
            "maxSupportedTransactionVersion": 0,
            "commitment": "confirmed",
        }],
    )
    if tx:
        try:
            msg = tx["transaction"]["message"]
            for ix in msg.get("instructions", []):
                info = (ix.get("parsed") or {}).get("info") or {}
                if info.get("destination") == wallet and info.get("source"):
                    cand = _sanitize_funder(info["source"])
                    if cand:
                        funder = cand
                        break
            if funder is None:
                keys = msg.get("accountKeys", [])
                if keys:
                    first = keys[0]
                    pk = first.get("pubkey") if isinstance(first, dict) else first
                    if pk and pk != wallet:
                        funder = _sanitize_funder(pk)
        except Exception:
            pass

    return {
        "birth": birth, "funder": funder, "n_sigs": n_total,
        "capped": False, "age_hours": age_h,
    }


def load_json_cache() -> dict:
    if JSON_CACHE.exists():
        try:
            return json.loads(JSON_CACHE.read_text())
        except Exception:
            return {}
    return {}


def save_json_cache(cache: dict) -> None:
    JSON_CACHE.write_text(json.dumps(cache, ensure_ascii=False))


def origin_from_db(wallet: str, now: float) -> Optional[dict]:
    row = get_db().get_wallet_funder(wallet)
    if not row:
        return None
    # старый capped без birth — пересчитать
    if row.get("capped") and not row.get("birth_unix"):
        return None
    birth = row.get("birth_unix")
    return {
        "birth": birth,
        "funder": _sanitize_funder(row.get("funder")),
        "n_sigs": int(row.get("n_sigs") or 0),
        "capped": bool(row.get("capped")),
        "age_hours": ((now - birth) / 3600) if birth else None,
    }


# ---------------------------------------------------------------- collect

def collect_mint_wallets(min_buyers: int) -> dict[str, set[str]]:
    """mint -> set(wallets). Источники: outcomes + clusters buyers."""
    by_mint: dict[str, set[str]] = defaultdict(set)

    if OUTCOMES.exists():
        with OUTCOMES.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue
                m, w = r.get("mint"), r.get("wallet")
                if m and w:
                    by_mint[m].add(w)

    if CLUSTERS.exists():
        with CLUSTERS.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue
                m = r.get("mint")
                buyers = r.get("buyers") or []
                if not m or not buyers:
                    continue
                for b in buyers:
                    if isinstance(b, (list, tuple)) and b:
                        by_mint[m].add(b[0])

    return {m: ws for m, ws in by_mint.items() if len(ws) >= min_buyers}


async def resolve_batch(wallets: list[str], per_min: int, limit: Optional[int]) -> dict:
    now = time.time()
    db = get_db()
    cache = load_json_cache()
    done: dict[str, dict] = {}

    todo = []
    for w in wallets:
        cached = origin_from_db(w, now)
        if cached is not None:
            done[w] = cached
            continue
        if w in cache and not (cache[w].get("capped") and not cache[w].get("birth")):
            # json-кэш analyze_clusters: {birth, funder}
            entry = cache[w]
            funder = _sanitize_funder(entry.get("funder"))
            if funder in _SYSTEM_OR_PROGRAM_FUNDERS:
                funder = None
            done[w] = {
                "birth": entry.get("birth"),
                "funder": funder,
                "n_sigs": entry.get("n_sigs") or 0,
                "capped": bool(entry.get("capped")),
            }
            continue
        todo.append(w)

    if limit is not None:
        todo = todo[:limit]

    print(
        f"кошельков уникальных {len(wallets)}: уже известно {len(done)}, "
        f"запросить {len(todo)} (~{len(todo) * 2} RPC, ~{per_min}/min)"
    )
    if not todo:
        return done

    async with aiohttp.ClientSession() as session:
        rpc = OfflineRpc(session, per_min=per_min)
        for i, w in enumerate(todo, 1):
            try:
                origin = await resolve_wallet(rpc, w, now)
            except Exception as exc:  # noqa: BLE001
                print(f"  resolve fail {w[:8]}: {exc}", flush=True)
                origin = {"birth": None, "funder": None, "n_sigs": 0, "capped": False}
            done[w] = origin
            try:
                db.upsert_wallet_funder(
                    w, origin.get("birth"), origin.get("funder"),
                    int(origin.get("n_sigs") or 0), bool(origin.get("capped")),
                )
            except Exception as exc:  # noqa: BLE001
                print(f"  db upsert fail {w[:8]}: {exc}", flush=True)
            cache[w] = {
                "birth": origin.get("birth"),
                "funder": origin.get("funder"),
                "n_sigs": origin.get("n_sigs"),
                "capped": origin.get("capped"),
            }
            if i == 1 or i % 5 == 0 or i == len(todo):
                save_json_cache(cache)
                print(
                    f"  {i}/{len(todo)}  ok_rpc={rpc._ok} 429={rpc._n429} "
                    f"funder={sum(1 for o in done.values() if o.get('funder'))}",
                    flush=True,
                )
        save_json_cache(cache)
    return done


# ---------------------------------------------------------------- clusters + report

def build_mint_clusters(
    mint_wallets: dict[str, set[str]], origins: dict[str, dict], now: float,
) -> dict:
    """По каждому минтам: best funder, size, список участников."""
    # глобальный fanout свежих
    fanout: Counter = Counter()
    for w, o in origins.items():
        f = o.get("funder")
        birth = o.get("birth")
        if not f or o.get("capped") or birth is None:
            continue
        if (now - birth) / 3600 > FRESH_HOURS:
            continue
        fanout[f] += 1
    hubs = {f for f, n in fanout.items() if n > MAX_FANOUT}

    out = {}
    for mint, wallets in mint_wallets.items():
        by_f: dict[str, list[str]] = defaultdict(list)
        for w in wallets:
            o = origins.get(w) or {}
            f = o.get("funder")
            birth = o.get("birth")
            if not f or f in hubs or o.get("capped") or birth is None:
                continue
            if (now - birth) / 3600 > FRESH_HOURS:
                continue
            by_f[f].append(w)
        best_f, best_n, best_ws = None, 0, []
        for f, ws in by_f.items():
            if len(ws) > best_n:
                best_f, best_n, best_ws = f, len(ws), ws
        out[mint] = {
            "n_buyers": len(wallets),
            "funder": best_f,
            "cluster_size": best_n,
            "cluster_wallets": best_ws,
            "is_cluster": best_n >= MIN_SHARED,
        }
    return out


def _pctile(vals: list[float], q: float) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    return s[min(max(int(round(q * (len(s) - 1))), 0), len(s) - 1)]


def _honest(r: dict) -> Optional[float]:
    if r.get("honest_growth") is not None and r.get("market_alive") is not None:
        try:
            return float(r["honest_growth"])
        except (TypeError, ValueError):
            pass
    if r.get("market_alive") is False:
        return -1.0
    g = r.get("growth")
    if g is None or not r.get("exit_price_usd"):
        return -1.0
    try:
        gf = float(g)
    except (TypeError, ValueError):
        return -1.0
    if abs(gf) < 0.005:
        return -1.0
    return gf


def report(mint_clusters: dict) -> None:
    if not OUTCOMES.exists():
        print("нет outcomes.jsonl")
        return

    rows = []
    with OUTCOMES.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    # аннотация
    annotated = 0
    for r in rows:
        m = r.get("mint")
        w = r.get("wallet")
        info = mint_clusters.get(m) or {}
        size = int(info.get("cluster_size") or 0)
        funder = info.get("funder")
        in_cluster = bool(
            info.get("is_cluster")
            and w
            and w in (info.get("cluster_wallets") or [])
        )
        r["_off_insider"] = in_cluster
        r["_off_size"] = size if in_cluster else 0
        r["_off_funder"] = funder if in_cluster else ""
        if m in mint_clusters:
            annotated += 1

    ins = [r for r in rows if r["_off_insider"]]
    base = [r for r in rows if not r["_off_insider"]]

    def summary(rs: list, label: str):
        if not rs:
            print(f"  {label}: n=0")
            return
        naive = [float(r["growth"]) for r in rs if isinstance(r.get("growth"), (int, float))]
        honest = [g for g in (_honest(r) for r in rs) if g is not None]
        wins = sum(1 for r in rs if r.get("is_win"))
        hw = sum(1 for r in rs if (_honest(r) or -1) >= settings.OUTCOME_WIN_THRESHOLD_PCT)
        print(f"  {label}: n={len(rs)}")
        if naive:
            print(
                f"    naive : WR={wins/len(rs):.1%} med={_pctile(naive,0.5):+.3f} "
                f"avg={sum(naive)/len(naive):+.3f} p90={_pctile(naive,0.9):+.3f}"
            )
        if honest:
            print(
                f"    honest: WR={hw/len(rs):.1%} med={_pctile(honest,0.5):+.3f} "
                f"avg={sum(honest)/len(honest):+.3f} p90={_pctile(honest,0.9):+.3f}"
            )

    n_cluster_mints = sum(1 for v in mint_clusters.values() if v.get("is_cluster"))
    print("\n" + "=" * 64)
    print("OFFLINE INSIDER LIFT (постфактум)")
    print("=" * 64)
    print(
        f"минтов с ≥{MIN_BUYERS} покупателями: {len(mint_clusters)} | "
        f"с кластером≥{MIN_SHARED}: {n_cluster_mints}"
    )
    print(f"outcomes с известным минтом: {annotated}/{len(rows)}")
    print(f"insider_cluster offline: {len(ins)} | база: {len(base)}")

    summary(base, "база")
    summary(ins, "insider (offline)")

    print("\n  по размеру funding-кластера:")
    for lo, hi, label in ((2, 2, "size=2"), (3, 3, "size=3"), (4, 10**9, "size≥4")):
        subset = [r for r in ins if lo <= r["_off_size"] <= hi]
        summary(subset, label)

    # holdout по времени
    rows_sorted = sorted(rows, key=lambda r: float(r.get("ts") or 0))
    mid = len(rows_sorted) // 2
    print("\n  holdout по времени:")
    for half, name in ((rows_sorted[:mid], "1-я половина"), (rows_sorted[mid:], "2-я половина")):
        summary([r for r in half if r["_off_insider"]], f"{name} · insider")
        summary([r for r in half if not r["_off_insider"]], f"{name} · база")

    size_hist = Counter(
        v["cluster_size"] for v in mint_clusters.values() if v.get("is_cluster")
    )
    print("\n  распределение размеров кластера (минты):", dict(sorted(size_hist.items())))


def daemonize(pid_path: str = "data/offline_insider.pid") -> None:
    """Двойной fork — процесс не умирает вместе с агент-шеллом Cursor."""
    Path(pid_path).parent.mkdir(parents=True, exist_ok=True)
    if os.fork() > 0:
        sys.exit(0)
    os.setsid()
    if os.fork() > 0:
        sys.exit(0)
    sys.stdout.flush()
    sys.stderr.flush()
    with open(pid_path, "w") as f:
        f.write(str(os.getpid()))


def main():
    ap = argparse.ArgumentParser(description="Офлайн insider: resolve + lift")
    ap.add_argument("--resolve", action="store_true", help="добрать funder'ов через RPC")
    ap.add_argument("--report", action="store_true", help="посчитать lift по outcomes")
    ap.add_argument("--daemon", action="store_true", help="отсоединиться от терминала (двойной fork)")
    ap.add_argument("--min-buyers", type=int, default=MIN_BUYERS)
    ap.add_argument("--per-min", type=int, default=20, help="RPC/мин офлайн (не трогает live gate)")
    ap.add_argument("--limit-wallets", type=int, default=None, help="потолок новых резолвов за запуск")
    ap.add_argument("--max-mints", type=int, default=None, help="случайный потолок минтов")
    args = ap.parse_args()

    if args.daemon:
        daemonize()

    if not args.resolve and not args.report:
        ap.print_help()
        print("\nНужен --resolve и/или --report")
        return

    mint_wallets = collect_mint_wallets(args.min_buyers)
    print(f"минтов с ≥{args.min_buyers} покупателями: {len(mint_wallets)}")

    if args.max_mints and len(mint_wallets) > args.max_mints:
        # детерминированно по mint id
        keep = sorted(mint_wallets)[: args.max_mints]
        mint_wallets = {m: mint_wallets[m] for m in keep}
        print(f"урезано до {len(mint_wallets)} минтов")

    wallets = sorted({w for ws in mint_wallets.values() for w in ws})
    origins: dict[str, dict] = {}

    if args.resolve:
        origins = asyncio.run(
            resolve_batch(wallets, per_min=args.per_min, limit=args.limit_wallets)
        )
        # добрать известных из БД для тех, кого limit отсёк
        now = time.time()
        for w in wallets:
            if w not in origins:
                o = origin_from_db(w, now)
                if o:
                    origins[w] = o
    else:
        now = time.time()
        for w in wallets:
            o = origin_from_db(w, now)
            if o:
                origins[w] = o
            else:
                cache = load_json_cache()
                if w in cache:
                    origins[w] = {
                        "birth": cache[w].get("birth"),
                        "funder": _sanitize_funder(cache[w].get("funder")),
                        "n_sigs": cache[w].get("n_sigs") or 0,
                        "capped": bool(cache[w].get("capped")),
                    }
        print(f"без --resolve: известно origins {len(origins)}/{len(wallets)}")

    now = time.time()
    # если resolve с limit — origins неполные; дотягиваем из db/json
    for w in wallets:
        if w not in origins:
            o = origin_from_db(w, now) or {}
            if o:
                origins[w] = o

    clusters = build_mint_clusters(mint_wallets, origins, now)
    MINTS_OUT.write_text(json.dumps(clusters, ensure_ascii=False, indent=1))
    print(f"сводка минтов → {MINTS_OUT} ({sum(1 for v in clusters.values() if v['is_cluster'])} кластеров)")

    if args.report:
        report(clusters)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        traceback.print_exc()
        raise
