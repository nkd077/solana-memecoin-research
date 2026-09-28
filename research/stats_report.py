"""
Отчёт по статистике сделок бота: читает data/trades_closed.jsonl и
(опционально) SQLite-сигналы — три слоя:

  1. Lab — исходы/базовая частота (всегда копится)
  2. Paper PnL — закрытые бумажные сделки, с разбивкой по strategy_tag
  3. Edge gate — сколько score-сигналов прошло бы / отрезало бы

Использование:
  python -m research.stats_report
  python -m research.stats_report --days 7
  python -m research.stats_report --top-whales 15
  python -m research.stats_report --insider-lift   # точка решения после эксперимента
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Optional

import pandas as pd

from config import settings

TRADES_LOG_PATH = settings.DATA_DIR / "trades_closed.jsonl"


def load_trades(path: Path, days: Optional[int]) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(
            f"Файл со статистикой не найден: {path}\n"
            "Бот ещё не закрыл ни одной позиции (или ни разу не запускался с новой версией кода)."
        )

    rows = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    if not rows:
        raise SystemExit("Файл со статистикой пуст — закрытых сделок пока нет.")

    df = pd.DataFrame(rows)
    df["logged_at"] = pd.to_datetime(df["logged_at"], unit="s")
    if "strategy_tag" not in df.columns:
        df["strategy_tag"] = "legacy"
    else:
        df["strategy_tag"] = df["strategy_tag"].fillna("legacy")
    if days:
        cutoff = pd.Timestamp.now() - pd.Timedelta(days=days)
        df = df[df["logged_at"] >= cutoff]
    return df


def _print_pnl_block(title: str, df: pd.DataFrame, top_whales: int):
    if df.empty:
        print(f"\n[{title}] сделок нет.")
        return

    closes = df[df["kind"] == "close"]
    partials = df[df["kind"] == "partial"]
    total_pnl = df["pnl_sol"].sum()
    n_closed = len(closes)
    wins = int((closes["pnl_sol"] > 0).sum()) if n_closed else 0
    winrate = wins / n_closed if n_closed else 0.0

    print(f"\n--- {title} ---")
    print(f"Закрыто: {n_closed} | в плюс: {wins} ({winrate:.0%}) | partials: {len(partials)}")
    print(f"Суммарный P&L: {total_pnl:+.4f} SOL")
    if n_closed:
        print(f"Средний P&L на закрытие: {closes['pnl_sol'].mean():+.4f} SOL")

    by_reason = df.groupby("reason_code")["pnl_sol"].agg(["count", "sum", "mean"])
    by_reason.columns = ["сделок", "суммарный P&L", "средний P&L"]
    print(by_reason.to_string(float_format=lambda x: f"{x:+.4f}"))

    if top_whales > 0 and "whale" in df.columns:
        by_whale = (
            df.groupby("whale")["pnl_sol"]
            .agg(["count", "sum", "mean"])
            .sort_values("sum", ascending=False)
        )
        by_whale.columns = ["сделок", "суммарный P&L", "средний P&L"]
        print(f"Топ-{top_whales} кошельков:")
        print(by_whale.head(top_whales).to_string(float_format=lambda x: f"{x:+.4f}"))


def print_report(df: pd.DataFrame, top_whales: int):
    if df.empty:
        print("За выбранный период сделок нет.")
        return

    dry_run = bool(df["dry_run"].iloc[0]) if "dry_run" in df else True
    print("=" * 64)
    print(f"СТАТИСТИКА СНАЙПИНГ-БОТА {'(DRY_RUN, бумажный P&L)' if dry_run else '(реальные сделки)'}")
    print("=" * 64)
    print(f"Период: {df['logged_at'].min()} — {df['logged_at'].max()}")
    print(
        f"Режим сейчас: ENTRY_MODE={settings.ENTRY_MODE} "
        f"ENFORCE_EDGE={settings.ENFORCE_EDGE_GATE} "
        f"WHALE_MIRROR={settings.WHALE_MIRROR_EXIT} "
        f"SCORE≥{settings.SCORE_THRESHOLD} CLUSTER≥{settings.MIN_CLUSTER_SIZE_TO_CONSIDER}"
    )

    _print_pnl_block("ВСЕ бумажные сделки (не смешивать пути!)", df, top_whales=0)

    for tag in sorted(df["strategy_tag"].dropna().unique()):
        label = {
            "score": "SCORE-путь (честный / должен проходить edge)",
            "insider_exp": "INSIDER-эксперимент v1 (funding-кластер)",
            "insider_exp_v2": "INSIDER-эксперимент v2 (size≥3, age, alive)",
            "insider_demo": "INSIDER paper-демка (анализ, не вердикт v2)",
            "legacy": "LEGACY (до разметки strategy_tag)",
        }.get(str(tag), str(tag))
        _print_pnl_block(label, df[df["strategy_tag"] == tag], top_whales)

    print("=" * 64)


def print_signal_gate_report(days: Optional[int]):
    """Счётчики gate из SQLite — видно, сколько отсекает честный edge."""
    db_path = Path(settings.DB_PATH)
    if not db_path.exists():
        return
    try:
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        where = ""
        params: tuple = ()
        if days:
            cutoff = pd.Timestamp.now().timestamp() - days * 86400
            where = " WHERE ts >= ?"
            params = (cutoff,)
        cur.execute(
            f"SELECT decision, COUNT(*) FROM signals{where} GROUP BY decision ORDER BY COUNT(*) DESC",
            params,
        )
        rows = cur.fetchall()
        conn.close()
    except Exception as exc:  # noqa: BLE001
        print(f"\n(сигналы SQLite недоступны: {exc})")
        return

    if not rows:
        return

    print("\n" + "=" * 64)
    print("LAB / GATES (сигналы SQLite, не PnL)")
    print("=" * 64)
    interesting = {
        "entry_ok_score", "entry_ok_insider", "entry_reject",
        "edge_pass", "edge_fail", "edge_reject_trade",
        "risk_reject", "score_pass", "score_fail",
    }
    for decision, n in rows:
        if decision in interesting or n >= 20:
            print(f"  {decision:<28} {n:>8}")

    by = dict(rows)
    edge_pass = by.get("edge_pass", 0)
    edge_fail = by.get("edge_fail", 0)
    edge_reject = by.get("edge_reject_trade", 0)
    insider_ok = by.get("entry_ok_insider", 0)
    score_ok = by.get("entry_ok_score", 0)
    print(
        f"\n  score-входов кандидатов: {score_ok} | insider-входов: {insider_ok} | "
        f"edge pass/fail: {edge_pass}/{edge_fail} | edge отрезал сделок: {edge_reject}"
    )
    print(
        "  Lab (outcomes) копится отдельно через register_early_buy — "
        "даже когда покупок 0."
    )


def print_edge_report():
    """Главный вопрос: обыгрывают ли отслеживаемые кошельки случайный выбор?"""
    p = Path("data/cache_state.json")
    if not p.exists():
        return
    try:
        d = json.load(open(p))
    except Exception:
        return

    raw = d.get("baseline:outcomes")
    baseline_rate, baseline_n = 0.0, 0
    if raw:
        try:
            b = json.loads(raw[0])
            baseline_n = b.get("total", 0)
            baseline_rate = b.get("wins", 0) / baseline_n if baseline_n else 0.0
        except Exception:
            pass

    profiles = []
    for k, v in d.items():
        if not (k.startswith("wallet:") or k.startswith("profile:")):
            continue
        try:
            pr = json.loads(v[0])
        except Exception:
            continue
        n = pr.get("wins", 0) + pr.get("losses", 0)
        if n >= 5:
            profiles.append((k.split(":", 1)[-1], pr.get("winrate", 0), n,
                             pr.get("is_proven_smart_money", False)))

    print("\n" + "=" * 62)
    print("LAB: ПРЕВОСХОДСТВО НАД СЛУЧАЙНЫМ ВЫБОРОМ")
    print("=" * 62)
    if baseline_n < 50:
        print(f"Базовая частота ещё набирается: {baseline_n} наблюдений (нужно от 100)")
        return
    print(f"Базовая частота: {baseline_rate*100:.1f}% ({baseline_n} наблюдений)")
    print(f"Для выхода в ноль нужен отбор примерно втрое лучше случайного\n")

    if not profiles:
        print("Кошельков с 5+ наблюдениями пока нет")
        return
    profiles.sort(key=lambda x: -x[1])
    print(f"{'кошелёк':<20} {'винрейт':>9} {'набл.':>6} {'превосх.':>9}  статус")
    for w, wr, n, smart in profiles[:15]:
        edge = wr / baseline_rate if baseline_rate else 0
        print(f"{w[:18]:<20} {wr*100:>8.1f}% {n:>6} {edge:>8.2f}x  "
              f"{'СМАРТ-МАНИ' if smart else ''}")

    above = sum(1 for _, wr, _, _ in profiles if wr > baseline_rate)
    print(f"\nКошельков выше базовой частоты: {above} из {len(profiles)}")
    if above <= len(profiles) * 0.5:
        print("Пока не видно, что отслеживаемые кошельки в целом обыгрывают рынок.")


def print_accum_report(days: Optional[int]):
    """Shadow-lab accumulation vs early — раздельные винрейты."""
    path = settings.DATA_DIR / "outcomes.jsonl"
    if not path.exists():
        return
    rows = []
    cutoff = None
    if days:
        cutoff = pd.Timestamp.now().timestamp() - days * 86400
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            if cutoff and float(r.get("ts") or 0) < cutoff:
                continue
            rows.append(r)
    if not rows:
        return

    print("\n" + "=" * 64)
    print("LAB TRACKS: early vs accumulation (shadow)")
    print("=" * 64)
    by = {}
    for r in rows:
        track = r.get("lab_track") or "lab_early"
        by.setdefault(track, []).append(r)

    for track in sorted(by.keys()):
        rs = by[track]
        n = len(rs)
        wins = sum(1 for r in rs if r.get("is_win"))
        growths = [r["growth"] for r in rs if isinstance(r.get("growth"), (int, float))]
        avg_g = sum(growths) / len(growths) if growths else 0.0
        print(f"\n  [{track}] n={n} wins={wins} WR={wins/n:.1%} avg_growth={avg_g:+.3f}")
        if track == "lab_accum":
            reasons = {}
            tw = 0
            for r in rs:
                reasons[r.get("accum_reason") or "?"] = reasons.get(r.get("accum_reason") or "?", 0) + 1
                if r.get("catalyst_has_twitter"):
                    tw += 1
            print(f"    reasons: {reasons}")
            print(f"    with twitter (dex proxy): {tw}/{n}")

    # SQLite accum_shadow signals
    db_path = Path(settings.DB_PATH)
    if db_path.exists():
        try:
            import sqlite3
            conn = sqlite3.connect(str(db_path))
            q = "SELECT COUNT(*) FROM signals WHERE decision='accum_shadow'"
            params: tuple = ()
            if days:
                q += " AND ts>=?"
                params = (cutoff,)
            n_sig = conn.execute(q, params).fetchone()[0]
            conn.close()
            print(f"\n  signals accum_shadow (SQLite): {n_sig}")
        except Exception:
            pass
    print(
        "\n  ACCUM_BUY_ENABLED="
        f"{settings.ACCUM_BUY_ENABLED} — paper-сделки accum "
        f"{'вкл' if settings.ACCUM_BUY_ENABLED else 'выкл (только shadow)'}"
    )


def print_insider_lift_report(days: Optional[int] = None):
    """Точка решения после insider-эксперимента.

    Сравнивает outcomes с insider_cluster=true vs база, по размеру
    funding-кластера (2 / 3 / 4+) и с разрезом по времени (1-я vs 2-я половина).

    По каждому срезу — наивный рост (по котировке) и честный (мёртвый
    рынок = −100%), плюс p90: insider-гипотеза про хвост.
    """
    path = settings.DATA_DIR / "outcomes.jsonl"
    if not path.exists():
        print("outcomes.jsonl нет — нечего считать")
        return

    cutoff = None
    if days:
        cutoff = pd.Timestamp.now().timestamp() - days * 86400

    rows = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            if cutoff and float(r.get("ts") or 0) < cutoff:
                continue
            rows.append(r)

    if len(rows) < 20:
        print(f"Мало outcomes ({len(rows)}) для lift — нужно больше наблюдений")
        return

    rows.sort(key=lambda r: float(r.get("ts") or 0))

    n_flag = sum(1 for r in rows if r.get("market_alive") is not None)
    n_dead_flag = sum(1 for r in rows if r.get("market_alive") is False)
    n_proxy_dead = sum(
        1 for r in rows
        if r.get("market_alive") is None
        and (
            r.get("growth") is None
            or not r.get("exit_price_usd")
            or (
                isinstance(r.get("growth"), (int, float))
                and abs(float(r["growth"])) < 0.005
            )
        )
    )

    def _pctile(vals: list[float], q: float) -> float:
        if not vals:
            return 0.0
        s = sorted(vals)
        # nearest-rank
        idx = min(max(int(round(q * (len(s) - 1))), 0), len(s) - 1)
        return s[idx]

    def _honest_growth(r: dict) -> Optional[float]:
        """Честный рост: неисполнимый выход = −100%.

        Пересчитываем по exit_liquidity / txns (строгий порог), даже если
        в строке старый market_alive=True на пуле $18.
        """
        # строгий пересчёт, если есть ликвидность выхода
        liq = r.get("exit_liquidity_usd")
        if liq is not None:
            try:
                from research.recompute_honest import honest_growth as _hg
                return _hg(r)
            except Exception:
                pass
        if r.get("exit_tradeable") is False:
            return -1.0
        if r.get("honest_growth") is not None and r.get("market_alive") is not None:
            try:
                return float(r["honest_growth"])
            except (TypeError, ValueError):
                pass
        if r.get("market_alive") is False:
            return -1.0
        if r.get("market_alive") is True:
            g = r.get("growth")
            return float(g) if isinstance(g, (int, float)) else -1.0
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

    def _summary(rs: list, label: str):
        if not rs:
            print(f"  {label}: n=0")
            return
        wins = sum(1 for r in rs if r.get("is_win"))
        naive = [float(r["growth"]) for r in rs if isinstance(r.get("growth"), (int, float))]
        honest = [g for g in (_honest_growth(r) for r in rs) if g is not None]
        dead_n = sum(1 for r in rs if _honest_growth(r) == -1.0 and (
            r.get("market_alive") is False
            or r.get("market_alive") is None
            and (
                r.get("growth") is None
                or not r.get("exit_price_usd")
                or (isinstance(r.get("growth"), (int, float)) and abs(float(r["growth"])) < 0.005)
            )
        ))

        def _line(tag: str, vals: list[float], wr: float):
            if not vals:
                print(f"    {tag}: n=0")
                return
            med = _pctile(vals, 0.5)
            avg = sum(vals) / len(vals)
            p90 = _pctile(vals, 0.9)
            print(
                f"    {tag}: n={len(vals)} WR={wr:.1%} "
                f"med={med:+.3f} avg={avg:+.3f} p90={p90:+.3f}"
            )

        honest_wins = sum(
            1 for r in rs
            if (_honest_growth(r) or -1) >= settings.OUTCOME_WIN_THRESHOLD_PCT
        )
        print(f"  {label}: n={len(rs)}  dead/untradeable≈{dead_n} ({dead_n/len(rs):.0%})")
        _line("naive ", naive, wins / len(rs))
        _line("honest", honest, honest_wins / len(rs) if rs else 0.0)
        if naive and honest:
            gap = (sum(naive) / len(naive)) - (sum(honest) / len(honest))
            print(f"    gap avg(naive−honest)={gap:+.3f}  ← завышение от «нулей»")

    base = [r for r in rows if not r.get("insider_cluster")]
    ins = [r for r in rows if r.get("insider_cluster")]

    print("\n" + "=" * 64)
    print("INSIDER LIFT (точка решения эксперимента)")
    print("=" * 64)
    print("entry_ok_insider в SQLite: ", end="")
    try:
        conn = sqlite3.connect(str(settings.DB_PATH))
        n1 = conn.execute(
            "SELECT COUNT(*) FROM signals WHERE decision='entry_ok_insider'"
        ).fetchone()[0]
        n2 = conn.execute(
            "SELECT COUNT(*) FROM signals WHERE decision='entry_ok_insider_v2'"
        ).fetchone()[0]
        conn.close()
        print(
            f"v1={n1} (frozen) | v2={n2} / лимит {settings.INSIDER_EXPERIMENT_MAX_SIGNALS} "
            f"(phase={settings.INSIDER_EXPERIMENT_PHASE})"
        )
    except Exception:
        print("n/a")
    print(
        f"  market_alive в журнале: {n_flag}/{len(rows)} строк "
        f"(dead по флагу={n_dead_flag}; legacy-прокси dead≈{n_proxy_dead})"
    )
    print("  honest: нет сделок на выходе → growth=−100%; p90 — хвост гипотезы")

    _summary(base, "база (insider_cluster=false/absent)")
    _summary(ins, "insider_cluster=true (все размеры)")

    def size_bucket(r) -> str:
        # funding cohort size, не co-buy cluster_size
        n = r.get("insider_cluster_size")
        if n is None:
            n = 0
        try:
            n = int(n)
        except (TypeError, ValueError):
            n = 0
        if n <= 2:
            return "size=2"
        if n == 3:
            return "size=3"
        return "size≥4"

    print("\n  по размеру funding-кластера:")
    for bucket in ("size=2", "size=3", "size≥4"):
        _summary([r for r in ins if size_bucket(r) == bucket], bucket)

    mid = len(rows) // 2
    first, second = rows[:mid], rows[mid:]
    print("\n  holdout по времени:")
    _summary([r for r in first if r.get("insider_cluster")], "1-я половина · insider")
    _summary([r for r in first if not r.get("insider_cluster")], "1-я половина · база")
    _summary([r for r in second if r.get("insider_cluster")], "2-я половина · insider")
    _summary([r for r in second if not r.get("insider_cluster")], "2-я половина · база")

    # lab_insider paper tracks (v1 + v2)
    for track, label in (
        ("lab_insider", "lab_insider v1 (бумажные входы)"),
        ("lab_insider_v2", "lab_insider v2 (строже: size≥3, age, alive)"),
        ("lab_insider_demo", "lab_insider DEMO (paper при young/untradeable)"),
    ):
        paper = [r for r in rows if r.get("lab_track") == track]
        if paper:
            print()
            _summary(paper, label)


def main():
    parser = argparse.ArgumentParser(description="Отчёт по статистике сделок бота")
    parser.add_argument("--days", type=int, default=None, help="только сделки за последние N дней")
    parser.add_argument("--top-whales", type=int, default=10)
    parser.add_argument(
        "--insider-lift", action="store_true",
        help="отчёт lift insider_cluster vs база (точка решения эксперимента)",
    )
    args = parser.parse_args()

    if args.insider_lift:
        print_insider_lift_report(args.days)
        return

    try:
        df = load_trades(TRADES_LOG_PATH, args.days)
        print_report(df, args.top_whales)
    except SystemExit as e:
        print(e)
    print_signal_gate_report(args.days)
    print_accum_report(args.days)
    print_edge_report()
    # короткий тизер lift, если уже есть insider outcomes
    path = settings.DATA_DIR / "outcomes.jsonl"
    if path.exists():
        n_ins = 0
        with open(path, encoding="utf-8") as f:
            for line in f:
                try:
                    if json.loads(line).get("insider_cluster"):
                        n_ins += 1
                except Exception:
                    pass
        if n_ins >= 30:
            print(f"\n(есть {n_ins} insider outcomes — полный разбор: python -m research.stats_report --insider-lift)")


if __name__ == "__main__":
    main()
