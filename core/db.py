"""
SQLite-хранилище истории сигналов, сделок, фич и исходов.

Одно долгоживущее соединение + WAL: в горячем пути теперь ещё и
COUNT по funder — открывать файл на каждый вызов слишком дорого.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Optional

from config import settings

logger = logging.getLogger("sniper.db")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    token_mint TEXT NOT NULL,
    wallet TEXT NOT NULL,
    sol_amount REAL,
    score REAL,
    passed INTEGER,
    features_json TEXT,
    decision TEXT,
    reason TEXT
);
CREATE INDEX IF NOT EXISTS idx_signals_ts ON signals(ts);
CREATE INDEX IF NOT EXISTS idx_signals_mint ON signals(token_mint);
CREATE INDEX IF NOT EXISTS idx_signals_wallet ON signals(wallet);
CREATE INDEX IF NOT EXISTS idx_signals_decision ON signals(decision);

CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    token_mint TEXT NOT NULL,
    whale TEXT,
    side TEXT NOT NULL,
    size_sol REAL,
    price_usd REAL,
    signature TEXT,
    dry_run INTEGER,
    reason_code TEXT,
    pnl_sol REAL,
    multiple REAL
);
CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(ts);
CREATE INDEX IF NOT EXISTS idx_trades_mint ON trades(token_mint);

CREATE TABLE IF NOT EXISTS outcomes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    wallet TEXT NOT NULL,
    token_mint TEXT NOT NULL,
    entry_price_usd REAL,
    exit_price_usd REAL,
    growth REAL,
    is_win INTEGER,
    features_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_outcomes_wallet ON outcomes(wallet);
CREATE INDEX IF NOT EXISTS idx_outcomes_ts ON outcomes(ts);
CREATE INDEX IF NOT EXISTS idx_outcomes_mint_wallet ON outcomes(token_mint, wallet);

CREATE TABLE IF NOT EXISTS wallet_funders (
    wallet TEXT PRIMARY KEY,
    birth_unix REAL,
    funder TEXT,
    n_sigs INTEGER,
    capped INTEGER,
    updated_at REAL
);
CREATE INDEX IF NOT EXISTS idx_wallet_funders_funder ON wallet_funders(funder);

CREATE TABLE IF NOT EXISTS smart_money (
    wallet TEXT PRIMARY KEY,
    source TEXT,
    winrate REAL,
    trades INTEGER,
    added_at REAL,
    last_seen_at REAL
);

CREATE TABLE IF NOT EXISTS health_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    kind TEXT NOT NULL,
    detail TEXT
);
"""


class Database:
    def __init__(self, path: Optional[Path] = None):
        self._path = Path(path or settings.DB_PATH)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(
            str(self._path), timeout=30, check_same_thread=False,
        )
        self._conn.row_factory = sqlite3.Row
        # WAL + synchronous=NORMAL — быстрее горячего пути.
        # Компромисс: при жёстком отключении питания последние
        # незакоммиченные транзакции могут потеряться. Для бумажного
        # журнала сигналов это приемлемо; не трогать без нужды.
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA temp_store=MEMORY")
        self._init()

    def _init(self):
        with self._lock:
            self._conn.executescript(_SCHEMA)
            self._conn.commit()

    @contextmanager
    def _connect(self):
        """Долгоживущее соединение под локом (не open/close на каждый вызов)."""
        with self._lock:
            try:
                yield self._conn
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def close(self):
        with self._lock:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001
                pass

    def log_signal(self, *, token_mint: str, wallet: str, sol_amount: float,
                   score: float, passed: bool, features: dict, decision: str,
                   reason: str = ""):
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO signals(ts,token_mint,wallet,sol_amount,score,passed,"
                "features_json,decision,reason) VALUES(?,?,?,?,?,?,?,?,?)",
                (time.time(), token_mint, wallet, sol_amount, score, int(passed),
                 json.dumps(features, ensure_ascii=False), decision, reason),
            )

    def count_signals(self, decision: str) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM signals WHERE decision=?", (decision,)
            ).fetchone()
            return int(row["n"] if row else 0)

    def log_trade(self, *, token_mint: str, whale: str, side: str, size_sol: float,
                  price_usd: float = 0.0, signature: str = "", dry_run: bool = True,
                  reason_code: str = "", pnl_sol: float = 0.0, multiple: float = 0.0):
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO trades(ts,token_mint,whale,side,size_sol,price_usd,"
                "signature,dry_run,reason_code,pnl_sol,multiple) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (time.time(), token_mint, whale, side, size_sol, price_usd,
                 signature, int(dry_run), reason_code, pnl_sol, multiple),
            )

    def log_outcome(self, *, wallet: str, token_mint: str, entry_price_usd: float,
                    exit_price_usd: float, growth: float, is_win: bool,
                    features: Optional[dict] = None):
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO outcomes(ts,wallet,token_mint,entry_price_usd,"
                "exit_price_usd,growth,is_win,features_json) VALUES(?,?,?,?,?,?,?,?)",
                (time.time(), wallet, token_mint, entry_price_usd, exit_price_usd,
                 growth, int(is_win), json.dumps(features or {}, ensure_ascii=False)),
            )

    def upsert_wallet_funder(self, wallet: str, birth_unix: Optional[float],
                             funder: Optional[str], n_sigs: int = 0, capped: bool = False):
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO wallet_funders(wallet,birth_unix,funder,n_sigs,capped,updated_at) "
                "VALUES(?,?,?,?,?,?) "
                "ON CONFLICT(wallet) DO UPDATE SET "
                "birth_unix=excluded.birth_unix, funder=excluded.funder, "
                "n_sigs=excluded.n_sigs, capped=excluded.capped, updated_at=excluded.updated_at",
                (wallet, birth_unix, funder, n_sigs, int(capped), time.time()),
            )

    def get_wallet_funder(self, wallet: str) -> Optional[dict]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM wallet_funders WHERE wallet=?", (wallet,)
            ).fetchone()
            return dict(row) if row else None

    def count_wallets_by_funder(self, funder: str) -> int:
        """Глобальный fanout funder'а по SQLite — индекс по funder обязателен."""
        if not funder:
            return 0
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM wallet_funders WHERE funder=?",
                (funder,),
            ).fetchone()
            return int(row["n"] if row else 0)

    def upsert_smart_money(self, wallet: str, source: str, winrate: float = 0.0,
                           trades: int = 0):
        now = time.time()
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO smart_money(wallet,source,winrate,trades,added_at,last_seen_at) "
                "VALUES(?,?,?,?,?,?) "
                "ON CONFLICT(wallet) DO UPDATE SET "
                "source=excluded.source, winrate=excluded.winrate, trades=excluded.trades, "
                "last_seen_at=excluded.last_seen_at",
                (wallet, source, winrate, trades, now, now),
            )

    def list_smart_money(self) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT wallet FROM smart_money ORDER BY last_seen_at DESC"
            ).fetchall()
            return [r["wallet"] for r in rows]

    def log_health(self, kind: str, detail: str):
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO health_events(ts,kind,detail) VALUES(?,?,?)",
                (time.time(), kind, detail),
            )

    def recent_signals(self, limit: int = 50) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM signals ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]

    def recent_trades(self, limit: int = 50) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]

    def recent_health(self, limit: int = 30) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM health_events ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]

    def summary(self) -> dict[str, Any]:
        with self._connect() as conn:
            def _count(table: str) -> int:
                return int(conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"])

            wins = conn.execute(
                "SELECT COUNT(*) AS n FROM outcomes WHERE is_win=1"
            ).fetchone()["n"]
            outcomes = _count("outcomes")
            return {
                "signals": _count("signals"),
                "trades": _count("trades"),
                "outcomes": outcomes,
                "outcome_wins": int(wins),
                "smart_money": _count("smart_money"),
                "wallet_funders": _count("wallet_funders"),
            }

    def export_outcomes_for_backtest(self) -> Iterable[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT ts,wallet,token_mint,entry_price_usd,exit_price_usd,growth,is_win,features_json "
                "FROM outcomes ORDER BY ts"
            ).fetchall()
            for r in rows:
                d = dict(r)
                try:
                    d["features"] = json.loads(d.pop("features_json") or "{}")
                except json.JSONDecodeError:
                    d["features"] = {}
                yield d


_db: Optional[Database] = None


def get_db() -> Database:
    global _db
    if _db is None:
        _db = Database()
    return _db
