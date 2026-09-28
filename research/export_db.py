"""
Выгрузка outcomes из SQLite в JSONL для бэктеста / wallet_factors.

Запуск: python -m research.export_db
        python -m research.export_db --out data/outcomes_from_db.jsonl
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from core.db import get_db


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/outcomes_from_db.jsonl")
    args = ap.parse_args()
    db = get_db()
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with out.open("w", encoding="utf-8") as f:
        for row in db.export_outcomes_for_backtest():
            # совместимость с wallet_factors / analyze: growth, is_win, wallet, mint
            payload = {
                "ts": row["ts"],
                "wallet": row["wallet"],
                "mint": row["token_mint"],
                "token_mint": row["token_mint"],
                "entry_price_usd": row["entry_price_usd"],
                "exit_price_usd": row["exit_price_usd"],
                "growth": row["growth"],
                "is_win": bool(row["is_win"]),
                **(row.get("features") or {}),
            }
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
            n += 1
    print(f"exported {n} outcomes -> {out}")
    print("summary:", db.summary())


if __name__ == "__main__":
    main()
