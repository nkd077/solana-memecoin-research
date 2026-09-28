"""
Учёт статистики закрытых сделок. Каждая продажа (частичная фиксация
прибыли или полное закрытие позиции) дописывается отдельной строкой в
data/trades_closed.jsonl — построчный JSON, а не Redis, чтобы статистика
переживала перезапуск бота (Redis без диска или наш in-memory фолбэк
теряют всё при рестарте) и была легко анализируема (grep, pandas, Excel).

Отчёт по накопленной статистике смотрите через `python -m research.stats_report`
(в корне проекта).
"""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Optional

from config import settings

logger = logging.getLogger("sniper.stats")

TRADES_LOG_PATH = settings.DATA_DIR / "trades_closed.jsonl"


class TradeStats:
    def __init__(self, path: Path = TRADES_LOG_PATH):
        self._path = path
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def record_exit(
        self,
        mint: str,
        whale: str,
        kind: str,           # "partial" | "close"
        reason_code: str,    # "stop_loss" | "partial_tp_1" | "partial_tp_2" | "trailing_stop" | "max_hold" | "whale_sell" | "no_price"
        reason: str,         # человекочитаемая деталь для логов
        sold_sol: float,
        pnl_sol: float,
        entry_price_usd: float,
        exit_price_usd: float,
        opened_at: float,
        dry_run: bool,
        signature: Optional[str],
        strategy_tag: str = "legacy",
    ):
        multiple = (exit_price_usd / entry_price_usd) if entry_price_usd > 0 else 0.0
        record = {
            "logged_at": time.time(),
            "mint": mint,
            "whale": whale,
            "kind": kind,
            "reason_code": reason_code,
            "reason": reason,
            "sold_sol": round(sold_sol, 6),
            "pnl_sol": round(pnl_sol, 6),
            "entry_price_usd": entry_price_usd,
            "exit_price_usd": exit_price_usd,
            "multiple": round(multiple, 4),
            "held_minutes": round((time.time() - opened_at) / 60, 1),
            "dry_run": dry_run,
            "signature": signature,
            "strategy_tag": strategy_tag or "legacy",
        }
        try:
            with open(self._path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        except Exception:  # noqa: BLE001
            logger.exception("Не удалось записать статистику сделки")
