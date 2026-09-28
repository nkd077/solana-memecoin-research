"""
Мониторинг здоровья: деградация WebSocket и квота/429 Helius.

Пишет события в БД и шлёт алерты в Telegram (если подключен).
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from config import settings
from core.db import Database, get_db

logger = logging.getLogger("sniper.health")


@dataclass
class HealthState:
    last_ws_event_at: float = 0.0
    last_ws_reconnect_at: float = 0.0
    ws_reconnects: int = 0
    helius_429: int = 0
    helius_errors: int = 0
    last_alert_at: dict = field(default_factory=dict)
    scanner_notifications: int = 0
    scanner_parsed: int = 0


class HealthMonitor:
    def __init__(self, db: Optional[Database] = None, telegram=None):
        self._db = db or get_db()
        self._telegram = telegram
        self.state = HealthState()
        self._started_at = time.time()

    def attach_telegram(self, telegram):
        self._telegram = telegram

    def note_ws_event(self):
        self.state.last_ws_event_at = time.time()
        self.state.scanner_notifications += 1

    def note_ws_parsed(self):
        self.state.scanner_parsed += 1

    def note_ws_reconnect(self):
        self.state.ws_reconnects += 1
        self.state.last_ws_reconnect_at = time.time()
        self._emit("ws_reconnect", f"переподключений: {self.state.ws_reconnects}")

    def note_helius_429(self):
        self.state.helius_429 += 1
        if self.state.helius_429 >= settings.HEALTH_HELIUS_429_THRESHOLD:
            self._emit(
                "helius_429",
                f"Helius 429 ×{self.state.helius_429} — квота/рейтлимит",
                force_alert=True,
            )

    def note_helius_error(self, detail: str = ""):
        self.state.helius_errors += 1
        self._emit("helius_error", detail or "ошибка Helius")

    def snapshot(self) -> dict:
        now = time.time()
        gap = (now - self.state.last_ws_event_at) if self.state.last_ws_event_at else None
        return {
            "uptime_sec": int(now - self._started_at),
            "ws_gap_sec": int(gap) if gap is not None else None,
            "ws_reconnects": self.state.ws_reconnects,
            "helius_429": self.state.helius_429,
            "helius_errors": self.state.helius_errors,
            "scanner_notifications": self.state.scanner_notifications,
            "scanner_parsed": self.state.scanner_parsed,
            "last_ws_event_at": self.state.last_ws_event_at,
        }

    async def run_forever(self):
        while True:
            await asyncio.sleep(15)
            try:
                await self._check_ws_gap()
            except Exception:  # noqa: BLE001
                logger.exception("health check failed")

    async def _check_ws_gap(self):
        if not settings.USE_WEBSOCKET_SCANNER:
            return
        if not self.state.last_ws_event_at:
            # ещё не было ни одного события — даём прогреться
            if time.time() - self._started_at < 90:
                return
            self._emit("ws_silent", "WS ещё не прислал ни одного события", force_alert=True)
            return
        gap = time.time() - self.state.last_ws_event_at
        if gap >= settings.HEALTH_ALERT_WS_GAP_SEC:
            self._emit(
                "ws_gap",
                f"нет событий WS уже {int(gap)}с (порог {settings.HEALTH_ALERT_WS_GAP_SEC}с)",
                force_alert=True,
            )

    def _emit(self, kind: str, detail: str, force_alert: bool = False):
        now = time.time()
        last = self.state.last_alert_at.get(kind, 0)
        # антиспам: не чаще раза в 5 минут на один kind
        if force_alert and (now - last) < 300:
            return
        self._db.log_health(kind, detail)
        logger.warning("HEALTH [%s]: %s", kind, detail)
        if force_alert:
            self.state.last_alert_at[kind] = now
            if self._telegram and self._telegram.enabled:
                asyncio.create_task(self._telegram.alert(f"[{kind}] {detail}"))
