"""
Telegram-бот: алерты, статус, ручной approve покупок.

Использует Bot API напрямую через aiohttp — без тяжёлых SDK.
Команды: /status, /pause, /resume, /positions, /help
Approve: inline-кнопки под алертом сигнала (если TELEGRAM_APPROVE_REQUIRED).
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional

import aiohttp

from config import settings
from core.db import Database, get_db

logger = logging.getLogger("sniper.telegram")

API = "https://api.telegram.org/bot{token}/{method}"


@dataclass
class PendingApprove:
    token_mint: str
    wallet: str
    sol_amount: float
    score: float
    created_at: float
    future: asyncio.Future


class TelegramBot:
    def __init__(self, session: aiohttp.ClientSession, db: Optional[Database] = None):
        self._session = session
        self._db = db or get_db()
        self._token = (settings.TELEGRAM_BOT_TOKEN or "").strip()
        self._chat_id = (settings.TELEGRAM_CHAT_ID or "").strip()
        self._offset = 0
        self._paused = False
        self._pending: dict[str, PendingApprove] = {}
        self._status_provider: Optional[Callable[[], Awaitable[str]]] = None
        self._positions_provider: Optional[Callable[[], Awaitable[str]]] = None

    @property
    def enabled(self) -> bool:
        return bool(self._token and self._chat_id)

    @property
    def paused(self) -> bool:
        return self._paused

    def set_providers(self, status, positions):
        self._status_provider = status
        self._positions_provider = positions

    async def _call(self, method: str, **payload) -> Optional[dict]:
        if not self._token:
            return None
        url = API.format(token=self._token, method=method)
        try:
            async with self._session.post(url, json=payload, timeout=20) as resp:
                if resp.status != 200:
                    logger.warning("Telegram %s -> %s", method, resp.status)
                    return None
                data = await resp.json()
                if not data.get("ok"):
                    logger.warning("Telegram %s error: %s", method, data)
                    return None
                return data.get("result")
        except Exception as exc:  # noqa: BLE001
            logger.debug("Telegram call failed: %s", exc)
            return None

    async def send(self, text: str, reply_markup: Optional[dict] = None) -> Optional[dict]:
        if not self.enabled:
            return None
        body = {
            "chat_id": self._chat_id,
            "text": text[:4000],
            "disable_web_page_preview": True,
        }
        if reply_markup:
            body["reply_markup"] = reply_markup
        return await self._call("sendMessage", **body)

    async def alert(self, text: str):
        await self.send(f"🔔 {text}")

    async def request_approve(self, *, token_mint: str, wallet: str,
                              sol_amount: float, score: float,
                              detail: str = "") -> bool:
        """Ждёт ручного OK/NO. True = можно покупать.

        Если бот выключен или approve не требуется — сразу True.
        Таймаут = отказ (безопаснее не купить, чем купить без подтверждения).
        """
        if not self.enabled or not settings.TELEGRAM_APPROVE_REQUIRED:
            return True
        if self._paused:
            await self.send(f"⏸ Пауза: сигнал {token_mint[:8]}… пропущен")
            return False

        key = f"{token_mint}:{int(time.time())}"
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending[key] = PendingApprove(
            token_mint=token_mint, wallet=wallet, sol_amount=sol_amount,
            score=score, created_at=time.time(), future=fut,
        )
        markup = {
            "inline_keyboard": [[
                {"text": "✅ Купить", "callback_data": f"ok:{key}"},
                {"text": "❌ Пропуск", "callback_data": f"no:{key}"},
            ]]
        }
        text = (
            f"📡 Сигнал на покупку\n"
            f"mint: `{token_mint}`\n"
            f"whale: `{wallet}`\n"
            f"size: {sol_amount:.4f} SOL | score: {score:.3f}\n"
            f"{detail}"
        )
        await self.send(text, reply_markup=markup)

        try:
            return await asyncio.wait_for(fut, timeout=settings.TELEGRAM_APPROVE_TIMEOUT_SEC)
        except asyncio.TimeoutError:
            self._pending.pop(key, None)
            await self.send(f"⌛️ Таймаут approve для {token_mint[:8]}… — пропуск")
            return False

    async def run_forever(self):
        if not self.enabled:
            logger.info("Telegram выключен (нет TELEGRAM_BOT_TOKEN/CHAT_ID)")
            # Не выходим: иначе supervisor в main.py рестартит задачу каждые 15с
            while True:
                await asyncio.sleep(3600)
            return
        await self.send(
            f"🤖 Sniper bot online (DRY_RUN={settings.DRY_RUN}, "
            f"approve={settings.TELEGRAM_APPROVE_REQUIRED})"
        )
        while True:
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                logger.exception("Telegram poll error")
            await asyncio.sleep(1.0)

    async def _poll_once(self):
        updates = await self._call(
            "getUpdates", offset=self._offset, timeout=25,
            allowed_updates=["message", "callback_query"],
        )
        if not updates:
            return
        for upd in updates:
            self._offset = max(self._offset, int(upd.get("update_id", 0)) + 1)
            if "callback_query" in upd:
                await self._on_callback(upd["callback_query"])
            elif "message" in upd:
                await self._on_message(upd["message"])

    async def _on_callback(self, cb: dict):
        data = (cb.get("data") or "")
        await self._call("answerCallbackQuery", callback_query_id=cb.get("id"))
        if ":" not in data:
            return
        action, key = data.split(":", 1)
        pending = self._pending.pop(key, None)
        if not pending or pending.future.done():
            return
        ok = action == "ok"
        pending.future.set_result(ok)
        await self.send("✅ Approve" if ok else "❌ Rejected")

    async def _on_message(self, msg: dict):
        chat = msg.get("chat") or {}
        if str(chat.get("id")) != str(self._chat_id):
            return
        text = (msg.get("text") or "").strip()
        if text.startswith("/status"):
            body = await self._status_provider() if self._status_provider else "нет провайдера"
            await self.send(body)
        elif text.startswith("/positions"):
            body = await self._positions_provider() if self._positions_provider else "нет позиций"
            await self.send(body)
        elif text.startswith("/pause"):
            self._paused = True
            await self.send("⏸ Торговля на паузе (сигналы не исполняются)")
        elif text.startswith("/resume"):
            self._paused = False
            await self.send("▶️ Торговля возобновлена")
        elif text.startswith("/help"):
            await self.send(
                "/status — сводка\n"
                "/positions — открытые позиции\n"
                "/pause — пауза исполнения\n"
                "/resume — снять паузу"
            )
