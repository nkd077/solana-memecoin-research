"""
Общий бюджет Helius RPC для всего процесса.

Без этого funding + wallet_screener + rug бьют один и тот же ключ
параллельно и за минуты ловят 429. Здесь:
  - лимит запросов в минуту
  - cooldown после 429
  - единая точка, чтобы все модули молча пропускали RPC
"""
from __future__ import annotations

import logging
import time
from collections import deque
from typing import Optional

from config import settings

logger = logging.getLogger("sniper.helius_gate")


class HeliusGate:
    def __init__(self):
        self._times: deque[float] = deque()
        self._cooldown_until = 0.0
        self._health = None
        self._last_skip_log = 0.0
        self._skipped = 0

    def attach_health(self, health) -> None:
        self._health = health

    @property
    def available(self) -> bool:
        return time.time() >= self._cooldown_until

    def cooldown_remaining(self) -> float:
        return max(0.0, self._cooldown_until - time.time())

    def allow(self) -> bool:
        """True = можно слать следующий RPC-запрос."""
        now = time.time()
        if now < self._cooldown_until:
            self._note_skip()
            return False

        window = 60.0
        while self._times and now - self._times[0] > window:
            self._times.popleft()

        max_per_min = max(1, int(settings.HELIUS_RPC_MAX_PER_MIN))
        if len(self._times) >= max_per_min:
            self._note_skip()
            return False

        self._times.append(now)
        return True

    async def acquire(self, poll_sec: float = 1.0) -> None:
        """Ждать слот бюджета (для долгих поллеров, не для hot-path)."""
        import asyncio
        while not self.allow():
            wait = max(poll_sec, min(5.0, self.cooldown_remaining() or poll_sec))
            await asyncio.sleep(wait)

    def note_429(self) -> None:
        cool = max(60.0, float(settings.HELIUS_RPC_COOLDOWN_SEC))
        self._cooldown_until = time.time() + cool
        if self._health is not None:
            self._health.note_helius_429()
        logger.warning(
            "Helius 429 — общий RPC cooldown %.0f сек (funding/screener/rug ждут)",
            cool,
        )

    def note_error(self, detail: str = "") -> None:
        if self._health is not None:
            self._health.note_helius_error(detail)

    def _note_skip(self) -> None:
        self._skipped += 1
        now = time.time()
        if now - self._last_skip_log >= 60:
            logger.info(
                "Helius gate: пропущено RPC=%d, cooldown_left=%.0fs, budget=%d/min",
                self._skipped,
                self.cooldown_remaining(),
                settings.HELIUS_RPC_MAX_PER_MIN,
            )
            self._skipped = 0
            self._last_skip_log = now


_gate: Optional[HeliusGate] = None


def get_helius_gate() -> HeliusGate:
    global _gate
    if _gate is None:
        _gate = HeliusGate()
    return _gate
