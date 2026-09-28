"""
Стабильный smart-money фид без зависимости от хрупкого GMGN WS.

Источники (объединяются):
  1. Кошельки с is_proven_smart_money из Redis/кэша репутации
  2. Файл SMART_MONEY_FILE (по одному адресу на строку)
  3. Таблица smart_money в SQLite (накопленные / импортированные)

Фид периодически синхронизирует proven-кошельки в БД и отдаёт set()
для скоринга и опциональной подписки.
"""
from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Optional, Set

from config import settings
from core.db import Database, get_db
from core.redis_cache import RedisCache

logger = logging.getLogger("sniper.smart_money_feed")


class SmartMoneyFeed:
    def __init__(self, cache: RedisCache, db: Optional[Database] = None):
        self._cache = cache
        self._db = db or get_db()
        self._wallets: Set[str] = set()
        self._file_mtime = 0.0

    @property
    def wallets(self) -> Set[str]:
        return set(self._wallets)

    def contains(self, wallet: str) -> bool:
        return wallet in self._wallets

    async def bootstrap(self):
        await self._load_file()
        for w in self._db.list_smart_money():
            self._wallets.add(w)
        logger.info("SmartMoneyFeed: %d кошельков после bootstrap", len(self._wallets))

    async def note_proven(self, wallet: str, winrate: float, trades: int):
        self._wallets.add(wallet)
        self._db.upsert_smart_money(wallet, source="proven", winrate=winrate, trades=trades)

    async def run_forever(self):
        await self.bootstrap()
        while True:
            try:
                await self._load_file()
                await self._sync_from_cache()
            except Exception:  # noqa: BLE001
                logger.exception("SmartMoneyFeed sync error")
            await asyncio.sleep(settings.SMART_MONEY_SYNC_SEC)

    async def _load_file(self):
        path = Path(settings.SMART_MONEY_FILE)
        if not path.exists():
            return
        mtime = path.stat().st_mtime
        if mtime <= self._file_mtime:
            return
        self._file_mtime = mtime
        added = 0
        for line in path.read_text(encoding="utf-8").splitlines():
            w = line.strip()
            if not w or w.startswith("#"):
                continue
            if w not in self._wallets:
                added += 1
            self._wallets.add(w)
            self._db.upsert_smart_money(w, source="file")
        if added:
            logger.info("SmartMoneyFeed: +%d из файла %s", added, path)

    async def _sync_from_cache(self):
        """Подтягивает proven-кошельки из in-memory/Redis снимка.

        RedisCache хранит профили по ключам wallet:* — точный SCAN зависит
        от бэкенда. Используем публичный API, если есть; иначе читаем
        data/cache_state.json напрямую как запасной путь.
        """
        # Запасной путь: снимок с диска
        snap = settings.DATA_DIR / "cache_state.json"
        if not snap.exists():
            return
        try:
            import json
            raw = json.loads(snap.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            return

        added = 0
        for key, item in (raw or {}).items():
            if not isinstance(key, str) or not key.startswith("wallet:"):
                continue
            try:
                value, _exp = item
                profile = json.loads(value) if isinstance(value, str) else value
            except Exception:  # noqa: BLE001
                continue
            if not isinstance(profile, dict):
                continue
            if not profile.get("is_proven_smart_money"):
                continue
            wallet = key.split("wallet:", 1)[1]
            if wallet not in self._wallets:
                added += 1
            self._wallets.add(wallet)
            self._db.upsert_smart_money(
                wallet,
                source="cache",
                winrate=float(profile.get("winrate") or 0),
                trades=int(profile.get("trades_seen") or 0),
            )
        if added:
            logger.info("SmartMoneyFeed: +%d proven из cache_state", added)

    def export_file(self):
        path = Path(settings.SMART_MONEY_FILE)
        path.parent.mkdir(parents=True, exist_ok=True)
        lines = sorted(self._wallets)
        path.write_text("# auto-exported smart money\n" + "\n".join(lines) + "\n", encoding="utf-8")
        return path
