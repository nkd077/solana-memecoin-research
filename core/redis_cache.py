"""
Кэш на Redis для профилей кошельков, кластеров покупок и открытых позиций.
Если Redis недоступен, автоматически откатывается на in-memory словарь
(удобно для локальной разработки и DRY_RUN без Redis).
"""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any, Optional

from config import settings

logger = logging.getLogger("sniper.redis_cache")

try:
    import redis.asyncio as aioredis
except ImportError:  # redis-py не установлен
    aioredis = None


class _InMemoryFallback:
    """Замена Redis с сохранением на диск.

    Важно, почему сохранение обязательно, а не «приятное дополнение»:
    репутация кошелька засчитывается только после
    MIN_TRADES_FOR_TRUSTED_WINRATE отслеженных исходов, а кошельки в
    потоке Pump.fun повторяются редко — набор этих наблюдений занимает
    дни. Если кэш живёт только в памяти, любой перезапуск обнуляет
    счётчик, и порог не будет достигнут никогда, сколько бота ни гоняй.

    Пишем весь снимок в JSON не чаще раза в FLUSH_INTERVAL_SEC (записей
    десятки в минуту, дёргать диск на каждую незачем) и обязательно при
    корректном завершении."""

    FLUSH_INTERVAL_SEC = 120
    # Не дампить гигантский снимок синхронно в event loop — иначе WS
    # heartbeat рвётся и процесс выглядит «зависшим», а потом его убивают.

    def __init__(self, persist_path: Optional[Path] = None):
        self._store: dict[str, tuple[str, Optional[float]]] = {}
        self._persist_path = persist_path
        self._last_flush = 0.0
        self._dirty = False
        self._load()

    def _load(self):
        if not self._persist_path or not self._persist_path.exists():
            return
        try:
            with open(self._persist_path, encoding="utf-8") as f:
                raw = json.load(f)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Не удалось прочитать сохранённый кэш (%s) — начинаем с пустого", exc)
            return

        now = time.time()
        restored = 0
        for key, item in (raw or {}).items():
            try:
                value, expires_at = item
            except (TypeError, ValueError):
                continue
            if expires_at is not None and now > expires_at:
                continue  # запись протухла, пока бот был выключен
            self._store[key] = (value, expires_at)
            restored += 1
        if restored:
            logger.info("Восстановлен кэш с диска: %d записей (репутация кошельков не потеряна)", restored)

    def _flush(self, force: bool = False):
        if not self._persist_path or not self._dirty:
            return
        now = time.time()
        if not force and (now - self._last_flush) < self.FLUSH_INTERVAL_SEC:
            return
        # Протухшие записи иначе удаляются только при чтении по ключу, а
        # ключи вроде "видели такой mint" читаются один раз. За сутки работы
        # файл распух бы от мусора, который заново читается при каждом старте.
        expired = [k for k, (_v, exp) in self._store.items() if exp is not None and now > exp]
        for k in expired:
            self._store.pop(k, None)
        if expired:
            logger.debug("Из сохраняемого кэша убрано протухших записей: %d", len(expired))

        # Снимок в отдельном потоке: json.dump 50k+ ключей блокировал бы
        # asyncio на секунды и ронял подписку WS.
        store_snapshot = dict(self._store)
        path = self._persist_path
        self._last_flush = now
        self._dirty = False

        def _write():
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                tmp = path.with_suffix(".tmp")
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(store_snapshot, f)
                tmp.replace(path)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Не удалось сохранить кэш на диск: %s", exc)

        if force:
            _write()
            return
        try:
            import threading
            threading.Thread(target=_write, name="cache-flush", daemon=True).start()
        except Exception:  # noqa: BLE001
            _write()

    async def get(self, key: str) -> Optional[str]:
        item = self._store.get(key)
        if item is None:
            return None
        value, expires_at = item
        if expires_at is not None and time.time() > expires_at:
            self._store.pop(key, None)
            return None
        return value

    async def set(self, key: str, value: str, ex: Optional[int] = None):
        expires_at = time.time() + ex if ex else None
        self._store[key] = (value, expires_at)
        self._dirty = True
        self._flush()

    async def sadd(self, key: str, *values: str):
        raw = await self.get(key)
        s = set(json.loads(raw)) if raw else set()
        s.update(values)
        await self.set(key, json.dumps(list(s)))

    async def smembers(self, key: str) -> set:
        raw = await self.get(key)
        return set(json.loads(raw)) if raw else set()

    async def expire(self, key: str, ttl_sec: int):
        item = self._store.get(key)
        if item:
            value, _ = item
            self._store[key] = (value, time.time() + ttl_sec)

    async def srem(self, key: str, *values: str):
        raw = await self.get(key)
        s = set(json.loads(raw)) if raw else set()
        s.difference_update(values)
        await self.set(key, json.dumps(list(s)))

    async def delete(self, *keys: str):
        for k in keys:
            self._store.pop(k, None)
        self._dirty = True
        self._flush()

    async def close(self):
        self._flush(force=True)


class RedisCache:
    """Асинхронная обёртка над Redis с фолбэком в память."""

    def __init__(self, url: str):
        self._url = url
        self._client = None

    async def connect(self):
        if aioredis is None:
            logger.warning("Пакет redis не установлен — используется in-memory кэш")
            self._client = _InMemoryFallback(persist_path=settings.DATA_DIR / "cache_state.json")
            return
        try:
            client = aioredis.from_url(self._url, decode_responses=True)
            await client.ping()
            self._client = client
            logger.info("Подключено к Redis: %s", self._url)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Redis недоступен (%s) — используется in-memory кэш", exc)
            self._client = _InMemoryFallback(persist_path=settings.DATA_DIR / "cache_state.json")

    async def close(self):
        if self._client:
            await self._client.close()

    # --- Профили кошельков ---
    async def get_wallet_profile(self, wallet: str) -> Optional[dict]:
        raw = await self._client.get(f"wallet:{wallet}")
        return json.loads(raw) if raw else None

    async def set_wallet_profile(self, wallet: str, profile: dict, ttl_sec: int = 6 * 3600):
        await self._client.set(f"wallet:{wallet}", json.dumps(profile), ex=ttl_sec)

    # --- Кластеры покупок (несколько китов купили один токен) ---
    async def register_token_buy(self, token_mint: str, wallet: str, ttl_sec: int = 1800) -> set:
        key = f"cluster:{token_mint}"
        await self._client.sadd(key, wallet)
        try:
            await self._client.expire(key, ttl_sec)
        except Exception:  # noqa: BLE001
            pass
        return set(await self._client.smembers(key))

    # --- Открытые позиции ---
    async def get_open_positions(self) -> dict[str, Any]:
        raw = await self._client.get("positions:open")
        return json.loads(raw) if raw else {}

    async def set_open_positions(self, positions: dict[str, Any]):
        await self._client.set("positions:open", json.dumps(positions))

    async def add_open_position(
        self,
        mint: str,
        whale: str,
        size_sol: float,
        entry_price_usd: float = 0.0,
        strategy_tag: str = "score",
    ):
        positions = await self.get_open_positions()
        positions[mint] = {
            "whale": whale,
            "size_sol": size_sol,        # исходный размер позиции (для справки/статистики)
            "remaining_sol": size_sol,   # сколько ещё "в позиции" — уменьшается частичными продажами
            "entry_price_usd": entry_price_usd,
            "highest_price_usd": entry_price_usd,
            "opened_at": time.time(),
            "partial_tp_1_done": False,
            "partial_tp_2_done": False,
            # score = честный путь; insider_exp = гипотеза funding-кластера
            "strategy_tag": strategy_tag or "score",
        }
        await self.set_open_positions(positions)

    async def update_position_high(self, mint: str, price_usd: float):
        positions = await self.get_open_positions()
        if mint in positions and price_usd > float(positions[mint].get("highest_price_usd") or 0):
            positions[mint]["highest_price_usd"] = price_usd
            await self.set_open_positions(positions)

    async def update_position_partial(self, mint: str, remaining_sol: float, level_key: str):
        """Отмечает частичную фиксацию прибыли: уменьшает remaining_sol и
        помечает уровень (level_key = "partial_tp_1_done" / "partial_tp_2_done")
        как уже взятый, чтобы не сработать повторно."""
        positions = await self.get_open_positions()
        if mint in positions:
            positions[mint]["remaining_sol"] = remaining_sol
            positions[mint][level_key] = True
            await self.set_open_positions(positions)

    async def remove_open_position(self, mint: str):
        positions = await self.get_open_positions()
        positions.pop(mint, None)
        await self.set_open_positions(positions)

    # --- Признак "новый токен" (для авто-обнаружения ранних покупателей) ---
    async def is_new_mint(self, mint: str, ttl_sec: int) -> bool:
        """True, если мы видим этот mint впервые (и запоминаем его на ttl_sec)."""
        key = f"mint_seen:{mint}"
        seen = await self._client.get(key)
        if seen:
            return False
        await self._client.set(key, "1", ex=ttl_sec)
        return True

    # --- Отложенная проверка исходов сделок (авто-репутация кошельков) ---
    async def add_pending_outcome(self, key: str, record: dict, ttl_sec: int = 7 * 24 * 3600):
        await self._client.set(f"outcome:{key}", json.dumps(record), ex=ttl_sec)
        await self._client.sadd("outcome:index", key)

    async def get_due_outcomes(self, now_ts: float) -> list:
        keys = await self._client.smembers("outcome:index")
        due = []
        stale = []
        for key in keys:
            raw = await self._client.get(f"outcome:{key}")
            if not raw:
                stale.append(key)
                continue
            record = json.loads(raw)
            if record.get("due_at", 0) <= now_ts:
                due.append((key, record))
        if stale:
            await self._client.srem("outcome:index", *stale)
        return due

    async def remove_pending_outcome(self, key: str):
        await self._client.delete(f"outcome:{key}")
        await self._client.srem("outcome:index", key)

    # --- Блок-лист скамерских/дев-кошельков (постоянный, между перезапусками) ---
    async def add_to_blocklist(self, wallet: str, reason: str):
        await self._client.set(f"blocklist:{wallet}", reason, ex=settings.BLOCKLIST_TTL_SEC)
        await self._client.sadd("blocklist:index", wallet)

    async def is_blocklisted(self, wallet: str) -> bool:
        return bool(await self._client.get(f"blocklist:{wallet}"))

    # --- Реестр адресов, замеченных как создатели токенов (дев-кошельки) ---
    # Используется, чтобы поймать кошельки, которые дев финансирует напрямую
    # для "органичных" ранних покупок собственного токена с других адресов.
    async def register_creator_wallet(self, wallet: str):
        await self._client.sadd("creator_wallets", wallet)

    async def is_known_creator_wallet(self, wallet: str) -> bool:
        return wallet in await self._client.smembers("creator_wallets")

    # --- Кэш результата скрининга кошелька (возраст, источник финансирования) ---
    async def get_wallet_screen(self, wallet: str) -> Optional[dict]:
        raw = await self._client.get(f"screen:{wallet}")
        return json.loads(raw) if raw else None

    async def set_wallet_screen(self, wallet: str, data: dict):
        await self._client.set(f"screen:{wallet}", json.dumps(data), ex=settings.WALLET_SCREEN_CACHE_TTL_SEC)

    # --- Счётчик разных новых токенов, купленных кошельком за сутки (антиспам) ---
    async def register_mint_for_wallet_today(self, wallet: str, mint: str) -> int:
        """Отмечает, что кошелёк купил этот mint сегодня, и возвращает
        количество РАЗНЫХ новых токенов, купленных им за сегодня — признак
        бота, скупающего всё подряд, а не селективного инсайдера."""
        day_key = time.strftime("%Y-%m-%d", time.gmtime())
        key = f"wallet_mints:{wallet}:{day_key}"
        await self._client.sadd(key, mint)
        try:
            await self._client.expire(key, 26 * 3600)
        except Exception:  # noqa: BLE001
            pass
        return len(await self._client.smembers(key))

    # --- Кэш результата rug-check для mint (mint/freeze authority, distribution) ---
    async def get_mint_screen(self, mint: str) -> Optional[dict]:
        raw = await self._client.get(f"mint_screen:{mint}")
        return json.loads(raw) if raw else None

    async def set_mint_screen(self, mint: str, data: dict, ttl_sec: int = 24 * 3600):
        await self._client.set(f"mint_screen:{mint}", json.dumps(data), ex=ttl_sec)

    # --- Базовая частота: сколько из ВСЕХ наблюдений оказались удачными ---
    # Нужна как точка отсчёта: кошелёк считается смарт-мани не по абсолютному
    # винрейту (на этом рынке высокого не бывает ни у кого), а по тому,
    # насколько он превосходит случайный выбор.
    async def add_baseline_outcome(self, is_win: bool):
        raw = await self._client.get("baseline:outcomes")
        data = json.loads(raw) if raw else {"wins": 0, "total": 0}
        data["total"] += 1
        if is_win:
            data["wins"] += 1
        await self._client.set("baseline:outcomes", json.dumps(data))

    async def get_baseline_rate(self) -> tuple:
        """Возвращает (доля удачных, число наблюдений)."""
        raw = await self._client.get("baseline:outcomes")
        if not raw:
            return 0.0, 0
        data = json.loads(raw)
        total = data.get("total", 0)
        return (data.get("wins", 0) / total if total else 0.0), total

    # --- Дневная статистика P&L ---
    async def get_daily_pnl(self, day_key: str) -> float:
        raw = await self._client.get(f"pnl:{day_key}")
        return float(raw) if raw else 0.0

    async def add_daily_pnl(self, day_key: str, delta_sol: float):
        current = await self.get_daily_pnl(day_key)
        await self._client.set(f"pnl:{day_key}", str(current + delta_sol), ex=3 * 24 * 3600)
