"""
Подписка на события Pump.fun вместо опроса раз в 15 секунд.

Зачем: измерение на 828 наблюдениях показало, что результат сильно
зависит от того, насколько рано мы вошли. Нижняя треть по ликвидности
на входе даёт -3.3% на сделку, верхняя -22%. Разница в 19 пунктов — это
не качество отбора, а опоздание: при опросе раз в 15 секунд мы узнаём о
покупке в среднем через 7.5 секунд, а с учётом разбора — через 8-20.
За это время ранняя фаза заканчивается без нас.

Подписка отдаёт событие примерно за секунду. Мы не догоним тех, кто
попадает в один блок с девом, но сместимся в ту зону кривой, где по
нашим же данным результат заметно лучше.

Устройство: сделка читается ПРЯМО ИЗ УВЕДОМЛЕНИЯ, без запросов к API.

Программа публикует данные сделки в логах строкой "Program data" —
там есть mint, объём, кошелёк, направление и виртуальные резервы кривой,
из которых сразу считается цена. Раскладка восстановлена по фактическим
данным (03.09.2026): смещения найдены по инварианту постоянного
произведения кривой, который сошёлся на 9 событиях из 10 и подтверждён
сверкой направления сделки с текстом логов.

Это снимает сразу три ограничения прежней схемы с разбором через API:
поток Pump.fun идёт со скоростью ~76 сделок в секунду, разбор пачками
по сотне отставал и копил очередь; тратилось до 6000 запросов в минуту;
и на каждую сделку шёл отдельный запрос цены. Теперь ноль запросов.
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import struct
import time
from collections.abc import AsyncIterator
from typing import Optional

import aiohttp

from config import settings
from core.tx_parser import BuyEvent, SellEvent, TxParser

logger = logging.getLogger("sniper.ws_scanner")

# Helius быстрее и стабильнее, но квота кончается. Публичные узлы —
# бесплатный запасной канал (тот же logsSubscribe на программу Pump.fun).
PUBLIC_WS_ENDPOINTS = (
    "wss://api.mainnet-beta.solana.com",
    "wss://solana-rpc.publicnode.com",
    "wss://rpc.ankr.com/solana/ws",
)

# Пометки в логах Pump.fun
BUY_MARKERS = ("Instruction: Buy",)          # ловит и Buy, и BuyV2
SELL_MARKERS = ("Instruction: Sell",)
CREATE_MARKER = "Instruction: Create"

# Дискриминатор события сделки и смещения полей — восстановлены по живым
# данным, а не взяты из документации: программа уже показала, что меняется
# (Buy -> BuyV2), а событие оказалось длиннее классического (358-373 байта).
TRADE_EVENT_DISC = bytes.fromhex("bddb7fd34ee661ee")
OFF_MINT = 8            # 32 байта
OFF_SOL = 40            # u64, лампорты
OFF_TOKENS = 48         # u64, атомарные единицы токена
OFF_IS_BUY = 56         # 1 байт
OFF_USER = 57           # 32 байта
OFF_RESERVES = 97       # два u64: виртуальные резервы SOL и токенов

LAMPORTS = 1_000_000_000
TOKEN_UNITS = 1_000_000
# Виртуальные резервы SOL стартуют с 30 и растут на величину реально
# внесённого SOL — отсюда ликвидность без отдельного запроса
CURVE_START_SOL = 30.0

# Инвариант бондинг-кривой Pump.fun: произведение виртуальных резервов
# постоянно на всём протяжении кривой. Значение измерено по 2908 живым
# событиям минта (медиана, разброс менее 2%).
#
# Зачем: в потоке идут запуски с ДРУГИМИ параметрами кривой. Для них
# "ликвидность = vSol - 30" и "градация на 85 SOL" просто неверны — именно
# отсюда взялись ликвидности в 1970 SOL и три ложных результата подряд.
# Токен, чьё произведение резервов не сходится с эталоном, к стандартной
# кривой отношения не имеет, и мерить его теми же порогами нельзя.
CURVE_K = 3.219e25
CURVE_K_TOLERANCE = 0.02

_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def _b58encode(raw: bytes) -> str:
    n = int.from_bytes(raw, "big")
    out = ""
    while n:
        n, r = divmod(n, 58)
        out = _B58[r] + out
    return "1" * (len(raw) - len(raw.lstrip(b"\x00"))) + out


def decode_trade_event(blob: bytes) -> Optional[dict]:
    """Разбирает событие сделки. None, если это событие другого типа."""
    if len(blob) < OFF_RESERVES + 16 or blob[:8] != TRADE_EVENT_DISC:
        return None
    try:
        mint = _b58encode(blob[OFF_MINT:OFF_MINT + 32])
        sol_raw, tokens_raw = struct.unpack_from("<QQ", blob, OFF_SOL)
        is_buy = bool(blob[OFF_IS_BUY])
        user = _b58encode(blob[OFF_USER:OFF_USER + 32])
        vsol_raw, vtok_raw = struct.unpack_from("<QQ", blob, OFF_RESERVES)
    except Exception:  # noqa: BLE001
        return None

    if not vtok_raw or not vsol_raw:
        return None
    virtual_sol = vsol_raw / LAMPORTS
    virtual_tokens = vtok_raw / TOKEN_UNITS
    curve_k = vsol_raw * vtok_raw
    standard_curve = abs(curve_k - CURVE_K) / CURVE_K < CURVE_K_TOLERANCE
    return {
        "curve_k": curve_k,
        "standard_curve": standard_curve,
        "mint": mint,
        "sol_amount": sol_raw / LAMPORTS,
        "token_amount": tokens_raw / TOKEN_UNITS,
        "is_buy": is_buy,
        "wallet": user,
        "price_sol": virtual_sol / virtual_tokens,
        "virtual_sol": virtual_sol,
        "liquidity_sol": max(virtual_sol - CURVE_START_SOL, 0.0),
    }


class WsScanner:
    def __init__(self, session: aiohttp.ClientSession, tx_parser: TxParser,
                 program_id: Optional[str] = None):
        self._session = session
        self._tx_parser = tx_parser
        self._program_id = program_id or settings.CHAIN_SCAN_PROGRAM_ID
        self._stats = {"notifications": 0, "matched": 0, "parsed": 0, "dropped": 0,
                       "grads": 0}
        self._queue: asyncio.Queue | None = None
        self._health = None
        self._connected_once = False
        self._endpoint_idx = 0
        # Градации пишем здесь: отдельный watch_graduations из Cursor-шелла
        # часто умирает на DNS после fork; у бота WS уже живой.
        self._grad_done: set[str] = set()
        self._grad_path = settings.DATA_DIR / "graduations_feed.jsonl"
        self._load_grad_done()

    def _load_grad_done(self) -> None:
        path = self._grad_path
        if not path.exists():
            return
        try:
            with open(path, encoding="utf-8") as fh:
                for line in fh:
                    try:
                        m = (json.loads(line) or {}).get("mint")
                    except Exception:  # noqa: BLE001
                        continue
                    if m:
                        self._grad_done.add(m)
            logger.info("WsScanner: known grads from feed n=%d", len(self._grad_done))
        except Exception:  # noqa: BLE001
            logger.debug("WsScanner: grad feed load failed", exc_info=True)

    def _note_graduation(self, trade: dict, signature: str) -> None:
        """Фиксируем пересечение ~85 SOL на стандартной кривой → лента когорты."""
        if not trade.get("standard_curve"):
            return
        liq = float(trade.get("liquidity_sol") or 0)
        if liq < 84.0:
            return
        mint = trade.get("mint") or ""
        if not mint or mint in self._grad_done:
            return
        self._grad_done.add(mint)
        self._stats["grads"] = self._stats.get("grads", 0) + 1
        rec = {
            "t_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "mint": mint,
            "liq_sol": round(liq, 4),
            "wallet": trade.get("wallet"),
            "is_buy": trade.get("is_buy"),
            "sol_amount": round(float(trade.get("sol_amount") or 0), 6),
            "price_sol": trade.get("price_sol"),
            "sig": signature,
            "source": "ws_scanner",
        }
        try:
            self._grad_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._grad_path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(rec) + "\n")
            logger.info(
                "GRADUATION #%d %s… liq=%.1f SOL (ws)",
                self._stats["grads"], mint[:12], liq,
            )
        except Exception:  # noqa: BLE001
            logger.debug("graduation write failed", exc_info=True)
            self._grad_done.discard(mint)

    def _ws_endpoints(self) -> list[str]:
        eps = []
        if settings.HELIUS_API_KEY:
            eps.append(settings.helius_ws())
        eps.extend(PUBLIC_WS_ENDPOINTS)
        return eps

    async def stream(self) -> AsyncIterator[object]:
        """Отдаёт события из очереди. Приём и выдача разделены, чтобы
        медленная обработка ниже по течению не тормозила чтение сокета."""
        endpoints = self._ws_endpoints()
        if not endpoints:
            logger.warning("Нет WS-эндпоинтов — подписка недоступна")
            return

        self._queue = asyncio.Queue(maxsize=5000)
        reader = asyncio.create_task(self._reader_loop(), name="ws_reader")
        try:
            while True:
                yield await self._queue.get()
        finally:
            reader.cancel()

    async def _reader_loop(self):
        """Держит подписку живой, переподключаясь при обрыве.

        При 429/ошибке Helius переключаемся на следующий публичный узел —
        иначе бот крутится в пустом цикле, пока квота не восстановится.
        """
        backoff = 1
        endpoints = self._ws_endpoints()
        while True:
            url = endpoints[self._endpoint_idx % len(endpoints)]
            label = "helius" if "helius" in url else url.split("//", 1)[-1].split("/", 1)[0]
            try:
                async with self._session.ws_connect(url, heartbeat=30) as ws:
                    await ws.send_json({
                        "jsonrpc": "2.0", "id": 1, "method": "logsSubscribe",
                        "params": [
                            {"mentions": [self._program_id]},
                            {"commitment": "confirmed"},
                        ],
                    })
                    logger.info(
                        "Подписка на %s через %s (задержка ~1 сек вместо опроса раз в %ds)",
                        self._program_id, label, settings.CHAIN_SCAN_POLL_INTERVAL_SEC,
                    )
                    backoff = 1
                    if self._connected_once and self._health is not None:
                        self._health.note_ws_reconnect()
                    self._connected_once = True
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            self._handle_notification(msg.data)
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
                logger.warning("Подписка (%s) закрыта, переподключение через %ds", label, backoff)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                err = str(exc)
                is_quota = "429" in err or "max usage" in err.lower()
                if is_quota and self._health is not None:
                    self._health.note_helius_429()
                # квота/отказ текущего узла → сразу следующий
                self._endpoint_idx += 1
                next_label = endpoints[self._endpoint_idx % len(endpoints)]
                next_short = "helius" if "helius" in next_label else next_label.split("//", 1)[-1].split("/", 1)[0]
                wait = 1 if is_quota else backoff
                logger.warning(
                    "Ошибка подписки на %s (%s: %s) — следующий узел %s через %ds",
                    label, type(exc).__name__, err[:160], next_short, wait,
                )
                await asyncio.sleep(wait)
                if not is_quota:
                    backoff = min(backoff * 2, 30)
                continue
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)
            # после нормального обрыва пробуем тот же узел ещё раз;
            # если снова упадёт — цикл выше сдвинет индекс
            self._endpoint_idx += 1

    def _handle_notification(self, raw: str):
        try:
            data = json.loads(raw)
        except Exception:  # noqa: BLE001
            return
        value = ((data.get("params") or {}).get("result") or {}).get("value") or {}
        signature = value.get("signature")
        if not signature or value.get("err"):
            return

        self._stats["notifications"] += 1
        if getattr(self, "_health", None) is not None:
            self._health.note_ws_event()
        logs = value.get("logs") or []

        # Покупка дева своего же токена: в той же транзакции есть создание.
        # Раньше это определялось разбором инструкций через API — из логов
        # видно так же надёжно и бесплатно.
        is_creator_buy = any(CREATE_MARKER in l for l in logs)

        for line in logs:
            if not line.startswith("Program data:"):
                continue
            try:
                blob = base64.b64decode(line.split("Program data:", 1)[1].strip())
            except Exception:  # noqa: BLE001
                continue
            trade = decode_trade_event(blob)
            if not trade:
                continue

            self._stats["matched"] += 1
            self._note_graduation(trade, signature)
            if trade["is_buy"]:
                if trade["sol_amount"] < settings.MIN_BUY_SOL_TO_CONSIDER:
                    continue
                event = BuyEvent(
                    signature=signature, wallet=trade["wallet"], token_mint=trade["mint"],
                    sol_amount=trade["sol_amount"], source="PUMP_FUN",
                    timestamp=int(time.time()), is_creator_buy=is_creator_buy,
                )
            else:
                event = SellEvent(
                    signature=signature, wallet=trade["wallet"], token_mint=trade["mint"],
                    sol_amount=trade["sol_amount"], source="PUMP_FUN",
                    timestamp=int(time.time()),
                )
            # цена и ликвидность приехали вместе с событием — отдельный
            # запрос к кривой на этапе отбора больше не нужен
            event.price_sol = trade["price_sol"]
            event.liquidity_sol = trade["liquidity_sol"]

            try:
                self._queue.put_nowait(event)
                self._stats["parsed"] += 1
                if getattr(self, "_health", None) is not None:
                    self._health.note_ws_parsed()
            except asyncio.QueueFull:
                self._stats["dropped"] = self._stats.get("dropped", 0) + 1

        self._stats["parsed"] += 1
        if self._stats["parsed"] % 2000 == 0:
            logger.info("Поток: уведомлений %d, сделок %d, в очереди %d, отброшено %d",
                        self._stats["notifications"], self._stats["matched"],
                        self._queue.qsize(), self._stats.get("dropped", 0))
