"""
Прямое сканирование блокчейна: вместо того чтобы полагаться на GMGN или
Arkham (платные/нестабильные источники "кто здесь кит"), бот сам читает
сделки на Pump.fun через Helius и находит ранних покупателей новых
токенов. Их дальнейшая судьба (выстрелил токен или нет) отслеживается в
WalletProfiler — так бот постепенно строит собственный список
"проверенных" инсайдеров, без ручного списка кошельков.

Это основной источник сигналов бота. Отдаёт не только покупки, но и
продажи — по продажам main.py определяет, что кит, за которым мы зашли,
сам вышел из позиции, и зеркалит выход (см. main.py::_handle_sell_event).

GMGN (core/gmgn_client.py) остаётся опциональным дополнительным
источником, если вы всё же захотите вручную подмешать в
GMGN_WATCH_WALLETS уже известные адреса.
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Optional

import aiohttp

from config import settings
from core.tx_parser import BuyEvent, SellEvent, TxParser

logger = logging.getLogger("sniper.chain_scanner")


class ChainScanner:
    def __init__(self, session: aiohttp.ClientSession, tx_parser: TxParser, program_id: Optional[str] = None):
        self._session = session
        self._tx_parser = tx_parser
        self._program_id = program_id or settings.CHAIN_SCAN_PROGRAM_ID
        self._last_signature: Optional[str] = None

    async def _fetch_new_signatures(self) -> list[str]:
        if not settings.HELIUS_API_KEY:
            return []

        opts = {"limit": 100}
        if self._last_signature:
            opts["until"] = self._last_signature

        body = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": [self._program_id, opts],
        }
        try:
            async with self._session.post(settings.helius_rpc(), json=body, timeout=15) as resp:
                if resp.status != 200:
                    logger.warning("Helius getSignaturesForAddress вернул %s", resp.status)
                    return []
                payload = await resp.json()
        except Exception as exc:  # noqa: BLE001
            logger.error("Ошибка запроса сигнатур к Helius: %s: %s", type(exc).__name__, exc)
            return []

        if payload.get("error"):
            logger.warning("Helius RPC error: %s", payload["error"])
            return []

        result = payload.get("result") or []
        sigs = [item["signature"] for item in result if not item.get("err")]
        if sigs:
            self._last_signature = sigs[0]  # newest сигнатура идёт первой
        return list(reversed(sigs))  # от старых к новым, для естественного порядка обработки

    async def stream(self) -> AsyncIterator[object]:
        """Периодически опрашивает Helius и отдаёт новые покупки (BuyEvent) и
        продажи (SellEvent) по программе (по умолчанию Pump.fun) — вызывающий
        код различает их через isinstance(). Известное ограничение: при очень
        высокой частоте сделок и большом CHAIN_SCAN_POLL_INTERVAL_SEC часть
        сигнатур между опросами может быть пропущена (RPC отдаёт максимум
        `limit` штук за раз) — уменьшите интервал, если это критично."""
        if not settings.HELIUS_API_KEY:
            logger.warning("HELIUS_API_KEY не задан — прямое сканирование блокчейна отключено")
            return

        logger.info(
            "Сканирование программы %s каждые %ss (мин. размер покупки для рассмотрения: %s SOL)",
            self._program_id, settings.CHAIN_SCAN_POLL_INTERVAL_SEC, settings.MIN_BUY_SOL_TO_CONSIDER,
        )
        while True:
            try:
                signatures = await self._fetch_new_signatures()
                logger.debug("Опрос: получено %d новых сигнатур от Helius", len(signatures))
                if signatures:
                    buys, sells = await self._tx_parser.parse_trades(signatures)
                    logger.debug("Разобрано: %d покупок, %d продаж (из %d сигнатур)",
                                 len(buys), len(sells), len(signatures))
                    qualifying_buys = 0
                    for event in buys:
                        if event.sol_amount >= settings.MIN_BUY_SOL_TO_CONSIDER:
                            qualifying_buys += 1
                            yield event
                    if buys:
                        logger.debug("Покупок прошло порог MIN_BUY_SOL_TO_CONSIDER (%s SOL): %d из %d",
                                     settings.MIN_BUY_SOL_TO_CONSIDER, qualifying_buys, len(buys))
                    # Продажи не фильтруем по объёму — нам важен сам факт, что
                    # конкретный кит продал, а не размер его продажи.
                    for event in sells:
                        yield event
            except Exception:  # noqa: BLE001
                logger.exception("Ошибка в цикле сканирования блокчейна")
            await asyncio.sleep(settings.CHAIN_SCAN_POLL_INTERVAL_SEC)
