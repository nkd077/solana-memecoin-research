"""
Парсер транзакций Solana: определяет покупки И продажи токенов через
Raydium AMM v4 и Pump.fun по подписи транзакции, используя Helius
Enhanced Transactions API.

Продажи нужны, чтобы бот мог "зеркалить" выход кита из позиции (см.
core/position_monitor.py, main.py::_handle_sell_event) — не только
копировать вход, но и реагировать, когда кит, за которым мы зашли,
сам продаёт токен.

Примечание: схема ответа Helius периодически меняется — перед боевым
использованием сверьте поля events.swap.* с актуальной документацией
https://docs.helius.dev/.

Также здесь же живёт детектор "покупка = сам дев токена": Pump.fun почти
всегда бандлит инструкцию создания токена (`create`) и первую покупку дева
в одной транзакции. Мы проверяем сырые инструкции транзакции на Anchor-
дискриминатор `create` программы Pump.fun (первые 8 байт data после
base58-декодирования, дискриминатор = sha256("global:create")[:8]) — если
он есть в той же транзакции и подписант совпадает с покупателем, это не
инсайдерский сигнал, а сам дев покупает свой токен. Такие кошельки боту
доверять нельзя (по ним нет статистики винрейта — только эта покупка), и
именно они чаще всего сливают токен на держателей. base58 декодируется
вручную (без внешней зависимости) чтобы не зависеть от наличия пакета
base58 в окружении.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import aiohttp

from config import settings

logger = logging.getLogger("sniper.tx_parser")

HELIUS_PARSE_URL = "https://api.helius.xyz/v0/transactions"

# Источники, которые нас интересуют
WATCHED_SOURCES = {"RAYDIUM", "PUMP_FUN", "PUMP_AMM"}

# Котировочные монеты: их НИКОГДА нельзя принимать за "снайпленный" токен.
# Именно из-за отсутствия этой проверки бот раньше принимал маршрутизацию
# свопов через USDC за раннюю покупку мемкоина и скорил стейблкоин.
WSOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
QUOTE_MINTS = {WSOL_MINT, USDC_MINT, USDT_MINT}

LAMPORTS_PER_SOL = 1_000_000_000

# Anchor global-инструкция "create" программы Pump.fun: первые 8 байт
# sha256("global:create"). Используется, чтобы отличить первую покупку
# дева (в одной транзакции с созданием токена) от обычной ранней покупки.
PUMP_FUN_CREATE_DISCRIMINATOR = bytes.fromhex("181ec828051c0777")

_B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def _b58decode(s: str) -> bytes:
    """Минимальный base58-декодер (алфавит Bitcoin/Solana) без внешних
    зависимостей — нужен только чтобы прочитать первые байты instruction
    data и сравнить с дискриминатором, полноценная библиотека не требуется."""
    n = 0
    for ch in s:
        idx = _B58_ALPHABET.find(ch)
        if idx < 0:
            raise ValueError(f"Недопустимый base58-символ: {ch!r}")
        n = n * 58 + idx
    full_bytes = n.to_bytes((n.bit_length() + 7) // 8, "big") if n > 0 else b""
    n_leading_zeros = len(s) - len(s.lstrip("1"))
    return b"\x00" * n_leading_zeros + full_bytes


@dataclass
class BuyEvent:
    signature: str
    wallet: str
    token_mint: str
    sol_amount: float
    source: str
    slot: Optional[int] = None
    timestamp: Optional[int] = None
    # True, если в этой же транзакции есть инструкция создания токена
    # Pump.fun с тем же подписантом — это покупка дева своего токена, а не
    # инсайдерский сигнал (см. _is_creator_buy)
    is_creator_buy: bool = False
    # Заполняются, когда событие пришло из подписки: там цена и ликвидность
    # приезжают вместе со сделкой, и отдельный запрос к кривой не нужен.
    price_sol: Optional[float] = None
    liquidity_sol: Optional[float] = None


@dataclass
class SellEvent:
    signature: str
    wallet: str
    token_mint: str
    sol_amount: float  # сколько SOL получено от продажи
    source: str
    slot: Optional[int] = None
    timestamp: Optional[int] = None
    # Заполняются, когда событие пришло из подписки: там цена и ликвидность
    # приезжают вместе со сделкой, и отдельный запрос к кривой не нужен.
    price_sol: Optional[float] = None
    liquidity_sol: Optional[float] = None


class TxParser:
    def __init__(self, session: aiohttp.ClientSession):
        self._session = session

    async def parse_signatures(self, signatures: list[str]) -> list[BuyEvent]:
        """Только покупки — обёртка над parse_trades для обратной совместимости
        (используется там, где продажи не нужны, например GMGN-путь в main.py)."""
        buys, _ = await self.parse_trades(signatures)
        return buys

    async def parse_trades(self, signatures: list[str]) -> tuple[list[BuyEvent], list[SellEvent]]:
        """Забирает и разбирает пачку транзакций по их подписям за один запрос
        к Helius, возвращает (покупки, продажи)."""
        if not signatures:
            return [], []
        if not settings.HELIUS_API_KEY:
            logger.debug("HELIUS_API_KEY не задан — парсинг транзакций пропущен")
            return [], []

        url = f"{HELIUS_PARSE_URL}?api-key={settings.HELIUS_API_KEY}"
        try:
            async with self._session.post(url, json={"transactions": signatures}, timeout=15) as resp:
                if resp.status != 200:
                    logger.warning("Helius parse вернул %s: %s", resp.status, await resp.text())
                    return [], []
                payload = await resp.json()
        except Exception as exc:  # noqa: BLE001
            logger.error("Ошибка запроса к Helius parse API: %s: %s", type(exc).__name__, exc)
            return [], []

        buys: list[BuyEvent] = []
        sells: list[SellEvent] = []
        for tx in payload:
            buy = self._extract_buy_event(tx)
            if buy:
                buys.append(buy)
                continue
            sell = self._extract_sell_event(tx)
            if sell:
                sells.append(sell)
        return buys, sells

    @staticmethod
    def _watched_source(tx: dict) -> Optional[str]:
        source = (tx.get("source") or "").upper()
        if source not in WATCHED_SOURCES and tx.get("type") != "SWAP":
            return None
        return source or "SWAP"

    # ------------------------------------------------------------------
    # Восстановление сделки из переводов.
    #
    # ВАЖНО (найдено на живых данных 2026-09-02): у большинства обычных
    # транзакций Pump.fun поле events у Helius ПУСТОЕ ({}), даже когда
    # type=SWAP. Enhanced-обогащение events.swap приходит в основном для
    # сложных маршрутизированных свопов (Jupiter и т.п.), где USDC часто
    # выступает промежуточным звеном. Старый парсер читал только
    # events.swap — из-за этого пропускал почти все реальные покупки на
    # бондинг-кривой и вместо них скорил маршрутизацию через USDC.
    #
    # Поэтому основной источник истины теперь — tokenTransfers и
    # nativeTransfers, которые присутствуют всегда, а events.swap
    # используется только как необязательное уточнение.
    # ------------------------------------------------------------------

    @staticmethod
    def _sol_leg(native_transfers: list, wallet: str, counterparty: Optional[str], outgoing: bool) -> float:
        """Сколько SOL кошелёк отдал (outgoing=True) или получил (False).

        Сначала пытаемся найти перевод именно с тем контрагентом, который
        участвовал в переводе токена — это точная нога свопа. Если такого
        нет (пул принимает SOL на другой адрес), берём максимальный перевод
        в нужную сторону: комиссии и Jito-чаевые всегда кратно меньше
        самой сделки, поэтому максимум — это она."""
        key_from, key_to = ("fromUserAccount", "toUserAccount") if outgoing else ("toUserAccount", "fromUserAccount")

        candidates = [n for n in native_transfers if n.get(key_from) == wallet]
        if not candidates:
            return 0.0

        if counterparty:
            exact = [n for n in candidates if n.get(key_to) == counterparty]
            if exact:
                return sum(int(n.get("amount", 0)) for n in exact) / LAMPORTS_PER_SOL

        return max(int(n.get("amount", 0)) for n in candidates) / LAMPORTS_PER_SOL

    @staticmethod
    def _wsol_leg(token_transfers: list, wallet: str, outgoing: bool) -> float:
        """Некоторые свопы двигают не нативный SOL, а обёрнутый WSOL —
        учитываем и такой вариант, иначе сделка будет выглядеть как
        покупка за 0 SOL."""
        key = "fromUserAccount" if outgoing else "toUserAccount"
        total = 0.0
        for t in token_transfers:
            if t.get("mint") == WSOL_MINT and t.get(key) == wallet:
                try:
                    total += float(t.get("tokenAmount") or 0)
                except (TypeError, ValueError):
                    continue
        return total

    def _extract_buy_event(self, tx: dict) -> Optional[BuyEvent]:
        source = self._watched_source(tx)
        if source is None:
            return None
        if tx.get("transactionError"):
            return None

        wallet = tx.get("feePayer")
        if not wallet:
            return None

        token_transfers = tx.get("tokenTransfers") or []
        native_transfers = tx.get("nativeTransfers") or []

        # Покупка = кошелёк ПОЛУЧИЛ токен (не котировочный) и ОТДАЛ SOL
        incoming = [
            t for t in token_transfers
            if t.get("toUserAccount") == wallet
            and t.get("mint")
            and t.get("mint") not in QUOTE_MINTS
        ]
        if not incoming:
            return None

        # Если токенов пришло несколько (мультихоп), берём самый крупный по
        # количеству — промежуточные ноги маршрута обычно мельче целевой.
        def _amount(t):
            try:
                return float(t.get("tokenAmount") or 0)
            except (TypeError, ValueError):
                return 0.0

        target = max(incoming, key=_amount)
        token_mint = target.get("mint")
        counterparty = target.get("fromUserAccount")

        sol_amount = self._sol_leg(native_transfers, wallet, counterparty, outgoing=True)
        if sol_amount <= 0:
            sol_amount = self._wsol_leg(token_transfers, wallet, outgoing=True)
        if sol_amount <= 0:
            return None

        return BuyEvent(
            signature=tx.get("signature", ""),
            wallet=wallet,
            token_mint=token_mint,
            sol_amount=sol_amount,
            source=source,
            slot=tx.get("slot"),
            timestamp=tx.get("timestamp"),
            is_creator_buy=self._is_creator_buy(tx, wallet),
        )

    @staticmethod
    def _is_creator_buy(tx: dict, wallet: str) -> bool:
        """True, если среди сырых инструкций этой транзакции есть Pump.fun
        `create` (создание токена) с тем же подписантом, что и покупатель —
        то есть это дев покупает свой только что созданный токен, а не
        независимый ранний покупатель. Устроено защитно: любая ошибка
        разбора (неожиданный формат данных Helius, отсутствие поля) просто
        возвращает False — мы не хотим по ошибке блокировать нормальные
        сделки из-за сбоя в этой эвристике."""
        try:
            instructions = tx.get("instructions") or []
            for ix in instructions:
                if ix.get("programId") != settings.PUMP_FUN_PROGRAM_ID:
                    continue
                data_b58 = ix.get("data")
                if not data_b58:
                    continue
                raw = _b58decode(data_b58)
                if raw[:8] != PUMP_FUN_CREATE_DISCRIMINATOR:
                    continue
                accounts = ix.get("accounts") or []
                # В инструкции create подписант (создатель) — один из
                # аккаунтов инструкции; проверяем и его, и feePayer/wallet,
                # чтобы не полагаться на конкретную позицию в списке.
                if wallet in accounts or wallet == tx.get("feePayer"):
                    return True
        except Exception:  # noqa: BLE001
            return False
        return False

    def _extract_sell_event(self, tx: dict) -> Optional[SellEvent]:
        """Зеркало _extract_buy_event: кошелёк ОТДАЛ токен и ПОЛУЧИЛ SOL."""
        source = self._watched_source(tx)
        if source is None:
            return None
        if tx.get("transactionError"):
            return None

        wallet = tx.get("feePayer")
        if not wallet:
            return None

        token_transfers = tx.get("tokenTransfers") or []
        native_transfers = tx.get("nativeTransfers") or []

        outgoing = [
            t for t in token_transfers
            if t.get("fromUserAccount") == wallet
            and t.get("mint")
            and t.get("mint") not in QUOTE_MINTS
        ]
        if not outgoing:
            return None

        def _amount(t):
            try:
                return float(t.get("tokenAmount") or 0)
            except (TypeError, ValueError):
                return 0.0

        target = max(outgoing, key=_amount)
        token_mint = target.get("mint")
        counterparty = target.get("toUserAccount")

        sol_amount = self._sol_leg(native_transfers, wallet, counterparty, outgoing=False)
        if sol_amount <= 0:
            sol_amount = self._wsol_leg(token_transfers, wallet, outgoing=False)
        if sol_amount <= 0:
            return None

        return SellEvent(
            signature=tx.get("signature", ""),
            wallet=wallet,
            token_mint=token_mint,
            sol_amount=sol_amount,
            source=source,
            slot=tx.get("slot"),
            timestamp=tx.get("timestamp"),
        )
