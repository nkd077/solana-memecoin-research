"""
Исполнительный модуль: получает котировку в Jupiter, собирает и подписывает
транзакцию свопа, отправляет через Jito Block Engine для приоритизации.

В режиме DRY_RUN (по умолчанию) или без загруженного приватного ключа
транзакции НЕ подписываются и НЕ отправляются — только логируется, что
было бы сделано.
"""
from __future__ import annotations

import base64
import logging
from dataclasses import dataclass
from typing import Optional

import aiohttp

from config import settings

logger = logging.getLogger("sniper.jito_executor")

LAMPORTS_PER_SOL = 1_000_000_000


@dataclass
class ExecutionResult:
    success: bool
    signature: Optional[str] = None
    dry_run: bool = False
    error: Optional[str] = None


class JitoExecutor:
    def __init__(self, session: aiohttp.ClientSession):
        self._session = session
        self._keypair = self._load_keypair()

    def _load_keypair(self):
        if not settings.PRIVATE_KEY:
            logger.warning("PRIVATE_KEY не задан — исполнение реальных сделок недоступно (только DRY_RUN)")
            return None
        try:
            import base58
            from solders.keypair import Keypair

            secret = base58.b58decode(settings.PRIVATE_KEY)
            return Keypair.from_bytes(secret)
        except Exception as exc:  # noqa: BLE001
            logger.error("Не удалось загрузить приватный ключ: %s", exc)
            return None

    async def get_quote(self, input_mint: str, output_mint: str, amount_lamports: int) -> Optional[dict]:
        params = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": amount_lamports,
            "slippageBps": settings.SLIPPAGE_BPS,
        }
        try:
            async with self._session.get(settings.JUPITER_QUOTE_URL, params=params, timeout=10) as resp:
                if resp.status != 200:
                    logger.warning("Jupiter quote вернул %s", resp.status)
                    return None
                return await resp.json()
        except Exception as exc:  # noqa: BLE001
            logger.error("Ошибка запроса котировки Jupiter: %s", exc)
            return None

    async def _build_swap_tx(self, quote: dict) -> Optional[str]:
        if not self._keypair:
            return None
        body = {
            "quoteResponse": quote,
            "userPublicKey": str(self._keypair.pubkey()),
            "wrapAndUnwrapSol": True,
            "prioritizationFeeLamports": "auto",
        }
        try:
            async with self._session.post(settings.JUPITER_SWAP_URL, json=body, timeout=10) as resp:
                if resp.status != 200:
                    logger.warning("Jupiter swap вернул %s", resp.status)
                    return None
                payload = await resp.json()
                return payload.get("swapTransaction")
        except Exception as exc:  # noqa: BLE001
            logger.error("Ошибка сборки транзакции свопа: %s", exc)
            return None

    def _sign_transaction(self, swap_tx_b64: str):
        from solders.transaction import VersionedTransaction

        raw = base64.b64decode(swap_tx_b64)
        tx = VersionedTransaction.from_bytes(raw)
        signed = VersionedTransaction(tx.message, [self._keypair])
        return signed

    async def _send_via_jito(self, signed_tx_b64: str) -> Optional[str]:
        body = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendBundle",
            "params": [[signed_tx_b64]],
        }
        try:
            async with self._session.post(settings.JITO_BLOCK_ENGINE_URL, json=body, timeout=10) as resp:
                payload = await resp.json()
                return payload.get("result")
        except Exception as exc:  # noqa: BLE001
            logger.error("Ошибка отправки бандла в Jito: %s", exc)
            return None

    async def execute_buy(self, token_mint: str, amount_sol: float) -> ExecutionResult:
        # Проверка DRY_RUN идёт ПЕРВОЙ и намеренно.
        #
        # Раньше сначала запрашивалась котировка Jupiter, и бумажная сделка
        # не записывалась, если котировка не пришла. А она и не приходит для
        # токенов на бондинг-кривой: Jupiter маршрутизирует только пулы DEX,
        # токены до выпуска ему не видны в принципе (см. их документацию про
        # Metis routing). Из-за этого ночной прогон дал 91 попытку покупки и
        # ноль записей в статистику.
        #
        # Для симуляции котировка не нужна: цену мы уже знаем с самой кривой,
        # и она для такого токена точнее, чем что-либо от агрегатора.
        if settings.DRY_RUN or not self._keypair:
            logger.info("[DRY_RUN] Покупка на %.4f SOL токена %s (симуляция, реального исполнения нет)",
                        amount_sol, token_mint)
            return ExecutionResult(success=True, dry_run=True)

        amount_lamports = int(amount_sol * LAMPORTS_PER_SOL)
        quote = await self.get_quote(settings.WSOL_MINT, token_mint, amount_lamports)
        if not quote:
            return ExecutionResult(success=False, error="Не удалось получить котировку Jupiter")

        swap_tx_b64 = await self._build_swap_tx(quote)
        if not swap_tx_b64:
            return ExecutionResult(success=False, error="Не удалось собрать транзакцию свопа")

        try:
            signed = self._sign_transaction(swap_tx_b64)
            signed_b64 = base64.b64encode(bytes(signed)).decode()
        except Exception as exc:  # noqa: BLE001
            return ExecutionResult(success=False, error=f"Ошибка подписи: {exc}")

        signature = await self._send_via_jito(signed_b64)
        if not signature:
            return ExecutionResult(success=False, error="Jito не вернул подпись бандла")

        logger.info("Отправлена сделка: %s SOL -> %s (bundle: %s)", amount_sol, token_mint, signature)
        return ExecutionResult(success=True, signature=signature)

    async def _get_token_account_amount(self, mint: str) -> Optional[dict]:
        """Возвращает {"amount": "<целое в атомарных единицах>", "decimals": N}
        для баланса указанного mint на нашем кошельке, либо None."""
        if not self._keypair:
            return None
        body = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTokenAccountsByOwner",
            "params": [
                str(self._keypair.pubkey()),
                {"mint": mint},
                {"encoding": "jsonParsed"},
            ],
        }
        try:
            async with self._session.post(settings.helius_rpc(), json=body, timeout=10) as resp:
                if resp.status != 200:
                    logger.warning("Helius getTokenAccountsByOwner вернул %s", resp.status)
                    return None
                payload = await resp.json()
        except Exception as exc:  # noqa: BLE001
            logger.error("Ошибка запроса баланса токена %s: %s", mint, exc)
            return None

        accounts = (payload.get("result") or {}).get("value") or []
        if not accounts:
            return None
        return accounts[0]["account"]["data"]["parsed"]["info"]["tokenAmount"]

    async def execute_sell(self, token_mint: str, fraction: float = 1.0) -> ExecutionResult:
        """Продаёт часть (fraction, 0..1) текущего баланса token_mint обратно в
        SOL через Jupiter+Jito. fraction=1.0 — продать всё. Баланс каждый раз
        запрашивается заново с кошелька (а не берётся из наших внутренних
        оценок), чтобы частичные продажи не накапливали ошибку округления.
        В DRY_RUN (или без загруженного ключа) реальную транзакцию не
        отправляет — вызывающий код (core/position_monitor.py) сам считает
        "бумажный" P&L."""
        if settings.DRY_RUN or not self._keypair:
            logger.info("[DRY_RUN] Продажа %.0f%% от %s пропущена (реального исполнения не было)",
                        fraction * 100, token_mint)
            return ExecutionResult(success=True, dry_run=True)

        token_amount = await self._get_token_account_amount(token_mint)
        if not token_amount or int(token_amount.get("amount", 0)) <= 0:
            return ExecutionResult(success=False, error="На кошельке нет баланса этого токена для продажи")

        raw_amount = int(int(token_amount["amount"]) * max(min(fraction, 1.0), 0.0))
        if raw_amount <= 0:
            return ExecutionResult(success=False, error="Рассчитанный объём продажи равен нулю")

        quote = await self.get_quote(token_mint, settings.WSOL_MINT, raw_amount)
        if not quote:
            return ExecutionResult(success=False, error="Не удалось получить котировку на продажу")

        swap_tx_b64 = await self._build_swap_tx(quote)
        if not swap_tx_b64:
            return ExecutionResult(success=False, error="Не удалось собрать транзакцию продажи")

        try:
            signed = self._sign_transaction(swap_tx_b64)
            signed_b64 = base64.b64encode(bytes(signed)).decode()
        except Exception as exc:  # noqa: BLE001
            return ExecutionResult(success=False, error=f"Ошибка подписи: {exc}")

        signature = await self._send_via_jito(signed_b64)
        if not signature:
            return ExecutionResult(success=False, error="Jito не вернул подпись бандла")

        logger.info("Отправлена продажа: %s -> SOL (bundle: %s)", token_mint, signature)
        return ExecutionResult(success=True, signature=signature)
