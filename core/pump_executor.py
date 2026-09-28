"""
Исполнение сделок на Pump.fun через PumpPortal с локальной подписью.

Почему так, а не сборкой инструкций вручную: программа Pump.fun меняется.
На живых данных 03.09.2026 инструкция buy содержала 18 аккаунтов вместо
12 в старых версиях — добавились аккумуляторы объёма и новый контур
комиссий. Ручная сборка сломалась бы при следующем обновлении молча,
посреди торговли. PumpPortal отдаёт готовую транзакцию, а `pool="auto"`
сам выбирает бондинг-кривую для невыпущенных токенов и AMM для
выпущенных — чего Jupiter в принципе не умеет (его Metis не
маршрутизирует токены на кривой).

Приватный ключ никуда не уходит: сервис получает только публичный адрес
и параметры сделки, транзакцию подписываем сами.

БЕЗОПАСНОСТЬ. Подписывать вслепую то, что прислал сервер, нельзя —
подпись авторизует любое содержимое. Поэтому перед подписью транзакция
разбирается и проверяется (см. verify_transaction):
  1. плательщик комиссии — наш кошелёк, а не чужой;
  2. вызываются только программы из белого списка;
  3. в инструкции buy аргумент max_sol_cost не больше задуманного —
     это тот самый предел, дороже которого сделка не исполнится
     на уровне самой программы;
  4. симуляция в RPC проходит успешно (ловит и порчу, и заведомо
     нерабочую транзакцию до траты комиссии).
Не сошлось хоть что-то — подпись не ставится.
"""
from __future__ import annotations

import base64
import logging
import struct
from dataclasses import dataclass
from typing import Optional

import aiohttp

from config import settings

logger = logging.getLogger("sniper.pump_executor")

_B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def _b58encode(raw: bytes) -> str:
    """base58 без внешней зависимости — нужен только для адресов из таблиц."""
    n = int.from_bytes(raw, "big")
    out = ""
    while n > 0:
        n, rem = divmod(n, 58)
        out = _B58_ALPHABET[rem] + out
    pad = len(raw) - len(raw.lstrip(b"\x00"))
    return "1" * pad + out

PUMPPORTAL_TRADE_URL = "https://pumpportal.fun/api/trade-local"
LAMPORTS_PER_SOL = 1_000_000_000

BUY_DISCRIMINATOR = bytes.fromhex("66063d1201daebea")
SELL_DISCRIMINATOR = bytes.fromhex("33e685a4017f83ad")

# Программы, которым позволено участвовать в нашей сделке. Всё остальное —
# повод отказаться от подписи: транзакция делает не то, что мы просили.
ALLOWED_PROGRAMS = {
    settings.PUMP_FUN_PROGRAM_ID,                      # сама Pump.fun
    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",     # PumpSwap (AMM после выпуска)
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",    # Raydium AMM v4
    "11111111111111111111111111111111",                # System
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",     # SPL Token
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",     # Token-2022
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",    # Associated Token
    "ComputeBudget111111111111111111111111111111",     # ComputeBudget
    "pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ",     # контур комиссий Pump.fun
    # Роутер PumpPortal: оборачивает покупку Pump.fun и берёт свою комиссию
    # в той же транзакции. Опознан по структуре 03.09.2026 — принимает те же
    # аккаунты, что и buy у Pump.fun (начиная с её global PDA), плюс один,
    # и почти тот же дискриминатор (66323d... против 66063d...).
    "FAdo9NCw1ssek6Z6yeWzWjhLVsr8uiCwcWNUnKgzTnHe",
}

# Дискриминатор покупки у роутера PumpPortal — отличается от Pump.fun одним
# байтом, но раскладка аргументов та же: (amount u64, max_sol_cost u64).
ROUTER_BUY_DISCRIMINATOR = bytes.fromhex("66323d1201daebea")
ROUTER_PROGRAM = "FAdo9NCw1ssek6Z6yeWzWjhLVsr8uiCwcWNUnKgzTnHe"


@dataclass
class VerifyResult:
    ok: bool
    reason: str = ""
    max_sol_cost: Optional[float] = None


class PumpExecutor:
    def __init__(self, session: aiohttp.ClientSession, keypair=None):
        self._session = session
        self._keypair = keypair

    @property
    def pubkey(self) -> Optional[str]:
        return str(self._keypair.pubkey()) if self._keypair else None

    # ---------- сборка ----------

    async def build_trade_tx(self, action: str, mint: str, amount, denominated_in_sol: bool,
                             slippage_pct: float, priority_fee_sol: float) -> Optional[bytes]:
        if not self._keypair:
            logger.warning("Нет ключа — транзакцию собирать не для кого")
            return None

        payload = {
            "publicKey": self.pubkey,
            "action": action,
            "mint": mint,
            "amount": amount,
            "denominatedInSol": "true" if denominated_in_sol else "false",
            "slippage": slippage_pct,
            "priorityFee": priority_fee_sol,
            "pool": "auto",   # сам выберет кривую или AMM в зависимости от стадии токена
        }
        try:
            async with self._session.post(PUMPPORTAL_TRADE_URL, data=payload, timeout=15) as resp:
                if resp.status != 200:
                    logger.error("PumpPortal вернул %s: %s", resp.status, (await resp.text())[:300])
                    return None
                return await resp.read()
        except Exception as exc:  # noqa: BLE001
            logger.error("Ошибка запроса к PumpPortal: %s: %s", type(exc).__name__, exc)
            return None

    # ---------- проверка перед подписью ----------

    async def _resolve_all_keys(self, msg) -> list:
        """Полный список аккаунтов с раскрытием таблиц адресов.

        В версионных транзакциях часть аккаунтов хранится не в самой
        транзакции, а в справочниках (address lookup tables) — в сообщении
        лежат только индексы. Без раскрытия проверка «нужный ли это токен»
        давала бы ложный отказ, потому что mint может подгружаться оттуда.

        Порядок склейки — как в Solana: статические, затем writable из
        таблиц, затем readonly."""
        keys = [str(k) for k in msg.account_keys]
        lookups = list(getattr(msg, "address_table_lookups", None) or [])
        if not lookups:
            return keys

        writable_extra, readonly_extra = [], []
        for lookup in lookups:
            table_addr = str(lookup.account_key)
            body = {"jsonrpc": "2.0", "id": 1, "method": "getAccountInfo",
                    "params": [table_addr, {"encoding": "base64"}]}
            try:
                async with self._session.post(settings.helius_rpc(), json=body, timeout=10) as resp:
                    data = await resp.json()
                raw = (((data.get("result") or {}).get("value") or {}).get("data") or [None])[0]
                if not raw:
                    continue
                blob = base64.b64decode(raw)
            except Exception as exc:  # noqa: BLE001
                logger.debug("Не удалось прочитать таблицу адресов %s: %s", table_addr, exc)
                continue

            # Список адресов в аккаунте таблицы начинается со смещения 56,
            # дальше подряд по 32 байта на адрес
            addrs = []
            body_bytes = blob[56:]
            for i in range(0, len(body_bytes) - 31, 32):
                addrs.append(_b58encode(body_bytes[i:i + 32]))

            for idx in list(lookup.writable_indexes):
                if idx < len(addrs):
                    writable_extra.append(addrs[idx])
            for idx in list(lookup.readonly_indexes):
                if idx < len(addrs):
                    readonly_extra.append(addrs[idx])

        return keys + writable_extra + readonly_extra

    async def verify_transaction(self, raw_tx: bytes, expected_mint: str,
                                 max_sol_allowed: float) -> VerifyResult:
        """Разбирает транзакцию и проверяет, что она делает именно то, что мы просили."""
        try:
            from solders.transaction import VersionedTransaction
        except ImportError:
            return VerifyResult(False, "не установлен solders — проверить транзакцию нечем")

        try:
            tx = VersionedTransaction.from_bytes(raw_tx)
        except Exception as exc:  # noqa: BLE001
            return VerifyResult(False, f"транзакция не разбирается: {exc}")

        msg = tx.message
        keys = await self._resolve_all_keys(msg)
        if not keys:
            return VerifyResult(False, "в транзакции нет аккаунтов")

        # 1. Плательщик — наш кошелёк
        if keys[0] != self.pubkey:
            return VerifyResult(False, f"плательщик не наш кошелёк: {keys[0]}")

        # 2. Белый список программ
        invoked = set()
        for ix in msg.instructions:
            if ix.program_id_index >= len(keys):
                return VerifyResult(False, "инструкция ссылается на несуществующий аккаунт")
            invoked.add(keys[ix.program_id_index])
        unexpected = invoked - ALLOWED_PROGRAMS
        if unexpected:
            return VerifyResult(False, f"вызываются посторонние программы: {', '.join(sorted(unexpected))}")

        # 3. Предел трат — главная защита от перерасхода.
        #
        # Ищем инструкцию покупки и у самой Pump.fun, и у роутера PumpPortal:
        # фактическая покупка идёт через роутер, и проверка, смотревшая только
        # на программу Pump.fun, молча ничего не находила и пропускала
        # транзакцию без ограничения. Поэтому теперь ненайденный предел —
        # это ОТКАЗ, а не разрешение: лучше не совершить сделку, чем подписать
        # транзакцию, лимит трат которой мы не смогли прочитать.
        max_sol_cost = None
        for ix in msg.instructions:
            prog = keys[ix.program_id_index]
            data = bytes(ix.data)
            if len(data) < 24:
                continue
            is_pump_buy = prog == settings.PUMP_FUN_PROGRAM_ID and data[:8] == BUY_DISCRIMINATOR
            is_router_buy = prog == ROUTER_PROGRAM and data[:8] == ROUTER_BUY_DISCRIMINATOR
            if not (is_pump_buy or is_router_buy):
                continue

            _amount, max_cost = struct.unpack("<QQ", data[8:24])
            candidate = max_cost / LAMPORTS_PER_SOL
            max_sol_cost = candidate if max_sol_cost is None else max(max_sol_cost, candidate)
            if candidate > max_sol_allowed:
                return VerifyResult(
                    False,
                    f"лимит трат в транзакции {candidate:.6f} SOL больше разрешённого "
                    f"{max_sol_allowed:.6f} SOL",
                    candidate,
                )

        if max_sol_cost is None:
            return VerifyResult(
                False,
                "в транзакции не найдена инструкция покупки с читаемым пределом трат — "
                "подписывать вслепую нельзя",
            )

        # 4. Нужный ли это токен
        if expected_mint not in keys:
            return VerifyResult(False, f"в транзакции нет нужного токена {expected_mint}")

        return VerifyResult(True, "проверки пройдены", max_sol_cost)

    async def simulate(self, raw_signed_tx_b64: str) -> tuple[bool, str]:
        """Прогон в RPC без отправки: ловит нерабочую транзакцию до траты комиссии."""
        body = {
            "jsonrpc": "2.0", "id": 1, "method": "simulateTransaction",
            "params": [raw_signed_tx_b64, {"encoding": "base64", "commitment": "processed",
                                            "replaceRecentBlockhash": True}],
        }
        try:
            async with self._session.post(settings.helius_rpc(), json=body, timeout=15) as resp:
                data = await resp.json()
        except Exception as exc:  # noqa: BLE001
            return False, f"симуляция не выполнилась: {type(exc).__name__}: {exc}"

        value = (data.get("result") or {}).get("value") or {}
        err = value.get("err")
        if err:
            logs = value.get("logs") or []
            tail = " | ".join(logs[-3:]) if logs else ""
            return False, f"симуляция вернула ошибку: {err} {tail}"
        return True, "симуляция успешна"

    # ---------- подпись ----------

    def sign(self, raw_tx: bytes) -> Optional[str]:
        try:
            from solders.transaction import VersionedTransaction
            tx = VersionedTransaction.from_bytes(raw_tx)
            signed = VersionedTransaction(tx.message, [self._keypair])
            return base64.b64encode(bytes(signed)).decode()
        except Exception as exc:  # noqa: BLE001
            logger.error("Ошибка подписи: %s: %s", type(exc).__name__, exc)
            return None

    # ---------- отправка ----------

    async def send(self, signed_tx_b64: str) -> Optional[str]:
        body = {
            "jsonrpc": "2.0", "id": 1, "method": "sendTransaction",
            "params": [signed_tx_b64, {"encoding": "base64", "skipPreflight": False,
                                        "maxRetries": 3, "preflightCommitment": "processed"}],
        }
        try:
            async with self._session.post(settings.helius_rpc(), json=body, timeout=20) as resp:
                data = await resp.json()
        except Exception as exc:  # noqa: BLE001
            logger.error("Ошибка отправки транзакции: %s: %s", type(exc).__name__, exc)
            return None
        if data.get("error"):
            logger.error("RPC отклонил транзакцию: %s", data["error"])
            return None
        return data.get("result")

    # ---------- сделки ----------

    async def execute_buy(self, token_mint: str, amount_sol: float) -> "ExecutionResult":
        from core.jito_executor import ExecutionResult

        if settings.DRY_RUN or not self._keypair:
            logger.info("[DRY_RUN] Покупка на %.4f SOL токена %s (симуляция)", amount_sol, token_mint)
            return ExecutionResult(success=True, dry_run=True)

        slippage_pct = settings.SLIPPAGE_BPS / 100
        priority_fee = settings.JITO_TIP_LAMPORTS / LAMPORTS_PER_SOL

        raw = await self.build_trade_tx("buy", token_mint, amount_sol, True, slippage_pct, priority_fee)
        if not raw:
            return ExecutionResult(success=False, error="PumpPortal не вернул транзакцию")

        # Запас над запрошенной суммой: проскальзывание + комиссия за
        # приоритет. Всё, что просит больше — не подписываем.
        max_allowed = amount_sol * (1 + slippage_pct / 100) + priority_fee + 0.001
        verdict = await self.verify_transaction(raw, token_mint, max_allowed)
        if not verdict.ok:
            logger.error("Покупка %s отменена проверкой: %s", token_mint, verdict.reason)
            return ExecutionResult(success=False, error=f"проверка не пройдена: {verdict.reason}")

        signed = self.sign(raw)
        if not signed:
            return ExecutionResult(success=False, error="не удалось подписать")

        ok, sim_msg = await self.simulate(signed)
        if not ok:
            logger.error("Покупка %s отменена симуляцией: %s", token_mint, sim_msg)
            return ExecutionResult(success=False, error=f"симуляция не прошла: {sim_msg}")

        signature = await self.send(signed)
        if not signature:
            return ExecutionResult(success=False, error="сеть не приняла транзакцию")

        logger.info("Куплено %.4f SOL -> %s (предел трат %.6f SOL), tx=%s",
                    amount_sol, token_mint, verdict.max_sol_cost or 0, signature)
        return ExecutionResult(success=True, signature=signature)

    async def execute_sell(self, token_mint: str, fraction: float = 1.0) -> "ExecutionResult":
        from core.jito_executor import ExecutionResult

        if settings.DRY_RUN or not self._keypair:
            logger.info("[DRY_RUN] Продажа %.0f%% токена %s (симуляция)", fraction * 100, token_mint)
            return ExecutionResult(success=True, dry_run=True)

        percent = max(min(fraction, 1.0), 0.0) * 100
        if percent <= 0:
            return ExecutionResult(success=False, error="доля продажи равна нулю")

        slippage_pct = settings.SLIPPAGE_BPS / 100
        priority_fee = settings.JITO_TIP_LAMPORTS / LAMPORTS_PER_SOL

        # PumpPortal принимает долю строкой с процентом — так не нужно
        # отдельно запрашивать баланс токена и округлять его самим
        raw = await self.build_trade_tx("sell", token_mint, f"{percent:.4f}%", False,
                                        slippage_pct, priority_fee)
        if not raw:
            return ExecutionResult(success=False, error="PumpPortal не вернул транзакцию продажи")

        verdict = await self.verify_sell(raw, token_mint)
        if not verdict.ok:
            logger.error("Продажа %s отменена проверкой: %s", token_mint, verdict.reason)
            return ExecutionResult(success=False, error=f"проверка не пройдена: {verdict.reason}")

        signed = self.sign(raw)
        if not signed:
            return ExecutionResult(success=False, error="не удалось подписать")

        ok, sim_msg = await self.simulate(signed)
        if not ok:
            logger.error("Продажа %s отменена симуляцией: %s", token_mint, sim_msg)
            return ExecutionResult(success=False, error=f"симуляция не прошла: {sim_msg}")

        signature = await self.send(signed)
        if not signature:
            return ExecutionResult(success=False, error="сеть не приняла транзакцию продажи")

        logger.info("Продано %.0f%% %s, tx=%s", percent, token_mint, signature)
        return ExecutionResult(success=True, signature=signature)

    async def verify_sell(self, raw_tx: bytes, expected_mint: str) -> VerifyResult:
        """Проверка продажи. Риск здесь зеркальный покупке: не переплатить,
        а получить слишком мало. Поэтому смотрим, что нижняя граница выручки
        вообще задана — нулевая означала бы согласие отдать токены даром."""
        try:
            from solders.transaction import VersionedTransaction
            tx = VersionedTransaction.from_bytes(raw_tx)
        except Exception as exc:  # noqa: BLE001
            return VerifyResult(False, f"транзакция не разбирается: {exc}")

        msg = tx.message
        keys = await self._resolve_all_keys(msg)
        if not keys or keys[0] != self.pubkey:
            return VerifyResult(False, "плательщик не наш кошелёк")

        invoked = {keys[ix.program_id_index] for ix in msg.instructions
                   if ix.program_id_index < len(keys)}
        unexpected = invoked - ALLOWED_PROGRAMS
        if unexpected:
            return VerifyResult(False, f"посторонние программы: {', '.join(sorted(unexpected))}")

        if expected_mint not in keys:
            return VerifyResult(False, f"в транзакции нет токена {expected_mint}")

        found_min_out = None
        for ix in msg.instructions:
            prog = keys[ix.program_id_index]
            data = bytes(ix.data)
            if len(data) < 24:
                continue
            if prog in (settings.PUMP_FUN_PROGRAM_ID, ROUTER_PROGRAM):
                _amount, min_out = struct.unpack("<QQ", data[8:24])
                found_min_out = min_out / LAMPORTS_PER_SOL

        if found_min_out is None:
            return VerifyResult(False, "не найдена инструкция продажи — подписывать вслепую нельзя")
        if found_min_out <= 0:
            return VerifyResult(False, "нижняя граница выручки равна нулю — токены отдавались бы даром")

        return VerifyResult(True, f"минимальная выручка {found_min_out:.6f} SOL")
