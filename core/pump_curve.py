"""
Чтение состояния бондинг-кривой Pump.fun напрямую из блокчейна.

Зачем: до "выпуска" на Raydium (примерно $69k капитализации) новый токен
Pump.fun не индексируется ни DexScreener, ни Birdeye — у них просто нет
такой пары. А это ровно те токены, ради которых бот и существует.
Получается тупик: сигнал есть, а оценить цену/ликвидность нечем.

Бондинг-кривая — это и есть рынок такого токена до выпуска, поэтому её
состояние и является авторитетным источником цены. Читаем аккаунт кривой
через тот же Helius RPC, без дополнительных ключей и сторонних API.

Структура аккаунта BondingCurve (Anchor):
    8  байт  — дискриминатор
    u64      — virtual_token_reserves
    u64      — virtual_sol_reserves
    u64      — real_token_reserves
    u64      — real_sol_reserves
    u64      — token_total_supply
    u8       — complete (кривая закрыта, ликвидность ушла на Raydium)
    [32]     — creator (в новых версиях аккаунта, необязательно)

Цена токена в SOL = (virtual_sol_reserves / 1e9) / (virtual_token_reserves / 1e6),
где 9 — десятичные SOL, 6 — стандартные десятичные токена Pump.fun.
"""
from __future__ import annotations

import base64
import logging
import struct
from dataclasses import dataclass
from typing import Optional

import aiohttp

from config import settings

logger = logging.getLogger("sniper.pump_curve")

SOL_DECIMALS = 9
PUMP_TOKEN_DECIMALS = 6
CURVE_STRUCT = "<QQQQQ?"          # 5 x u64 + bool
CURVE_STRUCT_SIZE = struct.calcsize(CURVE_STRUCT)
DISCRIMINATOR_SIZE = 8


@dataclass
class CurveState:
    mint: str
    price_sol: float          # цена одного токена в SOL
    price_usd: float
    liquidity_sol: float      # реальный SOL, лежащий в кривой
    liquidity_usd: float
    is_complete: bool         # True = токен уже выпущен на Raydium
    curve_address: str
    virtual_sol: float = 0.0  # виртуальные резервы SOL (старт ~30)

    def entry_slippage_pct(self, position_sol: float) -> float:
        """Ожидаемое проскальзывание при покупке на position_sol.

        Для постоянного произведения с виртуальными резервами:
            получаем_токенов = vT * dS / (vS + dS)
            цена_сделки / спот_цена - 1 = dS / vS

        Ключевой момент: у Pump.fun виртуальные резервы стартуют с ~30 SOL,
        поэтому даже у токена с копеечной РЕАЛЬНОЙ ликвидностью
        проскальзывание на малой позиции мизерное. Судить о проскальзывании
        по реальной ликвидности — ошибка."""
        if self.virtual_sol <= 0:
            return 1.0
        return position_sol / self.virtual_sol

    def exit_capacity_ratio(self, position_sol: float) -> float:
        """Во сколько раз реальный SOL в кривой больше нашей позиции.

        В отличие от проскальзывания, ВЫХОД ограничен именно реальным
        SOL: больше, чем в кривой есть на самом деле, забрать нельзя."""
        if position_sol <= 0:
            return float("inf")
        return self.liquidity_sol / position_sol


def derive_curve_address(mint: str) -> Optional[str]:
    """PDA кривой: seeds = [b"bonding-curve", mint], программа Pump.fun."""
    try:
        from solders.pubkey import Pubkey
    except ImportError:
        logger.warning("Пакет solders не установлен — чтение бондинг-кривой недоступно")
        return None
    try:
        mint_key = Pubkey.from_string(mint)
        program = Pubkey.from_string(settings.PUMP_FUN_PROGRAM_ID)
        pda, _bump = Pubkey.find_program_address([b"bonding-curve", bytes(mint_key)], program)
        return str(pda)
    except Exception as exc:  # noqa: BLE001
        logger.debug("Не удалось вывести адрес кривой для %s: %s", mint, exc)
        return None


TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
ASSOCIATED_TOKEN_PROGRAM_ID = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"


def derive_curve_token_account(mint: str) -> Optional[str]:
    """Токен-аккаунт (ATA) бондинг-кривой — именно на нём лежит весь
    непроданный запас токена до выпуска на Raydium.

    Нужен, чтобы исключить кривую из проверки концентрации держателей:
    иначе любой ранний токен выглядит как "99% в одних руках" просто
    потому, что почти всё ещё не выкуплено с кривой, и rug-check
    забраковал бы вообще все ранние сигналы."""
    try:
        from solders.pubkey import Pubkey
    except ImportError:
        return None
    try:
        curve = derive_curve_address(mint)
        if not curve:
            return None
        ata, _bump = Pubkey.find_program_address(
            [
                bytes(Pubkey.from_string(curve)),
                bytes(Pubkey.from_string(TOKEN_PROGRAM_ID)),
                bytes(Pubkey.from_string(mint)),
            ],
            Pubkey.from_string(ASSOCIATED_TOKEN_PROGRAM_ID),
        )
        return str(ata)
    except Exception as exc:  # noqa: BLE001
        logger.debug("Не удалось вывести токен-аккаунт кривой для %s: %s", mint, exc)
        return None


class PumpCurveClient:
    def __init__(self, session: aiohttp.ClientSession):
        self._session = session

    async def get_curve_state(self, mint: str, sol_price_usd: float) -> Optional[CurveState]:
        curve_address = derive_curve_address(mint)
        if not curve_address:
            return None

        body = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            # confirmed, а не finalized: свежая кривая на финализированном уровне
            # ещё не видна, и цена не находилась бы у самых новых токенов
            "params": [curve_address, {"encoding": "base64", "commitment": "confirmed"}],
        }
        try:
            async with self._session.post(settings.helius_rpc(), json=body, timeout=10) as resp:
                if resp.status != 200:
                    logger.debug("getAccountInfo для кривой %s вернул %s", mint, resp.status)
                    return None
                payload = await resp.json()
        except Exception as exc:  # noqa: BLE001
            logger.debug("Ошибка запроса кривой для %s: %s: %s", mint, type(exc).__name__, exc)
            return None

        value = (payload.get("result") or {}).get("value")
        if not value:
            # Аккаунта нет — это не токен Pump.fun либо кривая уже закрыта и удалена
            return None

        data_field = value.get("data")
        if not isinstance(data_field, list) or not data_field:
            return None

        try:
            raw = base64.b64decode(data_field[0])
        except Exception:  # noqa: BLE001
            return None

        body_bytes = raw[DISCRIMINATOR_SIZE:DISCRIMINATOR_SIZE + CURVE_STRUCT_SIZE]
        if len(body_bytes) < CURVE_STRUCT_SIZE:
            logger.debug("Аккаунт кривой %s короче ожидаемого (%d байт)", mint, len(raw))
            return None

        try:
            (virtual_token_reserves, virtual_sol_reserves,
             real_token_reserves, real_sol_reserves,
             token_total_supply, complete) = struct.unpack(CURVE_STRUCT, body_bytes)
        except struct.error as exc:
            logger.debug("Не удалось разобрать аккаунт кривой %s: %s", mint, exc)
            return None

        if virtual_token_reserves <= 0 or virtual_sol_reserves <= 0:
            return None

        sol_reserves = virtual_sol_reserves / (10 ** SOL_DECIMALS)
        token_reserves = virtual_token_reserves / (10 ** PUMP_TOKEN_DECIMALS)
        price_sol = sol_reserves / token_reserves

        liquidity_sol = real_sol_reserves / (10 ** SOL_DECIMALS)

        return CurveState(
            mint=mint,
            price_sol=price_sol,
            price_usd=price_sol * sol_price_usd,
            liquidity_sol=liquidity_sol,
            liquidity_usd=liquidity_sol * sol_price_usd,
            is_complete=bool(complete),
            curve_address=curve_address,
            virtual_sol=sol_reserves,
        )
