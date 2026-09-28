"""
Проверки против скам-монет: mint-authority, freeze-authority, distribution.
"""
from __future__ import annotations

import logging
from typing import Optional

import aiohttp

from core.pump_curve import derive_curve_address

logger = logging.getLogger("sniper.rug_checker")

# Уровень подтверждения для всех запросов проверки.
#
# По умолчанию RPC отвечает на уровне finalized, который отстаёт секунд на
# тринадцать. Пока бот опрашивал блокчейн раз в 15 секунд, токен успевал
# финализироваться и проверка работала. После перехода на подписку мы стали
# приходить через секунду — и на финализированном уровне аккаунт монеты ещё
# не существует. Две трети проверок падали с «аккаунт не найден», а падение
# проверки означает отказ от сделки.
COMMITMENT = "confirmed"

# Пороги для детектирования скама
MAX_SUPPLY_MULTIPLE = 1_000_000_000  # если создатель может напечатать миллиарды, это плохо
MAX_HOLDER_SHARE = 0.60  # если одна сторона держит >60%, это плохо


class RugChecker:
    """Проверяет монету на признаки скама/rug-pull."""

    def __init__(self, helius_rpc_url: str):
        self._helius_url = helius_rpc_url

    async def check_mint(self, mint_address: str) -> dict:
        """Проверяет mint-authority и freeze-authority через Helius.
        
        Возвращает:
        {
            "is_safe": bool,
            "mint_authority_is_null": bool,
            "freeze_authority_is_null": bool,
            "issues": [str, ...]  # список найденных проблем
        }
        """
        issues = []

        # Запрос к Helius для получения информации о mint
        try:
            async with aiohttp.ClientSession() as session:
                # ВНИМАНИЕ: метода getParsedAccountInfo в Solana RPC НЕ существует
                # (была моя ошибка — из-за неё проверка mint/freeze-authority
                # молча падала на каждом токене). Правильный вызов —
                # getAccountInfo с encoding=jsonParsed.
                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getAccountInfo",
                    "params": [mint_address, {"encoding": "jsonParsed", "commitment": COMMITMENT}],
                }
                async with session.post(self._helius_url, json=payload, timeout=10) as resp:
                    if resp.status != 200:
                        logger.warning("Helius getParsedAccountInfo failed for %s: %d", mint_address, resp.status)
                        return {"is_safe": False, "check_failed": True, "issues": ["Не удалось запросить данные mint"]}

                    data = await resp.json()
                    if "error" in data:
                        logger.warning("Helius error for %s: %s", mint_address, data["error"])
                        return {"is_safe": False, "check_failed": True, "issues": ["Ошибка при запросе данных mint"]}

                    result = data.get("result") or {}
                    # ВАЖНО: "value" присутствует в ответе даже когда равен
                    # null (аккаунт не найден), поэтому проверять наличие
                    # ключа недостаточно — падало на .get() у пустоты.
                    value = result.get("value")
                    if not isinstance(value, dict):
                        return {"is_safe": False, "check_failed": True,
                                "issues": ["Аккаунт монеты не найден или ответ пуст"]}

                    payload = value.get("data")
                    # при jsonParsed это словарь, но если разобрать не вышло,
                    # RPC отдаёт список [данные, "base64"] — тогда полей нет
                    if not isinstance(payload, dict):
                        return {"is_safe": False, "check_failed": True,
                                "issues": ["Данные монеты пришли в неразобранном виде"]}

                    parsed = payload.get("parsed") or {}
                    info = parsed.get("info") or {}

        except Exception as e:
            logger.exception("RugChecker error: %s", e)
            return {"is_safe": False, "issues": [f"Исключение: {e}"]}

        # Проверка mint-authority
        mint_authority = info.get("mintAuthority")
        mint_authority_is_null = mint_authority is None or mint_authority == "11111111111111111111111111111111"
        if not mint_authority_is_null:
            issues.append("⚠️ Mint-authority существует — создатель может печатать токены")

        # Проверка freeze-authority
        freeze_authority = info.get("freezeAuthority")
        freeze_authority_is_null = freeze_authority is None or freeze_authority == "11111111111111111111111111111111"
        if not freeze_authority_is_null:
            issues.append("⚠️ Freeze-authority существует — создатель может заморозить счета")

        # Проверка на очень большое supply. Solana RPC отдаёт supply СТРОКОЙ
        # (чтобы избежать потери точности на больших числах в JSON/JS), поэтому
        # приводим к int явно, а не полагаемся на то, что это уже число.
        try:
            supply = int(info.get("supply", 0) or 0)
        except (TypeError, ValueError):
            supply = 0
        decimals = int(info.get("decimals", 0) or 0)
        max_supply_with_decimals = supply / (10 ** decimals) if decimals else supply
        if max_supply_with_decimals > MAX_SUPPLY_MULTIPLE and decimals < 6:
            issues.append(f"⚠️ Очень большое supply ({max_supply_with_decimals:.0f}), легко создать инфляцию")

        is_safe = len(issues) == 0

        return {
            "is_safe": is_safe,
            "mint_authority_is_null": mint_authority_is_null,
            "freeze_authority_is_null": freeze_authority_is_null,
            "issues": issues,
        }

    async def check_distribution(self, mint_address: str) -> dict:
        """Проверяет распределение токенов — не должно быть одного крупного держателя.
        
        Возвращает:
        {
            "is_safe": bool,
            "top_holder_share": float,  # доля топ-холдера (0..1)
            "top_holders_count": int,
            "issues": [str, ...]
        }
        """
        issues = []
        top_holder_share = 0.0

        try:
            async with aiohttp.ClientSession() as session:
                # Запрос наибольших держателей через Helius getTokenLargestAccounts
                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getTokenLargestAccounts",
                    "params": [mint_address, {"commitment": COMMITMENT}],
                }
                async with session.post(self._helius_url, json=payload, timeout=10) as resp:
                    if resp.status != 200:
                        logger.warning("getTokenLargestAccounts failed for %s: %d", mint_address, resp.status)
                        return {
                            "is_safe": False,
                            "check_failed": True,
                            "top_holder_share": 1.0,
                            "top_holders_count": 0,
                            "issues": ["Не удалось запросить распределение"],
                        }

                    data = await resp.json()
                    if "error" in data:
                        logger.warning("getTokenLargestAccounts error for %s: %s", mint_address, data["error"])
                        return {
                            "is_safe": False,
                            "check_failed": True,
                            "top_holder_share": 1.0,
                            "top_holders_count": 0,
                            "issues": ["Ошибка при запросе распределения"],
                        }

                    accounts = data.get("result", {}).get("value", [])
                    if not accounts:
                        return {
                            "is_safe": False,
                            "check_failed": True,
                            "top_holder_share": 1.0,
                            "top_holders_count": 0,
                            "issues": ["Нет информации о холдерах"],
                        }

                    # Исключаем аккаунты, принадлежащие бондинг-кривой Pump.fun.
                    #
                    # До выпуска на Raydium кривая держит весь невыкупленный
                    # запас и всегда идёт первой в списке — без исключения
                    # КАЖДЫЙ ранний токен выглядел бы как "почти всё в одних
                    # руках" и блокировался бы.
                    #
                    # Владельца спрашиваем у блокчейна, а не выводим адрес
                    # аккаунта формулой: вывод ATA оказался неверным на живых
                    # данных (проверено), и такая формула молча ломается при
                    # изменениях программы. Один getMultipleAccounts на все
                    # адреса разом — надёжнее и стоит одного запроса.
                    curve_pda = derive_curve_address(mint_address)
                    if curve_pda and accounts:
                        owners = await self._resolve_owners(
                            session, [a.get("address") for a in accounts]
                        )
                        before = len(accounts)
                        accounts = [
                            a for a in accounts
                            if owners.get(a.get("address")) != curve_pda
                        ]
                        if len(accounts) < before:
                            logger.debug("%s: из проверки концентрации исключено аккаунтов кривой: %d",
                                         mint_address, before - len(accounts))

                    if not accounts:
                        # Кроме кривой держателей нет — токен только что создан,
                        # оценивать концентрацию не на чем
                        return {
                            "is_safe": True,
                            "top_holder_share": 0.0,
                            "top_holders_count": 0,
                            "issues": [],
                        }

                    total_supply = sum(float(a.get("uiAmount", 0) or 0) for a in accounts)
                    if total_supply > 0:
                        top_amount = float(accounts[0].get("uiAmount", 0) or 0)
                        top_holder_share = top_amount / total_supply if total_supply else 0.0

        except Exception as e:
            logger.exception("Distribution check error: %s", e)
            return {
                "is_safe": False,
                "check_failed": True,
                "top_holder_share": 1.0,
                "top_holders_count": 0,
                "issues": [f"Исключение: {e}"],
            }

        # Проверка порога
        if top_holder_share > MAX_HOLDER_SHARE:
            issues.append(
                f"⚠️ Топ-холдер владеет {top_holder_share*100:.1f}% токенов (>{MAX_HOLDER_SHARE*100}%)"
            )

        is_safe = len(issues) == 0

        return {
            "is_safe": is_safe,
            "top_holder_share": round(top_holder_share, 4),
            "top_holders_count": len(accounts),
            "issues": issues,
        }

    async def _resolve_owners(self, session, addresses: list) -> dict:
        """Владельцы токен-аккаунтов одним запросом: адрес -> owner."""
        owners = {}
        if not addresses:
            return owners
        try:
            payload = {
                "jsonrpc": "2.0", "id": 1, "method": "getMultipleAccounts",
                "params": [addresses, {"encoding": "jsonParsed", "commitment": COMMITMENT}],
            }
            async with session.post(self._helius_url, json=payload, timeout=15) as resp:
                if resp.status != 200:
                    return owners
                data = await resp.json()
            values = (data.get("result") or {}).get("value") or []
            for addr, val in zip(addresses, values):
                if not val:
                    continue
                info = ((val.get("data") or {}).get("parsed") or {}).get("info") or {}
                if info.get("owner"):
                    owners[addr] = info["owner"]
        except Exception as exc:  # noqa: BLE001
            logger.debug("Не удалось определить владельцев токен-аккаунтов: %s", exc)
        return owners

    async def full_check(self, mint_address: str) -> dict:
        """Полная проверка: mint/freeze/distribution."""
        mint_check = await self.check_mint(mint_address)
        dist_check = await self.check_distribution(mint_address)

        combined_issues = mint_check.get("issues", []) + dist_check.get("issues", [])
        is_safe = mint_check["is_safe"] and dist_check["is_safe"]
        check_failed = bool(mint_check.get("check_failed") or dist_check.get("check_failed"))

        return {
            "is_safe": is_safe,
            "check_failed": check_failed,
            "mint_authority_is_null": mint_check.get("mint_authority_is_null"),
            "freeze_authority_is_null": mint_check.get("freeze_authority_is_null"),
            "top_holder_share": dist_check.get("top_holder_share"),
            "issues": combined_issues,
        }
