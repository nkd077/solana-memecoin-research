"""
Проверка live-исполнения end-to-end (без обязательной реальной покупки мемкоина).

Что проверяет:
  1. Ключ загружается, pubkey читается
  2. Helius RPC отвечает (getHealth / getBalance)
  3. UnifiedExecutor в режиме DRY_RUN успешно «покупает» и «продаёт»
  4. Опционально (--live-dust): реальная микро-сделка WSOL wrap/unwrap
     или минимальный buy через PumpPortal на TEST_MINT (опасно!)

Запуск:
  python -m tools.verify_live
  python -m tools.verify_live --live-dust   # только если сознательно готовы потратить SOL
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

import aiohttp

from config import settings
from core.db import get_db
from core.executor import UnifiedExecutor


OUT = Path("data/live_verify.json")


async def check_rpc(session: aiohttp.ClientSession) -> dict:
    body = {"jsonrpc": "2.0", "id": 1, "method": "getHealth"}
    async with session.post(settings.helius_rpc(), json=body, timeout=15) as resp:
        return {"status": resp.status, "body": await resp.json()}


async def check_balance(session: aiohttp.ClientSession, pubkey: str) -> dict:
    body = {
        "jsonrpc": "2.0", "id": 1, "method": "getBalance",
        "params": [pubkey],
    }
    async with session.post(settings.helius_rpc(), json=body, timeout=15) as resp:
        data = await resp.json()
        lamports = ((data.get("result") or {}).get("value"))
        return {"lamports": lamports, "sol": (lamports or 0) / 1e9}


async def run(live_dust: bool = False) -> dict:
    report = {"ts": time.time(), "dry_run_setting": settings.DRY_RUN, "checks": {}}
    db = get_db()

    async with aiohttp.ClientSession() as session:
        # RPC
        try:
            report["checks"]["helius_health"] = await check_rpc(session)
            report["checks"]["helius_health"]["ok"] = (
                report["checks"]["helius_health"]["status"] == 200
            )
        except Exception as exc:  # noqa: BLE001
            report["checks"]["helius_health"] = {"ok": False, "error": str(exc)}

        executor = UnifiedExecutor(session)
        pubkey = executor.pubkey
        report["checks"]["keypair"] = {
            "ok": bool(pubkey),
            "pubkey": pubkey,
            "mode": executor.mode,
        }

        if pubkey:
            try:
                report["checks"]["balance"] = await check_balance(session, pubkey)
                report["checks"]["balance"]["ok"] = True
            except Exception as exc:  # noqa: BLE001
                report["checks"]["balance"] = {"ok": False, "error": str(exc)}

        # DRY_RUN путь всегда
        mint = "So11111111111111111111111111111111111111112"  # dummy для симуляции
        # Временно форсируем DRY_RUN на симуляцию buy/sell API-контракта
        old = settings.DRY_RUN
        settings.DRY_RUN = True
        buy = await executor.execute_buy(mint, 0.01)
        sell = await executor.execute_sell(mint, 1.0)
        settings.DRY_RUN = old
        report["checks"]["dry_buy"] = {"ok": buy.success, "dry_run": buy.dry_run, "error": buy.error}
        report["checks"]["dry_sell"] = {"ok": sell.success, "dry_run": sell.dry_run, "error": sell.error}

        db.log_trade(
            token_mint=mint, whale="verify", side="buy", size_sol=0.01,
            dry_run=True, reason_code="verify_live", signature=buy.signature or "",
        )

        if live_dust:
            if settings.DRY_RUN:
                report["checks"]["live_dust"] = {
                    "ok": False,
                    "error": "Для --live-dust нужен DRY_RUN=false в .env",
                }
            elif not pubkey:
                report["checks"]["live_dust"] = {"ok": False, "error": "нет ключа"}
            else:
                test_mint = (settings.LIVE_VERIFY_MINT or "").strip()
                amount = float(settings.LIVE_VERIFY_SOL or 0.01)
                if not test_mint:
                    report["checks"]["live_dust"] = {
                        "ok": False,
                        "error": "Задайте LIVE_VERIFY_MINT в .env (реальный mint для микро-покупки)",
                    }
                else:
                    print(f"⚠️  LIVE dust buy {amount} SOL -> {test_mint}")
                    result = await executor.execute_buy(test_mint, amount)
                    report["checks"]["live_dust"] = {
                        "ok": result.success,
                        "signature": result.signature,
                        "error": result.error,
                        "dry_run": result.dry_run,
                    }
                    if result.success and result.signature:
                        # сразу продаём обратно насколько возможно
                        sell_r = await executor.execute_sell(test_mint, 1.0)
                        report["checks"]["live_dust_sell"] = {
                            "ok": sell_r.success,
                            "signature": sell_r.signature,
                            "error": sell_r.error,
                        }

    report["all_ok"] = all(
        (c.get("ok") is True) for k, c in report["checks"].items()
        if k != "live_dust_sell" and isinstance(c, dict) and "ok" in c
    )
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    return report


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--live-dust", action="store_true",
                    help="Реальная микро-сделка (нужны DRY_RUN=false и LIVE_VERIFY_MINT)")
    args = ap.parse_args()
    report = asyncio.run(run(live_dust=args.live_dust))
    print(json.dumps(report, indent=2, ensure_ascii=False))
    print(f"\nОтчёт: {OUT}")
    sys.exit(0 if report.get("all_ok") else 1)


if __name__ == "__main__":
    main()
