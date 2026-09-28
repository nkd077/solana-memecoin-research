"""
Клиент к GMGN WebSocket с обходом Cloudflare через curl_cffi.
Использует impersonate="chrome124" для имитации TLS-отпечатка Chrome.
"""
import json
import asyncio
from typing import Callable, Awaitable, Optional, List
from datetime import datetime
import os

# Используем curl_cffi вместо websockets для обхода Cloudflare
from curl_cffi.requests import Session
from curl_cffi.requests import WebSocket

from config import settings


class GMGNClient:
    def __init__(self):
        self.ws_url = settings.GMGN_WS_URL
        self.device_id = os.getenv("GMGN_DEVICE_ID", "web")
        self.fp_did = os.getenv("GMGN_FP_DID", "")
        self.app_ver = os.getenv("GMGN_APP_VER", "2.3.1")
        self.watch_wallets = os.getenv("GMGN_WATCH_WALLETS", "").split(",")
        self.watch_wallets = [w.strip() for w in self.watch_wallets if w.strip()]
        
        self._ws = None
        self._running = False
        self._session = None
        
        print(f"[GMGN] Инициализация")
        print(f"[GMGN] Отслеживаемых кошельков: {len(self.watch_wallets)}")
        if self.watch_wallets:
            print(f"[GMGN] Первые 3: {self.watch_wallets[:3]}")

    def _build_url(self) -> str:
        """Собирает URL с параметрами для обхода Cloudflare"""
        params = {
            "device_id": self.device_id,
            "fp_did": self.fp_did,
            "app_ver": self.app_ver,
            "platform": "web",
        }
        query = "&".join([f"{k}={v}" for k, v in params.items() if v])
        return f"{self.ws_url}?{query}"

    def _get_headers(self) -> dict:
        """Возвращает браузерные заголовки для обхода Cloudflare"""
        return {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Origin": "https://gmgn.ai",
            "Referer": "https://gmgn.ai/",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Sec-WebSocket-Version": "13",
            "Sec-WebSocket-Extensions": "permessage-deflate",
            "Sec-WebSocket-Key": "dGhlIHNhbXBsZSBub25jZQ==",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
        }

    async def connect(self):
        """Подключается к WebSocket с имитацией TLS-отпечатка Chrome"""
        url = self._build_url()
        headers = self._get_headers()
        
        print(f"[GMGN] Подключение к {url[:60]}...")
        
        # Используем curl_cffi с impersonate для обхода Cloudflare
        self._session = Session(impersonate="chrome124")
        self._ws = self._session.ws_connect(
            url,
            headers=headers,
            timeout=30,
        )
        
        print(f"[GMGN] ✅ Подключено!")

    async def subscribe_wallet_trades(self):
        """Подписывается на сделки отслеживаемых кошельков"""
        if not self.watch_wallets:
            print("[GMGN] ⚠️ Нет кошельков для отслеживания!")
            print("[GMGN] Добавьте GMGN_WATCH_WALLETS=адрес1,адрес2 в .env")
            return
        
        if not self._ws:
            await self.connect()
        
        # Формат подписки по документации ChipaDevTeam/GmGnAPI
        sub_msg = {
            "op": "subscribe",
            "channel": "wallet_trade_data",
            "data": {
                "addresses": self.watch_wallets,
                "chain": "solana"
            }
        }
        
        await self._ws.send(json.dumps(sub_msg))
        print(f"[GMGN] ✅ Подписан на {len(self.watch_wallets)} кошельков")

    async def subscribe_new_pools(self):
        """Подписывается на новые пулы"""
        if not self._ws:
            await self.connect()
        
        sub_msg = {
            "op": "subscribe",
            "channel": "new_pool",
            "data": {"chain": "solana"}
        }
        await self._ws.send(json.dumps(sub_msg))
        print("[GMGN] ✅ Подписан на новые пулы")

    async def listen(self, on_message: Callable[[dict], Awaitable[None]]):
        """Основной цикл чтения с автопереподключением"""
        if not self.watch_wallets:
            print("[GMGN] ⚠️ Нет кошельков, перехожу в режим ожидания")
            while True:
                await asyncio.sleep(60)
            return
        
        self._running = True
        backoff = 1.0
        
        while self._running:
            try:
                if self._ws is None:
                    await self.connect()
                    await self.subscribe_wallet_trades()
                
                # Читаем сообщения
                async for raw in self._ws:
                    backoff = 1.0
                    try:
                        msg = json.loads(raw)
                        await on_message(msg)
                    except json.JSONDecodeError:
                        continue
                        
            except Exception as e:
                print(f"[GMGN] ❌ Ошибка: {e}")
                print(f"[GMGN] Переподключение через {backoff}s...")
                self._ws = None
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def stop(self):
        self._running = False
        if self._ws:
            self._ws.close()


def parse_gmgn_wallet_trade(msg: dict) -> Optional[dict]:
    """
    Парсит сообщение от GMGN wallet_trade_data.
    Формат реконструирован на основе ChipaDevTeam/GmGnAPI.
    """
    # Проверяем канал
    channel = msg.get("channel") or msg.get("ch")
    if channel not in ("wallet_trade_data", "trade"):
        return None
    
    # Извлекаем данные
    data = msg.get("data", {})
    
    # Пытаемся найти сделку в разных форматах
    trade = None
    
    # Формат 1: прямая сделка
    if "trade" in data:
        trade = data["trade"]
    elif "trades" in data and data["trades"]:
        trade = data["trades"][0]
    elif "event" in data:
        trade = data
    
    if not trade:
        return None
    
    # Проверяем, что это покупка
    side = trade.get("side") or trade.get("type") or trade.get("event_type")
    if side and side.lower() not in ("buy", "purchase"):
        return None
    
    return {
        "wallet": trade.get("wallet") or trade.get("wallet_address") or trade.get("address"),
        "token_mint": trade.get("token") or trade.get("mint") or trade.get("token_address"),
        "side": "buy",
        "amount_usd": float(trade.get("amount_usd") or trade.get("usd_amount") or 0),
        "amount_sol": float(trade.get("amount_sol") or trade.get("sol_amount") or 0),
        "pool_liquidity_usd": float(trade.get("liquidity_usd") or trade.get("pool_liquidity") or 0),
        "timestamp": trade.get("timestamp") or datetime.now().timestamp(),
        "is_smart_money": trade.get("is_smart_money", False),
        "token_decimals": trade.get("decimals", 9),
    }
