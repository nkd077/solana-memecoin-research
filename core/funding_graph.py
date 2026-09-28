"""
Граф финансирования кошельков для live-цикла.

Логика перенесена из analyze_clusters.py / wallet_factors.py:
для кошелька находим первую входящую транзакцию (рождение) и адрес,
который его пополнил. Несколько свежих покупателей одного токена
с общим funder — признак организованной когорты.

Биржевые хабы (огромный fanout) исключаются: иначе Binance склеит
несвязанных людей в ложный кластер. Fanout считаем из SQLite
(COUNT по funder), не из счётчика «только после свежего RPC».
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

import aiohttp

from config import settings
from core.db import Database, get_db
from core.helius_gate import get_helius_gate

logger = logging.getLogger("sniper.funding_graph")

# Плательщик комиссии / программы — НЕ спонсор кошелька.
# Если fallback accountKeys[0] совпал с этим списком → funder неизвестен
# (иначе «общий funder» = общий релеер/роутер → ложные кластеры).
_SYSTEM_OR_PROGRAM_FUNDERS = frozenset({
    "11111111111111111111111111111111",
    "ComputeBudget111111111111111111111111111111",
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",  # Token-2022
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",  # ATA
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",  # Pump.fun
    "PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY",
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",
    "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB",
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
    "srmqPvymJeLKQ4B2vieyJv4WwAoULM9GWydFow2yX4E",
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",  # Raydium AMM
    # Jito tip accounts (часто fee-payer в первой tx)
    "96gYZGLnJYVFmbjzopPSU6QiUV5CWE35YqF9EjRnz7mL",
    "HFqU5x63VTqvQss8hp11i4bVmkdsTpeESxiqz7vuP4M",
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
    "DfXygSm4jCyNCybVYYK6DwvZqjUnUh8mg6tHPHUeN1s",
    "ADuUkR4vqLUMWXxW9ghEykNnCtg9rVYngTkGLNQjBhqL",
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
})


def _sanitize_funder(addr: Optional[str]) -> Optional[str]:
    if not addr or addr in _SYSTEM_OR_PROGRAM_FUNDERS:
        return None
    return addr


@dataclass
class WalletOrigin:
    wallet: str
    birth_unix: Optional[float] = None
    funder: Optional[str] = None
    n_sigs: int = 0
    capped: bool = False
    age_hours: Optional[float] = None

    @property
    def is_fresh(self) -> bool:
        if self.capped or self.age_hours is None:
            return False
        return self.age_hours <= settings.INSIDER_MAX_WALLET_AGE_HOURS


@dataclass
class MintFundingState:
    mint: str
    buyers: dict[str, WalletOrigin] = field(default_factory=dict)
    # funder -> set(wallets) на ЭТОМ минте (когорта, не глобальный хаб)
    by_funder: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    last_touch: float = field(default_factory=time.time)

    def register(self, origin: WalletOrigin):
        self.last_touch = time.time()
        self.buyers[origin.wallet] = origin
        if origin.funder and origin.is_fresh:
            self.by_funder[origin.funder].add(origin.wallet)

    def best_shared_funder(self) -> tuple[Optional[str], int]:
        """Самый крупный shared-funder кластер на минте.

        Глобальный fanout хаба сюда НЕ подмешиваем: размер когорты на
        одном токене и «сколько всего кошельков когда-либо профинансировал
        адрес» — разные величины. Хабы отсекаются раньше в register_buy.
        """
        best_f, best_n = None, 0
        for f, wallets in self.by_funder.items():
            n = len(wallets)
            if n > best_n:
                best_f, best_n = f, n
        return best_f, best_n


class FundingGraph:
    def __init__(self, session: aiohttp.ClientSession, db: Optional[Database] = None):
        self._session = session
        self._db = db or get_db()
        self._sem = asyncio.Semaphore(1)
        self._mints: dict[str, MintFundingState] = {}
        # кэш COUNT(*) FROM wallet_funders WHERE funder=? (не «только RPC++»)
        self._fanout_cache: dict[str, int] = {}
        self._inflight: dict[str, asyncio.Future] = {}
        # useful uncapped resolves vs любые RPC-попытки на mint
        self._rpc_resolves_per_mint: dict[str, int] = defaultdict(int)
        self._rpc_attempts_per_mint: dict[str, int] = defaultdict(int)
        self._health = None
        self._gate = get_helius_gate()
        self._register_count = 0
        self._resolve_ok = 0
        self._resolve_capped = 0
        self._resolve_empty = 0
        self._last_capped_log = 0.0

    def attach_health(self, health) -> None:
        self._health = health
        self._gate.attach_health(health)

    def mint_state(self, mint: str) -> MintFundingState:
        st = self._mints.get(mint)
        if st is None:
            st = MintFundingState(mint=mint)
            self._mints[mint] = st
        return st

    @property
    def rpc_available(self) -> bool:
        return self._gate.available

    def funder_fanout(self, funder: str) -> int:
        """Сколько разных кошельков в БД имеют этого funder'а."""
        if not funder:
            return 0
        cached = self._fanout_cache.get(funder)
        if cached is not None:
            return cached
        n = self._db.count_wallets_by_funder(funder)
        self._fanout_cache[funder] = n
        return n

    def _invalidate_fanout(self, funder: Optional[str]) -> None:
        if funder:
            self._fanout_cache.pop(funder, None)

    def _is_hub_funder(self, funder: Optional[str]) -> bool:
        if not funder:
            return False
        return self.funder_fanout(funder) > settings.INSIDER_MAX_FUNDER_FANOUT

    def _origin_from_cache(self, wallet: str, now: float) -> Optional[WalletOrigin]:
        cached = self._db.get_wallet_funder(wallet)
        if not cached or not cached.get("updated_at"):
            return None
        if (now - float(cached["updated_at"])) >= 7 * 86400:
            return None
        # Старый «capped без birth»: просто ≥100 sigs, birth не искали.
        # Пересчитать с пагинацией — иначе 83% кэша навсегда мёртвые.
        if cached.get("capped") and not cached.get("birth_unix"):
            return None
        birth = cached.get("birth_unix")
        age_h = ((now - birth) / 3600) if birth else None
        return WalletOrigin(
            wallet=wallet,
            birth_unix=birth,
            funder=cached.get("funder"),
            n_sigs=int(cached.get("n_sigs") or 0),
            capped=bool(cached.get("capped")),
            age_hours=age_h,
        )

    async def resolve_wallet(self, wallet: str, at_ts: Optional[float] = None) -> WalletOrigin:
        """Возвращает происхождение кошелька (из БД или RPC)."""
        now = at_ts or time.time()
        cached = self._origin_from_cache(wallet, now)
        if cached is not None:
            return cached

        if not self.rpc_available:
            return WalletOrigin(wallet=wallet)

        existing = self._inflight.get(wallet)
        if existing is not None:
            return await existing

        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        self._inflight[wallet] = fut
        try:
            origin = await self._fetch_origin(wallet, now)
            if origin.birth_unix is not None or origin.capped or origin.funder or origin.n_sigs > 0:
                self._db.upsert_wallet_funder(
                    wallet, origin.birth_unix, origin.funder, origin.n_sigs, origin.capped,
                )
                self._invalidate_fanout(origin.funder)
            fut.set_result(origin)
            return origin
        except Exception as exc:  # noqa: BLE001
            fut.set_exception(exc)
            raise
        finally:
            self._inflight.pop(wallet, None)

    async def register_buy(self, mint: str, wallet: str, at_ts: Optional[float] = None) -> WalletOrigin:
        now = at_ts or time.time()
        st = self.mint_state(mint)
        if wallet in st.buyers:
            return st.buyers[wallet]

        cached = self._origin_from_cache(wallet, now)
        min_buyers = max(1, int(settings.INSIDER_MIN_BUYERS_BEFORE_RPC))

        if cached is None:
            # Пока на минте мало покупателей — кластер невозможен, RPC не жжём.
            # Когда дойдём до порога — добираем stub'ы без funder (backfill).
            n_after = len(st.buyers) + 1
            if n_after < min_buyers:
                origin = WalletOrigin(wallet=wallet)
                st.register(origin)
                return origin

            useful = self._rpc_resolves_per_mint[mint]
            attempts = self._rpc_attempts_per_mint[mint]
            if (
                useful >= settings.INSIDER_MAX_RPC_PER_MINT
                or attempts >= settings.INSIDER_MAX_RPC_ATTEMPTS_PER_MINT
            ):
                origin = WalletOrigin(wallet=wallet)
                st.register(origin)
                return origin
            if not self.rpc_available:
                origin = WalletOrigin(wallet=wallet)
                st.register(origin)
                return origin

            # Backfill: stubs, зарегистрированные до порога покупателей.
            await self._backfill_stubs(mint, now)

            origin = await self.resolve_wallet(wallet, now)
            self._rpc_attempts_per_mint[mint] += 1
            if not origin.capped and (origin.funder or origin.birth_unix is not None):
                self._rpc_resolves_per_mint[mint] += 1
        else:
            origin = cached

        # Хаб: глобальный fanout из БД (работает и на cache-hit)
        if self._is_hub_funder(origin.funder):
            origin = WalletOrigin(
                wallet=origin.wallet,
                birth_unix=origin.birth_unix,
                funder=None,
                n_sigs=origin.n_sigs,
                capped=origin.capped,
                age_hours=origin.age_hours,
            )
        st.register(origin)

        self._register_count += 1
        if self._register_count % 200 == 0:
            self.prune()
        return origin

    async def _backfill_stubs(self, mint: str, now: float) -> None:
        """Резолвит покупателей, которых записали stub'ом до порога RPC."""
        st = self._mints.get(mint)
        if not st:
            return
        stubs = [
            w for w, o in st.buyers.items()
            if o.funder is None and o.birth_unix is None and not o.capped and o.n_sigs == 0
        ]
        for w in stubs:
            useful = self._rpc_resolves_per_mint[mint]
            attempts = self._rpc_attempts_per_mint[mint]
            if (
                useful >= settings.INSIDER_MAX_RPC_PER_MINT
                or attempts >= settings.INSIDER_MAX_RPC_ATTEMPTS_PER_MINT
                or not self.rpc_available
            ):
                break
            # убрать stub, чтобы resolve записал заново
            st.buyers.pop(w, None)
            for f, ws in list(st.by_funder.items()):
                ws.discard(w)
                if not ws:
                    st.by_funder.pop(f, None)
            origin = await self.resolve_wallet(w, now)
            self._rpc_attempts_per_mint[mint] += 1
            if not origin.capped and (origin.funder or origin.birth_unix is not None):
                self._rpc_resolves_per_mint[mint] += 1
            if self._is_hub_funder(origin.funder):
                origin = WalletOrigin(
                    wallet=origin.wallet,
                    birth_unix=origin.birth_unix,
                    funder=None,
                    n_sigs=origin.n_sigs,
                    capped=origin.capped,
                    age_hours=origin.age_hours,
                )
            st.register(origin)

    def cluster_for_mint(self, mint: str) -> tuple[Optional[str], int]:
        st = self._mints.get(mint)
        if not st:
            return None, 0
        return st.best_shared_funder()

    def prune(self, max_mints: int = 3000, max_age_sec: float = 6 * 3600):
        """Режет разросшиеся _mints / resolves (в отличие от AccumulationDetector раньше не чистились)."""
        now = time.time()
        stale = [m for m, st in self._mints.items() if now - st.last_touch > max_age_sec]
        for m in stale:
            self._mints.pop(m, None)
            self._rpc_resolves_per_mint.pop(m, None)
            self._rpc_attempts_per_mint.pop(m, None)
        if len(self._mints) > max_mints:
            ordered = sorted(self._mints.items(), key=lambda kv: kv[1].last_touch)
            for m, _ in ordered[: len(self._mints) - max_mints]:
                self._mints.pop(m, None)
                self._rpc_resolves_per_mint.pop(m, None)
                self._rpc_attempts_per_mint.pop(m, None)
        # fanout_cache можно подрезать, если раздулся
        if len(self._fanout_cache) > 50_000:
            self._fanout_cache.clear()

    async def _rpc(self, method: str, params: list) -> Optional[object]:
        if not self._gate.allow():
            return None
        body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        async with self._sem:
            for attempt in range(2):
                try:
                    async with self._session.post(settings.helius_rpc(), json=body, timeout=12) as resp:
                        if resp.status == 429:
                            self._gate.note_429()
                            return None
                        if resp.status != 200:
                            self._gate.note_error(f"{method} HTTP {resp.status}")
                            return None
                        data = await resp.json()
                        return data.get("result")
                except Exception:  # noqa: BLE001
                    await asyncio.sleep(0.5 * (attempt + 1))
        return None

    async def _fetch_origin(self, wallet: str, now: float) -> WalletOrigin:
        """Рождение + funder.

        getSignatures отдаёт СВЕЖИЕ первыми. limit=100:
          • <100 → нашли birth в одной странице;
          • =100 и oldest.blockTime уже старше MAX_AGE → точно не fresh,
            capped без дальнейших RPC;
          • =100 и oldest ещё «свежий» → пагинируем (шумная молодая когорта),
            иначе раньше теряли инсайдер-фермы с >100 tx за сутки.
        """
        page_limit = 100
        max_pages = max(1, int(settings.INSIDER_MAX_SIG_PAGES))
        max_age_h = float(settings.INSIDER_MAX_WALLET_AGE_HOURS)

        sigs = await self._rpc(
            "getSignaturesForAddress", [wallet, {"limit": page_limit}],
        )
        if not sigs:
            self._resolve_empty += 1
            self._maybe_log_capped_ratio()
            return WalletOrigin(wallet=wallet, n_sigs=0)

        n_total = len(sigs)
        oldest = sigs[-1]
        pages = 1

        while len(sigs) >= page_limit and pages < max_pages:
            oldest_bt = oldest.get("blockTime")
            if oldest_bt is not None and (now - oldest_bt) / 3600 > max_age_h:
                # 100-я свежайшая tx уже старше окна — birth ещё старше.
                self._resolve_capped += 1
                self._maybe_log_capped_ratio()
                return WalletOrigin(wallet=wallet, n_sigs=n_total, capped=True)

            more = await self._rpc(
                "getSignaturesForAddress",
                [wallet, {"limit": page_limit, "before": oldest["signature"]}],
            )
            if not more:
                self._resolve_capped += 1
                self._maybe_log_capped_ratio()
                return WalletOrigin(wallet=wallet, n_sigs=n_total, capped=True)
            n_total += len(more)
            oldest = more[-1]
            sigs = more
            pages += 1
            if len(more) < page_limit:
                break
        else:
            # Исчерпали страницы, всё ещё полная пачка → не знаем birth.
            if len(sigs) >= page_limit:
                self._resolve_capped += 1
                self._maybe_log_capped_ratio()
                return WalletOrigin(wallet=wallet, n_sigs=n_total, capped=True)

        birth = oldest.get("blockTime")
        age_h = ((now - birth) / 3600) if birth else None
        if age_h is not None and age_h > max_age_h:
            # Birth найден, но кошелёк старше окна — в кластер не пойдёт,
            # funder не тратим (экономия 1 RPC).
            self._resolve_capped += 1
            self._maybe_log_capped_ratio()
            return WalletOrigin(
                wallet=wallet, birth_unix=birth, n_sigs=n_total,
                capped=True, age_hours=age_h,
            )

        funder = None
        tx = await self._rpc(
            "getTransaction",
            [oldest["signature"], {
                "encoding": "jsonParsed",
                "maxSupportedTransactionVersion": 0,
                "commitment": "confirmed",
            }],
        )
        if tx:
            try:
                msg = tx["transaction"]["message"]
                for ix in msg.get("instructions", []):
                    info = (ix.get("parsed") or {}).get("info") or {}
                    if info.get("destination") == wallet and info.get("source"):
                        cand = _sanitize_funder(info["source"])
                        if cand:
                            funder = cand
                            break
                if funder is None:
                    keys = msg.get("accountKeys", [])
                    if keys:
                        first = keys[0]
                        pk = first.get("pubkey") if isinstance(first, dict) else first
                        if pk and pk != wallet:
                            funder = _sanitize_funder(pk)
            except Exception:  # noqa: BLE001
                pass

        self._resolve_ok += 1
        self._maybe_log_capped_ratio()
        return WalletOrigin(
            wallet=wallet, birth_unix=birth, funder=funder,
            n_sigs=n_total, capped=False, age_hours=age_h,
        )

    def _maybe_log_capped_ratio(self) -> None:
        total = self._resolve_ok + self._resolve_capped + self._resolve_empty
        if total < 50:
            return
        now = time.time()
        if now - self._last_capped_log < 300:
            return
        self._last_capped_log = now
        capped_pct = 100.0 * self._resolve_capped / total
        logger.info(
            "Funding resolve stats: total=%d ok=%d capped=%d (%.0f%%) empty=%d — "
            "кластер только uncapped; quota=useful≤%d attempts≤%d/mint; "
            "budget≈%d wallets/min",
            total, self._resolve_ok, self._resolve_capped, capped_pct,
            self._resolve_empty,
            settings.INSIDER_MAX_RPC_PER_MINT,
            settings.INSIDER_MAX_RPC_ATTEMPTS_PER_MINT,
            max(1, settings.HELIUS_RPC_MAX_PER_MIN // 2),
        )
