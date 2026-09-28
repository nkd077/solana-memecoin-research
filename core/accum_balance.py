"""
lab_accum_balance — накопление по балансам топ-держателей (состояние).

Спека: docs/specs/lab_accum_balance.md (зафиксирована до данных).

conc = сумма абсолютных балансов топ-N после исключений (не доля supply).
d_conc_pct = (bal_now − bal_prev) / bal_prev  — supply-независимо.
Supply кэшируется один раз: только для исключения доли >50% и отчёта.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from config import settings
from core.helius_gate import get_helius_gate
from core.pump_curve import derive_curve_address
from core.rug_checker import COMMITMENT

logger = logging.getLogger("sniper.accum_balance")

LAB_TRACK = "lab_accum_balance"

# Владелец PDA пула — программа AMM (не Token Program).
AMM_PROGRAM_IDS = frozenset({
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",  # Raydium AMM v4
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",  # Raydium CLMM
    "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",  # Raydium CPMM
    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",  # PumpSwap / Pump AMM
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",  # Pump.fun curve (на всякий)
})

BURN_ADDRESSES = frozenset({
    "1nc1nerator11111111111111111111111111111111",
    "11111111111111111111111111111111",
    "deaddeaddeaddeaddeaddeaddeaddeaddeaddead",
    "BurnBurnBurnBurnBurnBurnBurnBurnBurnBurnBu",
})


@dataclass
class MintMeta:
    pool_account: Optional[str] = None
    pool_resolved: bool = False
    supply: Optional[float] = None
    last_signal_at: float = 0.0
    snaps: list[dict] = field(default_factory=list)
    active: bool = False
    admitted_ts: float = 0.0
    admit_reason: str = ""
    dropped_ts: float = 0.0
    drop_reason: str = ""
    pending_drop_reason: str = ""
    completed_windows: int = 0
    snaps_since_admit: int = 0
    liq_below_streak: int = 0


@dataclass
class WindowObs:
    mint: str
    ts: float
    is_signal: bool
    void: bool
    void_reason: str
    conc_now: float
    conc_prev: float
    d_conc_pct: float
    d_conc_usd: float
    price_now: float
    d_price_pct: float
    liq_usd: float
    top_n_used: int
    excluded_accounts: list[str]
    monotone_ok: bool
    window_snapshots: int
    window_hours: float
    holders_count: int
    pool_account: Optional[str]
    supply: Optional[float]
    top_retention_min: float = 1.0
    top_retention_mean: float = 1.0
    persist_share: float = 1.0
    d_persist: float = 0.0
    d_in: float = 0.0
    d_out: float = 0.0
    top_floor: float = 0.0


class AccumBalanceStore:
    """Мета (кэш пула/supply + хвост окна) + jsonl снимков."""

    def __init__(self, data_dir: Optional[Path] = None):
        root = Path(data_dir or settings.DATA_DIR)
        self.meta_path = root / "accum_balance_meta.json"
        self.snaps_path = root / "accum_balance_snaps.jsonl"
        self.windows_path = root / "accum_balance_windows.jsonl"
        self._meta: dict[str, MintMeta] = {}
        self.cursor = 0
        self._load()

    @staticmethod
    def _window_keep() -> int:
        # В мете — только окно (+1 запас на стык), история в snaps.jsonl.
        return max(int(settings.ACCUM_BAL_WINDOW_SNAPS), 4) + 1

    @staticmethod
    def _hydrate_from_snaps(st: MintMeta) -> None:
        """Подтянуть pool/supply из снимков, если топ-уровень меты пуст."""
        for s in reversed(st.snaps):
            if st.pool_account is None and s.get("pool_account"):
                st.pool_account = s.get("pool_account")
                st.pool_resolved = True
            if st.supply is None and s.get("supply") is not None:
                try:
                    st.supply = float(s["supply"])
                except (TypeError, ValueError):
                    pass
            if st.pool_account is not None and st.supply is not None:
                break
        # Пул искали, но не нашли — тоже кэшируем, чтобы не долбить RPC.
        if st.snaps and not st.pool_resolved and st.pool_account is None:
            # если в снимках явно null после резолва — считать resolved
            if any("pool_account" in s for s in st.snaps):
                st.pool_resolved = True

    def _load(self) -> None:
        if self.meta_path.exists():
            try:
                raw = json.loads(self.meta_path.read_text(encoding="utf-8"))
                if not isinstance(raw, dict):
                    raw = {}
                # Служебное отдельно от мятов (не путать с записями mint → dict).
                svc = raw.get("_svc") if isinstance(raw.get("_svc"), dict) else {}
                self.cursor = int(
                    svc.get("cursor")
                    or raw.get("_cursor")  # legacy flat key
                    or 0
                )
                # Новая схема: {"_svc":..., "mints":{mint: {...}}}
                # Старая: {mint: {...}, "_cursor": N}
                mint_map = raw.get("mints") if isinstance(raw.get("mints"), dict) else raw
                keep = self._window_keep()
                for mint, m in mint_map.items():
                    if not isinstance(mint, str) or mint.startswith("_"):
                        continue
                    if mint in ("mints",) or not isinstance(m, dict):
                        continue
                    # Запись мята должна иметь хотя бы один из известных ключей
                    if not any(k in m for k in ("snaps", "pool_account", "pool_resolved", "supply")):
                        continue
                    snaps = list(m.get("snaps") or [])[-keep:]
                    active = bool(m.get("active"))
                    dropped_ts = float(m.get("dropped_ts") or 0)
                    if not active and snaps and dropped_ts <= 0:
                        active = True
                    admit_reason = str(m.get("admit_reason") or "")
                    if active and snaps and not admit_reason:
                        admit_reason = "legacy_pre_active_set"
                    st = MintMeta(
                        pool_account=m.get("pool_account"),
                        pool_resolved=bool(m.get("pool_resolved")),
                        supply=m.get("supply"),
                        last_signal_at=float(m.get("last_signal_at") or 0),
                        snaps=snaps,
                        active=active,
                        admitted_ts=float(m.get("admitted_ts") or (float(snaps[0].get("ts")) if snaps else 0)),
                        admit_reason=admit_reason,
                        dropped_ts=dropped_ts,
                        drop_reason=str(m.get("drop_reason") or ""),
                        pending_drop_reason=str(m.get("pending_drop_reason") or ""),
                        completed_windows=int(m.get("completed_windows") or 0),
                        snaps_since_admit=int(m.get("snaps_since_admit") or len(snaps)),
                        liq_below_streak=int(m.get("liq_below_streak") or 0),
                    )
                    self._hydrate_from_snaps(st)
                    self._meta[mint] = st
            except Exception:  # noqa: BLE001
                logger.warning("accum_balance meta load failed", exc_info=True)
        # Догрузить хвост из jsonl если meta пустая/устарела
        if self.snaps_path.exists():
            by_mint: dict[str, list] = {}
            try:
                with open(self.snaps_path, encoding="utf-8") as fh:
                    for line in fh:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            s = json.loads(line)
                        except Exception:  # noqa: BLE001
                            continue
                        m = s.get("mint")
                        if m:
                            by_mint.setdefault(m, []).append(s)
                keep = self._window_keep()
                for mint, snaps in by_mint.items():
                    snaps = sorted(snaps, key=lambda x: float(x.get("ts") or 0))[-keep:]
                    st = self._meta.get(mint) or MintMeta()
                    if len(snaps) >= len(st.snaps):
                        st.snaps = snaps
                    self._hydrate_from_snaps(st)
                    self._meta[mint] = st
            except Exception:  # noqa: BLE001
                logger.debug("accum_balance snaps reload failed", exc_info=True)
        n_pool = sum(1 for st in self._meta.values() if st.pool_account)
        logger.info(
            "AccumBalanceStore: mints=%d with_pool=%d cursor=%d",
            len(self._meta), n_pool, self.cursor,
        )

    def save_meta(self) -> None:
        keep = self._window_keep()
        mints: dict[str, Any] = {}
        for mint, st in self._meta.items():
            # Не раздувать мету пустыми слотами от обхода курсора.
            # Active admits без снимков — храним (иначе active-set теряется на рестарте).
            if (
                not st.snaps
                and not st.pool_resolved
                and st.supply is None
                and not st.active
                and not st.admitted_ts
            ):
                continue
            mints[mint] = {
                "pool_account": st.pool_account,
                "pool_resolved": st.pool_resolved,
                "supply": st.supply,
                "last_signal_at": st.last_signal_at,
                "snaps": [slim_snap_for_meta(s) for s in st.snaps[-keep:]],
                "active": st.active,
                "admitted_ts": st.admitted_ts,
                "admit_reason": st.admit_reason,
                "dropped_ts": st.dropped_ts,
                "drop_reason": st.drop_reason,
                "pending_drop_reason": st.pending_drop_reason,
                "completed_windows": st.completed_windows,
                "snaps_since_admit": st.snaps_since_admit,
                "liq_below_streak": st.liq_below_streak,
            }
        payload: dict[str, Any] = {
            "_svc": {"cursor": int(getattr(self, "cursor", 0) or 0)},
            "mints": mints,
        }
        self.meta_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.meta_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload), encoding="utf-8")
        tmp.replace(self.meta_path)

    def load_cursor(self) -> int:
        return int(getattr(self, "cursor", 0) or 0)

    def iter_mints(self):
        """Безопасный обход: только MintMeta, без служебных ключей."""
        for mint, st in self._meta.items():
            if isinstance(st, MintMeta):
                yield mint, st

    def iter_active_mints(self):
        for mint, st in self.iter_mints():
            if st.active:
                yield mint, st

    def peek(self, mint: str) -> Optional[MintMeta]:
        """Без создания пустой записи (для курсора / due-check)."""
        return self._meta.get(mint)

    def get(self, mint: str) -> MintMeta:
        if mint not in self._meta:
            self._meta[mint] = MintMeta()
        return self._meta[mint]

    def admit(self, mint: str, *, ts: float, reason: str) -> MintMeta:
        st = self.get(mint)
        st.active = True
        st.admitted_ts = ts
        st.admit_reason = reason
        st.dropped_ts = 0.0
        st.drop_reason = ""
        st.pending_drop_reason = ""
        st.completed_windows = 0
        st.snaps_since_admit = 0
        st.liq_below_streak = 0
        st.snaps = []
        return st

    def drop(self, mint: str, *, ts: float, reason: str) -> None:
        st = self.get(mint)
        st.active = False
        st.dropped_ts = ts
        st.drop_reason = reason
        st.pending_drop_reason = ""

    @staticmethod
    def has_incomplete_window(st: MintMeta) -> bool:
        if not st.snaps:
            return False
        need = int(settings.ACCUM_BAL_WINDOW_SNAPS)
        return 0 < len(st.snaps) < need

    def append_snap(self, snap: dict) -> None:
        self.snaps_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.snaps_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(snap) + "\n")
        st = self.get(snap["mint"])
        # Синхронизировать кэш пула/supply со снимком (на случай рассинхрона).
        if snap.get("pool_account") and not st.pool_account:
            st.pool_account = snap["pool_account"]
            st.pool_resolved = True
        if snap.get("supply") is not None and st.supply is None:
            try:
                st.supply = float(snap["supply"])
            except (TypeError, ValueError):
                pass
        if "pool_account" in snap:
            st.pool_resolved = True
        st.snaps.append(snap)
        st.snaps = st.snaps[-self._window_keep():]

    def append_window(self, obs: WindowObs) -> None:
        self.windows_path.parent.mkdir(parents=True, exist_ok=True)
        rec = {
            "mint": obs.mint,
            "ts": obs.ts,
            "is_signal": obs.is_signal,
            "void": obs.void,
            "void_reason": obs.void_reason,
            "conc_now": obs.conc_now,
            "conc_prev": obs.conc_prev,
            "d_conc_pct": obs.d_conc_pct,
            "d_conc_usd": obs.d_conc_usd,
            "price_now": obs.price_now,
            "d_price_pct": obs.d_price_pct,
            "liq_usd": obs.liq_usd,
            "top_n_used": obs.top_n_used,
            "excluded_accounts": obs.excluded_accounts,
            "monotone_ok": obs.monotone_ok,
            "window_snapshots": obs.window_snapshots,
            "window_hours": round(obs.window_hours, 3),
            "holders_count": obs.holders_count,
            "pool_account": obs.pool_account,
            "supply": obs.supply,
            "top_retention_min": round(obs.top_retention_min, 4),
            "top_retention_mean": round(obs.top_retention_mean, 4),
            "persist_share": round(obs.persist_share, 4),
            "d_persist": obs.d_persist,
            "d_in": obs.d_in,
            "d_out": obs.d_out,
            "top_floor": obs.top_floor,
            "lab_track": LAB_TRACK,
        }
        with open(self.windows_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec) + "\n")


async def _rpc(session, method: str, params: list) -> Optional[dict]:
    gate = get_helius_gate()
    await gate.acquire()
    url = settings.helius_rpc()
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    try:
        async with session.post(url, json=payload, timeout=25) as resp:
            if resp.status == 429:
                gate.note_429()
                return None
            if resp.status != 200:
                gate.note_error(f"{method} HTTP {resp.status}")
                return None
            data = await resp.json()
            if data.get("error"):
                gate.note_error(str(data["error"])[:120])
                return None
            return data
    except Exception as exc:  # noqa: BLE001
        gate.note_error(str(exc)[:120])
        logger.debug("RPC %s failed: %s", method, exc)
        return None


async def fetch_largest_accounts(session, mint: str) -> Optional[list[dict]]:
    data = await _rpc(session, "getTokenLargestAccounts", [mint, {"commitment": COMMITMENT}])
    if not data:
        return None
    return list((data.get("result") or {}).get("value") or [])


async def fetch_supply(session, mint: str) -> Optional[float]:
    data = await _rpc(session, "getTokenSupply", [mint, {"commitment": COMMITMENT}])
    if not data:
        return None
    val = (data.get("result") or {}).get("value") or {}
    ui = val.get("uiAmount")
    if ui is None:
        return None
    return float(ui)


async def _multiple_accounts(session, addresses: list[str]) -> list[Optional[dict]]:
    if not addresses:
        return []
    data = await _rpc(
        session,
        "getMultipleAccounts",
        [addresses, {"encoding": "jsonParsed", "commitment": COMMITMENT}],
    )
    if not data:
        return [None] * len(addresses)
    return list((data.get("result") or {}).get("value") or [])


async def resolve_token_owners(session, addresses: list[str]) -> dict[str, str]:
    """Адрес токен-аккаунта → owner (кошелёк/PDA). Как rug_checker._resolve_owners."""
    owners: dict[str, str] = {}
    values = await _multiple_accounts(session, addresses)
    for addr, val in zip(addresses, values):
        if not val:
            continue
        info = ((val.get("data") or {}).get("parsed") or {}).get("info") or {}
        if info.get("owner"):
            owners[addr] = info["owner"]
    return owners


async def resolve_account_programs(session, addresses: list[str]) -> dict[str, str]:
    """Адрес → программа-владелец аккаунта (top-level owner)."""
    progs: dict[str, str] = {}
    values = await _multiple_accounts(session, addresses)
    for addr, val in zip(addresses, values):
        if not val:
            continue
        own = val.get("owner")
        if own:
            progs[addr] = own
    return progs


async def resolve_pool_account(session, mint: str, accounts: list[dict]) -> Optional[str]:
    """
    Пул = токен-аккаунт из топа, чей token-owner (PDA) принадлежит программе AMM.
    Не нашли → None (void по спеке). Один раз на mint.
    """
    addrs = [a.get("address") for a in accounts if a.get("address")]
    if not addrs:
        return None
    curve = derive_curve_address(mint)
    token_owners = await resolve_token_owners(session, addrs)
    # Кривая Pump — тоже «пул» до миграции
    for addr, own in token_owners.items():
        if curve and own == curve:
            return addr
    unique = list({o for o in token_owners.values() if o})
    programs = await resolve_account_programs(session, unique)
    for addr, own in token_owners.items():
        prog = programs.get(own)
        if prog and prog in AMM_PROGRAM_IDS:
            return addr
    return None


def compute_conc(
    accounts: list[dict],
    *,
    pool_account: Optional[str],
    supply: Optional[float],
    top_n: int,
    exclude_share: float,
) -> tuple[float, int, list[str], list[list]]:
    """
    Возвращает (conc_abs, holders_used, excluded, top_accounts).
    top_accounts = [[addr, uiAmount], ...] в порядке топа.
    """
    excluded: list[str] = []
    kept: list[tuple[str, float]] = []
    for a in accounts[: max(top_n * 2, top_n)]:
        addr = a.get("address") or ""
        bal = float(a.get("uiAmount") or 0)
        if not addr or bal <= 0:
            continue
        if pool_account and addr == pool_account:
            excluded.append(addr)
            continue
        if addr in BURN_ADDRESSES:
            excluded.append(addr)
            continue
        if supply and supply > 0 and (bal / supply) > exclude_share:
            excluded.append(addr)
            continue
        kept.append((addr, bal))
        if len(kept) >= top_n:
            break
    conc = sum(b for _, b in kept)
    return conc, len(kept), excluded, [[addr, bal] for addr, bal in kept]


def top_addr_set(top_accounts: list) -> set[str]:
    """Адреса из top_accounts: поддержка [addr, bal] и legacy [addr]."""
    out: set[str] = set()
    for item in top_accounts or []:
        if isinstance(item, (list, tuple)) and item:
            out.add(str(item[0]))
        elif isinstance(item, str):
            out.add(item)
    return out


def top_bal_map(top_accounts: list) -> dict[str, float]:
    """addr → balance. Legacy address-only → 0.0."""
    out: dict[str, float] = {}
    for item in top_accounts or []:
        if isinstance(item, (list, tuple)) and item:
            addr = str(item[0])
            bal = float(item[1]) if len(item) > 1 else 0.0
            out[addr] = bal
        elif isinstance(item, str):
            out[item] = 0.0
    return out


def top_retention(prev_top: list, now_top: list) -> float:
    """Доля адресов предыдущего топа, оставшихся в текущем."""
    prev_set = top_addr_set(prev_top)
    now_set = top_addr_set(now_top)
    if not prev_set:
        return 1.0 if not now_set else 0.0
    return len(prev_set & now_set) / len(prev_set)


def decompose_conc_delta(prev_top: list, now_top: list) -> dict[str, float]:
    """
    Δconc = d_persist + d_in - d_out
    persist_share = |d_persist| / (|d_persist|+|d_in|+|d_out|)
    """
    prev_m = top_bal_map(prev_top)
    now_m = top_bal_map(now_top)
    shared = set(prev_m) & set(now_m)
    entered = set(now_m) - set(prev_m)
    exited = set(prev_m) - set(now_m)
    d_persist = sum(now_m[a] - prev_m[a] for a in shared)
    d_in = sum(now_m[a] for a in entered)
    d_out = sum(prev_m[a] for a in exited)
    denom = abs(d_persist) + abs(d_in) + abs(d_out)
    persist_share = (abs(d_persist) / denom) if denom > 0 else 1.0
    return {
        "d_persist": d_persist,
        "d_in": d_in,
        "d_out": d_out,
        "persist_share": persist_share,
        "top_floor_prev": min(prev_m.values()) if prev_m else 0.0,
        "top_floor_now": min(now_m.values()) if now_m else 0.0,
    }


def slim_snap_for_meta(snap: dict) -> dict:
    """В meta — адреса без балансов (балансы живут в jsonl)."""
    s = {k: v for k, v in snap.items() if k != "top_accounts"}
    addrs: list[str] = []
    for item in snap.get("top_accounts") or []:
        if isinstance(item, (list, tuple)) and item:
            addrs.append(str(item[0]))
        elif isinstance(item, str):
            addrs.append(item)
    s["top_accounts"] = addrs
    return s


def evaluate_window(snaps: list[dict], meta: MintMeta) -> WindowObs:
    """Окно из ровно WINDOW_SNAPS снимков. Иначе void."""
    n = int(settings.ACCUM_BAL_WINDOW_SNAPS)
    snap_sec = max(60, int(settings.ACCUM_BAL_SNAP_SEC))
    max_hours = float(settings.ACCUM_BAL_MAX_WINDOW_HOURS)
    now_ts = float(snaps[-1]["ts"])
    mint = snaps[-1]["mint"]

    def _void(reason: str, window_hours: float = 0.0) -> WindowObs:
        return WindowObs(
            mint=mint, ts=now_ts, is_signal=False, void=True, void_reason=reason,
            conc_now=0, conc_prev=0, d_conc_pct=0, d_conc_usd=0,
            price_now=0, d_price_pct=0,
            liq_usd=0, top_n_used=0, excluded_accounts=[], monotone_ok=False,
            window_snapshots=len(snaps), window_hours=window_hours, holders_count=0,
            pool_account=meta.pool_account, supply=meta.supply,
        )

    if len(snaps) < n:
        return _void("incomplete_window")
    window = snaps[-n:]
    window_hours = (float(window[-1]["ts"]) - float(window[0]["ts"])) / 3600.0

    # Плывущая длительность — отсечь до gap-логики (иначе masked missed_snapshots).
    if window_hours > max_hours:
        return _void("window_too_long", window_hours)

    # Пропуски: больше одного «дырявого» интервала → void
    gaps_bad = 0
    for a, b in zip(window, window[1:]):
        dt = float(b["ts"]) - float(a["ts"])
        if dt > snap_sec * 1.5 or dt < snap_sec * 0.4:
            gaps_bad += 1
    if gaps_bad > 1:
        return _void("missed_snapshots", window_hours)

    if any(s.get("dex_missing") for s in window):
        return _void("dex_missing", window_hours)
    if not meta.pool_resolved or not meta.pool_account:
        return _void("no_pool_account", window_hours)

    min_liq = float(window[0].get("min_liq_usd") or 0)
    for s in window:
        liq = float(s.get("liq_usd") or 0)
        if liq < min_liq:
            return _void("liq_below_threshold", window_hours)

    concs = [float(s.get("conc") or 0) for s in window]
    prices = [float(s.get("price_usd") or 0) for s in window]
    if any(c <= 0 for c in concs) or any(p <= 0 for p in prices):
        return _void("bad_conc_or_price", window_hours)

    conc_prev, conc_now = concs[0], concs[-1]
    # База слишком мала (всё в пуле / пыль) — Δ% не определён осмысленно.
    base_usd = conc_prev * prices[0]
    min_conc_usd = float(
        window[0].get("min_conc_usd")
        or settings.ACCUM_BAL_MIN_CONC_USD
    )
    if base_usd < min_conc_usd:
        return _void("conc_base_too_small", window_hours)

    d_conc = (conc_now - conc_prev) / conc_prev
    d_price = (prices[-1] - prices[0]) / prices[0]
    d_conc_usd = (conc_now - conc_prev) * prices[-1]
    # Абсолютный пол от базы (явление), не от размера позиции.
    frac = float(
        window[-1].get("min_d_conc_base_frac")
        or window[0].get("min_d_conc_base_frac")
        or settings.ACCUM_BAL_MIN_D_CONC_BASE_FRAC
    )
    min_d_usd = base_usd * frac

    monotone_ok = True
    max_drop = float(settings.ACCUM_BAL_MAX_STEP_DROP)
    for a, b in zip(concs, concs[1:]):
        if a > 0 and (b - a) / a < -max_drop:
            monotone_ok = False
            break

    is_signal = (
        d_conc >= float(settings.ACCUM_BAL_MIN_D_CONC)
        and d_conc_usd >= min_d_usd
        and monotone_ok
        and float(settings.ACCUM_BAL_PRICE_MIN) <= d_price <= float(settings.ACCUM_BAL_PRICE_MAX)
    )

    last = window[-1]
    # Стабильность состава топа: |prev ∩ now| / |prev| по шагам окна.
    retentions: list[float] = []
    for a, b in zip(window, window[1:]):
        if b.get("top_retention") is not None:
            retentions.append(float(b["top_retention"]))
            continue
        prev_a = list(a.get("top_accounts") or [])
        now_a = list(b.get("top_accounts") or [])
        if prev_a:
            retentions.append(top_retention(prev_a, now_a))
    if retentions:
        top_ret_min = min(retentions)
        top_ret_mean = sum(retentions) / len(retentions)
    else:
        top_ret_min = 1.0
        top_ret_mean = 1.0

    # Разложение Δconc first→last: persist vs slot churn.
    decomp = decompose_conc_delta(
        list(window[0].get("top_accounts") or []),
        list(window[-1].get("top_accounts") or []),
    )
    top_floor = float(last.get("top_floor") or decomp.get("top_floor_now") or 0.0)

    return WindowObs(
        mint=mint,
        ts=now_ts,
        is_signal=is_signal,
        void=False,
        void_reason="",
        conc_now=conc_now,
        conc_prev=conc_prev,
        d_conc_pct=d_conc,
        d_conc_usd=d_conc_usd,
        price_now=prices[-1],
        d_price_pct=d_price,
        liq_usd=float(last.get("liq_usd") or 0),
        top_n_used=int(last.get("top_n_used") or 0),
        excluded_accounts=list(last.get("excluded") or []),
        monotone_ok=monotone_ok,
        window_snapshots=n,
        window_hours=window_hours,
        holders_count=int(last.get("holders_count") or 0),
        pool_account=meta.pool_account,
        supply=meta.supply,
        top_retention_min=top_ret_min,
        top_retention_mean=top_ret_mean,
        persist_share=float(decomp["persist_share"]),
        d_persist=float(decomp["d_persist"]),
        d_in=float(decomp["d_in"]),
        d_out=float(decomp["d_out"]),
        top_floor=top_floor,
    )
