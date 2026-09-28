"""
Централизованная конфигурация снайпинг-бота.
Все параметры читаются из переменных окружения (.env).
"""
import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


def _get_bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "on")


def _get_float(name: str, default: float) -> float:
    val = os.getenv(name)
    try:
        return float(val) if val is not None else default
    except ValueError:
        return default


def _get_int(name: str, default: int) -> int:
    val = os.getenv(name)
    try:
        return int(val) if val is not None else default
    except ValueError:
        return default


@dataclass
class Settings:
    # --- Режим работы ---
    DRY_RUN: bool = field(default_factory=lambda: _get_bool("DRY_RUN", True))

    # --- RPC / блокчейн ---
    HELIUS_API_KEY: str = field(default_factory=lambda: os.getenv("HELIUS_API_KEY", ""))
    HELIUS_RPC_URL: str = field(default_factory=lambda: os.getenv(
        "HELIUS_RPC_URL", "https://mainnet.helius-rpc.com/?api-key={key}"
    ))
    HELIUS_WS_URL: str = field(default_factory=lambda: os.getenv(
        "HELIUS_WS_URL", "wss://mainnet.helius-rpc.com/?api-key={key}"
    ))

    # --- Кошелёк исполнения ---
    PRIVATE_KEY: str = field(default_factory=lambda: os.getenv("PRIVATE_KEY", ""))

    # --- Внешние API ---
    ARKHAM_API_KEY: str = field(default_factory=lambda: os.getenv("ARKHAM_API_KEY", ""))

    # GMGN закрыт Cloudflare с проверкой TLS/JA3-отпечатка браузера — обычные
    # HTTP/WS клиенты получают HTTP 403 на этапе handshake. Подключение
    # реализовано через curl_cffi (см. core/gmgn_client.py) с имитацией
    # отпечатка Chrome. Реальный WS-эндпоинт и формат запроса реконструированы
    # по опыту сообщества и могут устареть — см. README, раздел про GMGN.
    GMGN_WS_URL: str = field(default_factory=lambda: os.getenv("GMGN_WS_URL", "wss://gmgn.ai/ws"))
    GMGN_API_KEY: str = field(default_factory=lambda: os.getenv("GMGN_API_KEY", ""))
    GMGN_ACCESS_TOKEN: str = field(default_factory=lambda: os.getenv("GMGN_ACCESS_TOKEN", ""))
    GMGN_IMPERSONATE: str = field(default_factory=lambda: os.getenv("GMGN_IMPERSONATE", "chrome124"))
    GMGN_APP_VER: str = field(default_factory=lambda: os.getenv("GMGN_APP_VER", "20260202-10623-98faccb"))
    GMGN_APP_LANG: str = field(default_factory=lambda: os.getenv("GMGN_APP_LANG", "en-US"))
    GMGN_TZ_NAME: str = field(default_factory=lambda: os.getenv("GMGN_TZ_NAME", "Europe/Moscow"))
    GMGN_TZ_OFFSET: str = field(default_factory=lambda: os.getenv("GMGN_TZ_OFFSET", "10800"))
    GMGN_USER_AGENT: str = field(default_factory=lambda: os.getenv(
        "GMGN_USER_AGENT",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    ))
    # Список кошельков, за сделками которых следим (через запятую). Публичного
    # общего потока "все умные деньги сразу" в WS не найдено — канал
    # wallet_trade_data отдаёт события только по заранее заданному списку
    # адресов. Соберите адреса вручную на https://gmgn.ai/rank или через
    # Arkham и впишите сюда.
    GMGN_WATCH_WALLETS: str = field(default_factory=lambda: os.getenv("GMGN_WATCH_WALLETS", ""))
    BIRDEYE_API_KEY: str = field(default_factory=lambda: os.getenv("BIRDEYE_API_KEY", ""))
    DEXSCREENER_BASE_URL: str = field(default_factory=lambda: os.getenv(
        "DEXSCREENER_BASE_URL", "https://api.dexscreener.com/latest/dex"
    ))

    # --- Jupiter / Jito ---
    JUPITER_QUOTE_URL: str = field(default_factory=lambda: os.getenv(
        "JUPITER_QUOTE_URL", "https://lite-api.jup.ag/swap/v1/quote"
    ))
    JUPITER_SWAP_URL: str = field(default_factory=lambda: os.getenv(
        "JUPITER_SWAP_URL", "https://lite-api.jup.ag/swap/v1/swap"
    ))
    JITO_BLOCK_ENGINE_URL: str = field(default_factory=lambda: os.getenv(
        "JITO_BLOCK_ENGINE_URL", "https://mainnet.block-engine.jito.wtf/api/v1/bundles"
    ))
    JITO_TIP_LAMPORTS: int = field(default_factory=lambda: _get_int("JITO_TIP_LAMPORTS", 10_000))

    # --- Redis ---
    REDIS_URL: str = field(default_factory=lambda: os.getenv("REDIS_URL", "redis://localhost:6379/0"))

    # --- SQLite ---
    DB_PATH: Path = field(default_factory=lambda: Path(os.getenv(
        "DB_PATH", str(BASE_DIR / "data" / "sniper.db")
    )))

    # --- Telegram ---
    TELEGRAM_BOT_TOKEN: str = field(default_factory=lambda: os.getenv("TELEGRAM_BOT_TOKEN", ""))
    TELEGRAM_CHAT_ID: str = field(default_factory=lambda: os.getenv("TELEGRAM_CHAT_ID", ""))
    TELEGRAM_APPROVE_REQUIRED: bool = field(default_factory=lambda: _get_bool("TELEGRAM_APPROVE_REQUIRED", False))
    TELEGRAM_APPROVE_TIMEOUT_SEC: int = field(default_factory=lambda: _get_int("TELEGRAM_APPROVE_TIMEOUT_SEC", 60))

    # --- Dashboard ---
    DASHBOARD_ENABLED: bool = field(default_factory=lambda: _get_bool("DASHBOARD_ENABLED", True))
    DASHBOARD_HOST: str = field(default_factory=lambda: os.getenv("DASHBOARD_HOST", "127.0.0.1"))
    DASHBOARD_PORT: int = field(default_factory=lambda: _get_int("DASHBOARD_PORT", 8787))

    # --- Executor ---
    # auto = PumpPortal сначала (bonding curve), при провале Jupiter+Jito
    EXECUTOR_MODE: str = field(default_factory=lambda: os.getenv("EXECUTOR_MODE", "auto"))

    # --- Insider / funding cluster ---
    INSIDER_DETECT_ENABLED: bool = field(default_factory=lambda: _get_bool("INSIDER_DETECT_ENABLED", True))
    INSIDER_MAX_WALLET_AGE_HOURS: float = field(default_factory=lambda: _get_float("INSIDER_MAX_WALLET_AGE_HOURS", 24.0))
    INSIDER_MIN_SHARED_FUNDER: int = field(default_factory=lambda: _get_int("INSIDER_MIN_SHARED_FUNDER", 2))
    INSIDER_MAX_FUNDER_FANOUT: int = field(default_factory=lambda: _get_int("INSIDER_MAX_FUNDER_FANOUT", 25))
    # Если True — insider-кластер может пройти даже при score ниже порога
    # (добавляется boost; при INSIDER_FORCE_PASS ещё и форс-проход порога)
    INSIDER_FORCE_PASS: bool = field(default_factory=lambda: _get_bool("INSIDER_FORCE_PASS", False))
    # После стольких entry_ok по текущей фазе — стоп бумажных входов, считаем lift.
    # v1 = entry_ok_insider (заморожен); v2 = entry_ok_insider_v2 (строже).
    INSIDER_EXPERIMENT_MAX_SIGNALS: int = field(
        default_factory=lambda: _get_int("INSIDER_EXPERIMENT_MAX_SIGNALS", 200)
    )
    INSIDER_EXPERIMENT_PHASE: str = field(
        default_factory=lambda: os.getenv("INSIDER_EXPERIMENT_PHASE", "v2").strip().lower() or "v2"
    )
    # Не birth-snipe: ждать возраст mint с первого увиденного buy.
    INSIDER_MIN_MINT_AGE_SEC: int = field(
        default_factory=lambda: _get_int("INSIDER_MIN_MINT_AGE_SEC", 300)
    )
    # На входе требовать живой рынок (Dex txns); иначе honest=−100%.
    # Для early pump Dex часто пуст — тогда см. INSIDER_CURVE_COUNTS_ALIVE.
    INSIDER_REQUIRE_MARKET_ALIVE: bool = field(
        default_factory=lambda: _get_bool("INSIDER_REQUIRE_MARKET_ALIVE", True)
    )
    # Если Dex молчит, но есть бондинг-кривая + ликвидность — пускать в v2.
    INSIDER_CURVE_COUNTS_ALIVE: bool = field(
        default_factory=lambda: _get_bool("INSIDER_CURVE_COUNTS_ALIVE", True)
    )
    # Выходы только для strategy_tag insider_exp_v2 (score/v1 не трогаем).
    INSIDER_STOP_LOSS_PCT: float = field(
        default_factory=lambda: _get_float("INSIDER_STOP_LOSS_PCT", 0.35)
    )
    INSIDER_PARTIAL_TP_1_MULTIPLE: float = field(
        default_factory=lambda: _get_float("INSIDER_PARTIAL_TP_1_MULTIPLE", 3.0)
    )
    INSIDER_PARTIAL_TP_1_FRACTION: float = field(
        default_factory=lambda: _get_float("INSIDER_PARTIAL_TP_1_FRACTION", 0.4)
    )
    INSIDER_PARTIAL_TP_2_MULTIPLE: float = field(
        default_factory=lambda: _get_float("INSIDER_PARTIAL_TP_2_MULTIPLE", 6.0)
    )
    INSIDER_PARTIAL_TP_2_FRACTION: float = field(
        default_factory=lambda: _get_float("INSIDER_PARTIAL_TP_2_FRACTION", 0.5)
    )
    INSIDER_TRAILING_ACTIVATE_PCT: float = field(
        default_factory=lambda: _get_float("INSIDER_TRAILING_ACTIVATE_PCT", 0.4)
    )
    INSIDER_TRAILING_STOP_PCT: float = field(
        default_factory=lambda: _get_float("INSIDER_TRAILING_STOP_PCT", 0.2)
    )
    INSIDER_MAX_HOLD_MINUTES: int = field(
        default_factory=lambda: _get_int("INSIDER_MAX_HOLD_MINUTES", 45)
    )
    # Paper-демки: при age/alive reject всё равно бумажный вход (только DRY),
    # отдельный тег — не портит счётчик v2 и не идёт в live-решение.
    INSIDER_DEMO_PAPER: bool = field(
        default_factory=lambda: _get_bool("INSIDER_DEMO_PAPER", True)
    )
    INSIDER_DEMO_MAX_SIGNALS: int = field(
        default_factory=lambda: _get_int("INSIDER_DEMO_MAX_SIGNALS", 200)
    )
    INSIDER_DEMO_MIN_LIQUIDITY_USD: float = field(
        default_factory=lambda: _get_float("INSIDER_DEMO_MIN_LIQUIDITY_USD", 50.0)
    )
    # Бюджет Helius RPC: без лимита funding+screener съедают квоту за минуты.
    HELIUS_RPC_MAX_PER_MIN: int = field(default_factory=lambda: _get_int("HELIUS_RPC_MAX_PER_MIN", 20))
    HELIUS_RPC_COOLDOWN_SEC: int = field(default_factory=lambda: _get_int("HELIUS_RPC_COOLDOWN_SEC", 600))
    # Сколько УСПЕШНЫХ uncapped-резолвов на mint (с birth/funder).
    # Capped/empty сюда НЕ входят — иначе 12 активных китов съедают бюджет
    # до появления свежего инсайдера на том же минте.
    INSIDER_MAX_RPC_PER_MINT: int = field(default_factory=lambda: _get_int("INSIDER_MAX_RPC_PER_MINT", 6))
    # Жёсткий потолок ЛЮБЫХ RPC-попыток на mint (capped+empty+ok).
    INSIDER_MAX_RPC_ATTEMPTS_PER_MINT: int = field(
        default_factory=lambda: _get_int("INSIDER_MAX_RPC_ATTEMPTS_PER_MINT", 36)
    )
    # Пагинация getSignatures: если первые 100 tx ещё «свежие» по blockTime —
    # добираем birth (шумная, но молодая когорта), иначе capped без funder.
    INSIDER_MAX_SIG_PAGES: int = field(default_factory=lambda: _get_int("INSIDER_MAX_SIG_PAGES", 3))
    # Live RPC только когда на минте уже ≥N покупателей (иначе бюджет
    # размазывается по монетам, где кластер невозможен). Офлайн-измерение —
    # offline_insider.py, ему этот лимит не нужен.
    INSIDER_MIN_BUYERS_BEFORE_RPC: int = field(
        default_factory=lambda: _get_int("INSIDER_MIN_BUYERS_BEFORE_RPC", 3)
    )
    # Не бить Helius ради «возраста» в скринере (дорого: limit=1000 на каждый wallet)
    WALLET_SCREEN_AGE_RPC: bool = field(default_factory=lambda: _get_bool("WALLET_SCREEN_AGE_RPC", False))

    # --- Три параллельных режима (lab / honest / experiment) ---
    # Lab: register_early_buy всегда до торговых фильтров (в main.py).
    # Honest: ENFORCE_EDGE_GATE режет score-путь даже в DRY_RUN (как live).
    # Experiment: ENTRY_MODE=insider|hybrid открывает бумажные сделки по
    # insider-кластеру без edge (только DRY_RUN); в live edge всегда обязателен.
    # score | insider | hybrid
    ENTRY_MODE: str = field(default_factory=lambda: os.getenv("ENTRY_MODE", "hybrid").strip().lower())
    ENFORCE_EDGE_GATE: bool = field(default_factory=lambda: _get_bool("ENFORCE_EDGE_GATE", True))
    # Зеркальный выход по продаже кита. False = только TP/SL/trailing/max_hold
    # (эксперимент: убрать структурный лаг whale_sell из PnL).
    WHALE_MIRROR_EXIT: bool = field(default_factory=lambda: _get_bool("WHALE_MIRROR_EXIT", True))

    # --- Accumulation shadow-lab (не early sniper) ---
    ACCUM_SHADOW_ENABLED: bool = field(default_factory=lambda: _get_bool("ACCUM_SHADOW_ENABLED", True))
    ACCUM_BUY_ENABLED: bool = field(default_factory=lambda: _get_bool("ACCUM_BUY_ENABLED", False))
    ACCUM_MIN_MINT_AGE_SEC: int = field(default_factory=lambda: _get_int("ACCUM_MIN_MINT_AGE_SEC", 300))
    ACCUM_WINDOW_SEC: int = field(default_factory=lambda: _get_int("ACCUM_WINDOW_SEC", 3600))
    ACCUM_REPEAT_WALLET_BUYS: int = field(default_factory=lambda: _get_int("ACCUM_REPEAT_WALLET_BUYS", 2))
    ACCUM_REPEAT_MIN_SOL_TOTAL: float = field(default_factory=lambda: _get_float("ACCUM_REPEAT_MIN_SOL_TOTAL", 1.0))
    ACCUM_CLUSTER_WALLETS: int = field(default_factory=lambda: _get_int("ACCUM_CLUSTER_WALLETS", 3))
    ACCUM_CLUSTER_MIN_SOL_EACH: float = field(default_factory=lambda: _get_float("ACCUM_CLUSTER_MIN_SOL_EACH", 0.5))
    ACCUM_OUTCOME_DELAY_MIN: int = field(default_factory=lambda: _get_int("ACCUM_OUTCOME_DELAY_MIN", 60))
    CATALYST_DEXSCREENER_ENABLED: bool = field(default_factory=lambda: _get_bool("CATALYST_DEXSCREENER_ENABLED", True))
    CATALYST_MAX_PER_MIN: int = field(default_factory=lambda: _get_int("CATALYST_MAX_PER_MIN", 6))

    # --- Whale-sit / volume-spike shadow (env keys legacy WHALE_SIT_*) ---
    # Меряем разгон (vol+avg), не «кит сидит». lab_track=lab_vol_spike.
    WHALE_SIT_SHADOW_ENABLED: bool = field(
        default_factory=lambda: _get_bool("WHALE_SIT_SHADOW_ENABLED", True)
    )
    WHALE_SIT_BUY_ENABLED: bool = field(
        default_factory=lambda: _get_bool("WHALE_SIT_BUY_ENABLED", False)
    )
    WHALE_SIT_MIN_AGE_SEC: int = field(
        default_factory=lambda: _get_int("WHALE_SIT_MIN_AGE_SEC", 3600)
    )
    WHALE_SIT_MIN_LIQ_USD: float = field(
        default_factory=lambda: _get_float("WHALE_SIT_MIN_LIQ_USD", 5000.0)
    )
    WHALE_SIT_MIN_SOL: float = field(
        default_factory=lambda: _get_float("WHALE_SIT_MIN_SOL", 2.0)
    )
    WHALE_SIT_COOLDOWN_SEC: int = field(
        default_factory=lambda: _get_int("WHALE_SIT_COOLDOWN_SEC", 86400)
    )
    WHALE_SIT_OUTCOME_DELAY_MIN: int = field(
        default_factory=lambda: _get_int("WHALE_SIT_OUTCOME_DELAY_MIN", 1440)
    )
    # Источник: poller по graduates (нужен). WS Pump — опционально и почти пуст.
    WHALE_SIT_POLLER_ENABLED: bool = field(
        default_factory=lambda: _get_bool("WHALE_SIT_POLLER_ENABLED", True)
    )
    WHALE_SIT_WS_ENABLED: bool = field(
        default_factory=lambda: _get_bool("WHALE_SIT_WS_ENABLED", False)
    )
    WHALE_SIT_POLL_SEC: int = field(
        default_factory=lambda: _get_int("WHALE_SIT_POLL_SEC", 300)
    )
    WHALE_SIT_MIN_VOL_M5_USD: float = field(
        default_factory=lambda: _get_float("WHALE_SIT_MIN_VOL_M5_USD", 2000.0)
    )
    # Средняя сделка = vol_m5 / (buys+sells). Число txns само по себе — пыль/боты.
    WHALE_SIT_MIN_AVG_TRADE_USD: float = field(
        default_factory=lambda: _get_float("WHALE_SIT_MIN_AVG_TRADE_USD", 200.0)
    )

    # --- Accum-balance: концентрация топ-держателей (состояние, не активность) ---
    # Спека: docs/specs/lab_accum_balance.md — пороги зафиксированы до данных.
    ACCUM_BAL_ENABLED: bool = field(
        default_factory=lambda: _get_bool("ACCUM_BAL_ENABLED", True)
    )
    ACCUM_BAL_BUY_ENABLED: bool = field(
        default_factory=lambda: _get_bool("ACCUM_BAL_BUY_ENABLED", False)
    )
    ACCUM_BAL_SNAP_SEC: int = field(
        default_factory=lambda: _get_int("ACCUM_BAL_SNAP_SEC", 21600)  # 6ч
    )
    ACCUM_BAL_WINDOW_SNAPS: int = field(
        default_factory=lambda: _get_int("ACCUM_BAL_WINDOW_SNAPS", 4)
    )
    ACCUM_BAL_MIN_D_CONC: float = field(
        default_factory=lambda: _get_float("ACCUM_BAL_MIN_D_CONC", 0.05)
    )
    ACCUM_BAL_MAX_STEP_DROP: float = field(
        default_factory=lambda: _get_float("ACCUM_BAL_MAX_STEP_DROP", 0.01)
    )
    ACCUM_BAL_PRICE_MIN: float = field(
        default_factory=lambda: _get_float("ACCUM_BAL_PRICE_MIN", -0.30)
    )
    ACCUM_BAL_PRICE_MAX: float = field(
        default_factory=lambda: _get_float("ACCUM_BAL_PRICE_MAX", 0.10)
    )
    ACCUM_BAL_LIQ_MULTIPLE: float = field(
        default_factory=lambda: _get_float("ACCUM_BAL_LIQ_MULTIPLE", 100.0)
    )
    ACCUM_BAL_POSITION_SOL: float = field(
        default_factory=lambda: _get_float("ACCUM_BAL_POSITION_SOL", 0.05)
    )
    ACCUM_BAL_COOLDOWN_SEC: int = field(
        default_factory=lambda: _get_int("ACCUM_BAL_COOLDOWN_SEC", 172800)  # 48ч
    )
    ACCUM_BAL_OUTCOME_DELAY_MIN: int = field(
        default_factory=lambda: _get_int("ACCUM_BAL_OUTCOME_DELAY_MIN", 10080)  # 7д
    )
    ACCUM_BAL_TOP_N: int = field(
        default_factory=lambda: _get_int("ACCUM_BAL_TOP_N", 20)
    )
    ACCUM_BAL_EXCLUDE_SHARE: float = field(
        default_factory=lambda: _get_float("ACCUM_BAL_EXCLUDE_SHARE", 0.50)
    )
    # Абсолютный пол базы (USD топ-conc): ниже — void.
    # v2 (2026-09-10): $150 ≈ p25 пилота; $50 ловил шум у порога отсечения.
    ACCUM_BAL_MIN_CONC_USD: float = field(
        default_factory=lambda: _get_float("ACCUM_BAL_MIN_CONC_USD", 150.0)
    )
    # Legacy: прирост ≥ позиция×mult. Заменён на долю базы (ниже).
    ACCUM_BAL_MIN_D_CONC_MULT: float = field(
        default_factory=lambda: _get_float("ACCUM_BAL_MIN_D_CONC_MULT", 1.0)
    )
    # Прирост conc в USD ≥ base_usd × frac (от явления, не от размера позиции).
    ACCUM_BAL_MIN_D_CONC_BASE_FRAC: float = field(
        default_factory=lambda: _get_float("ACCUM_BAL_MIN_D_CONC_BASE_FRAC", 0.05)
    )
    # Макс. длительность окна (ч); длиннее → void window_too_long.
    ACCUM_BAL_MAX_WINDOW_HOURS: float = field(
        default_factory=lambda: _get_float("ACCUM_BAL_MAX_WINDOW_HOURS", 36.0)
    )
    # Ёмкость active-set: сколько mint одновременно держим под 6ч-снимками.
    ACCUM_BAL_ACTIVE_CAP: int = field(
        default_factory=lambda: _get_int("ACCUM_BAL_ACTIVE_CAP", 530)
    )
    # После скольких завершённых окон mint нейтрально архивируется.
    ACCUM_BAL_MAX_COMPLETED_WINDOWS: int = field(
        default_factory=lambda: _get_int("ACCUM_BAL_MAX_COMPLETED_WINDOWS", 2)
    )
    # Если после admission за N снимков не сомкнулось ни одного окна — архивировать.
    ACCUM_BAL_MAX_SNAPS_WITHOUT_WINDOW: int = field(
        default_factory=lambda: _get_int("ACCUM_BAL_MAX_SNAPS_WITHOUT_WINDOW", 6)
    )
    # Два подряд liq<threshold для active mint без незавершённого окна → архив.
    ACCUM_BAL_DROP_LIQ_STREAK: int = field(
        default_factory=lambda: _get_int("ACCUM_BAL_DROP_LIQ_STREAK", 2)
    )
    # Сколько mint снимать за один «шаг» цикла (round-robin); 0 = один.
    ACCUM_BAL_BATCH_PER_LOOP: int = field(
        default_factory=lambda: _get_int("ACCUM_BAL_BATCH_PER_LOOP", 1)
    )

    # --- Smart money feed ---
    SMART_MONEY_FILE: str = field(default_factory=lambda: os.getenv(
        "SMART_MONEY_FILE", str(BASE_DIR / "data" / "smart_money_wallets.txt")
    ))
    SMART_MONEY_SYNC_SEC: int = field(default_factory=lambda: _get_int("SMART_MONEY_SYNC_SEC", 120))

    # --- Health / алерты ---
    HEALTH_ALERT_WS_GAP_SEC: int = field(default_factory=lambda: _get_int("HEALTH_ALERT_WS_GAP_SEC", 90))
    HEALTH_HELIUS_429_THRESHOLD: int = field(default_factory=lambda: _get_int("HEALTH_HELIUS_429_THRESHOLD", 10))

    # --- Live verify ---
    LIVE_VERIFY_MINT: str = field(default_factory=lambda: os.getenv("LIVE_VERIFY_MINT", ""))
    LIVE_VERIFY_SOL: float = field(default_factory=lambda: _get_float("LIVE_VERIFY_SOL", 0.01))

    # --- Логирование ---
    LOG_LEVEL: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))

    # --- Скоринг ---
    SCORE_THRESHOLD: float = field(default_factory=lambda: _get_float("SCORE_THRESHOLD", 0.65))

    # --- Управление рисками ---
    MIN_POSITION_SOL: float = field(default_factory=lambda: _get_float("MIN_POSITION_SOL", 0.01))
    MAX_POSITION_SOL: float = field(default_factory=lambda: _get_float("MAX_POSITION_SOL", 0.1))
    MAX_OPEN_POSITIONS: int = field(default_factory=lambda: _get_int("MAX_OPEN_POSITIONS", 3))
    MAX_POSITIONS_PER_WHALE: int = field(default_factory=lambda: _get_int("MAX_POSITIONS_PER_WHALE", 1))
    # Капитал, от которого считаются процентные лимиты. Без него дневной
    # стоп считался от максимальной одновременной позиции (0.3 SOL), а не от
    # счёта — то есть был в десять раз жёстче задуманного, и две убыточные
    # сделки останавливали торговлю на сутки.
    ACCOUNT_CAPITAL_SOL: float = field(default_factory=lambda: _get_float("ACCOUNT_CAPITAL_SOL", 3.0))
    DAILY_STOP_LOSS_PCT: float = field(default_factory=lambda: _get_float("DAILY_STOP_LOSS_PCT", 0.05))
    MAX_POOL_SHARE_PCT: float = field(default_factory=lambda: _get_float("MAX_POOL_SHARE_PCT", 0.02))
    SLIPPAGE_BPS: int = field(default_factory=lambda: _get_int("SLIPPAGE_BPS", 300))

    # --- Программы Solana, за которыми следим ---
    RAYDIUM_AMM_V4_PROGRAM_ID: str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
    PUMP_FUN_PROGRAM_ID: str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
    WSOL_MINT: str = "So11111111111111111111111111111111111111112"

    # --- Автообнаружение "инсайдеров" напрямую из блокчейна (без GMGN) ---
    # Вместо (или вместе с) заранее известного списка кошельков бот сам читает
    # сделки на Pump.fun через Helius, запоминает ранних покупателей новых
    # токенов и через OUTCOME_CHECK_DELAY_MIN проверяет, выстрелил ли токен.
    # Кошельки с достаточным числом успешных ранних покупок автоматически
    # получают флаг "проверенного" смарт-мани — это и есть self-built список
    # инсайдеров, который со временем становится точнее.
    CHAIN_SCAN_PROGRAM_ID: str = field(default_factory=lambda: os.getenv(
        "CHAIN_SCAN_PROGRAM_ID", "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
    ))
    CHAIN_SCAN_POLL_INTERVAL_SEC: int = field(default_factory=lambda: _get_int("CHAIN_SCAN_POLL_INTERVAL_SEC", 15))
    # Источник событий: подписка (быстро, ~1 сек) или опрос (медленно, ~8-20 сек).
    # Измерение показало, что опоздание стоит примерно 19 процентных пунктов
    # доходности: ранние входы дают -3.3% на сделку, поздние -22%. Опрос
    # оставлен как запасной вариант на случай проблем с подпиской.
    USE_WEBSOCKET_SCANNER: bool = field(default_factory=lambda: _get_bool("USE_WEBSOCKET_SCANNER", True))
    MIN_BUY_SOL_TO_CONSIDER: float = field(default_factory=lambda: _get_float("MIN_BUY_SOL_TO_CONSIDER", 0.3))
    MINT_SEEN_TTL_SEC: int = field(default_factory=lambda: _get_int("MINT_SEEN_TTL_SEC", 24 * 3600))

    # Через сколько минут после первой замеченной покупки проверять исход
    # (вырос ли токен в цене) и обновлять winrate кошелька
    OUTCOME_CHECK_DELAY_MIN: int = field(default_factory=lambda: _get_int("OUTCOME_CHECK_DELAY_MIN", 30))
    OUTCOME_CHECK_INTERVAL_SEC: int = field(default_factory=lambda: _get_int("OUTCOME_CHECK_INTERVAL_SEC", 300))
    # Рост цены от цены входа, который засчитывается как "победа" кошелька
    # Порог «победы» откалиброван по реальному распределению (замер 03.09.2026,
    # 67 токенов): медианная ранняя покупка на Pump.fun теряет 27% за полчаса,
    # в плюсе оказываются ~10%. Планку +200% берут 1.5% — при такой редкости
    # отличить навык от везения на 20 наблюдениях невозможно, и механизм
    # обучения был мёртв. +30% берут 7.5% — достаточно часто, чтобы
    # накапливать сигнал, и достаточно много, чтобы не быть шумом.
    OUTCOME_WIN_THRESHOLD_PCT: float = field(default_factory=lambda: _get_float("OUTCOME_WIN_THRESHOLD_PCT", 0.3))
    # Honest exit: пул должен поглотить позицию (не «≥1 сделка за час»).
    OUTCOME_MIN_EXIT_TXNS: int = field(default_factory=lambda: _get_int("OUTCOME_MIN_EXIT_TXNS", 2))
    OUTCOME_MIN_EXIT_LIQUIDITY_USD: float = field(
        default_factory=lambda: _get_float("OUTCOME_MIN_EXIT_LIQUIDITY_USD", 400.0)
    )
    OUTCOME_EXIT_LIQUIDITY_MULTIPLE: float = field(
        default_factory=lambda: _get_float("OUTCOME_EXIT_LIQUIDITY_MULTIPLE", 20.0)
    )
    # Сколько отслеженных исходов нужно накопить, прежде чем доверять winrate кошелька
    MIN_TRADES_FOR_TRUSTED_WINRATE: int = field(default_factory=lambda: _get_int("MIN_TRADES_FOR_TRUSTED_WINRATE", 20))
    # Порог winrate, начиная с которого кошелёк считается "проверенным" смарт-мани
    # Статус смарт-мани присваивается ОТНОСИТЕЛЬНО базовой частоты, а не по
    # абсолютному числу. Абсолютный порог 0.65 был бессмыслен: на рынке, где
    # в плюс выходит каждый десятый токен, винрейта 65% не бывает ни у кого,
    # и ни один кошелёк не проходил отбор. Правильный вопрос — не «часто ли
    # кошелёк угадывает», а «угадывает ли он ЧАЩЕ СЛУЧАЙНОГО и во сколько раз».
    # Расчёт показал: чтобы стратегия вышла в ноль, нужен отбор примерно втрое
    # лучше случайного, поэтому по умолчанию требуем 2.5x с запасом на шум.
    SMART_MONEY_EDGE_MULTIPLE: float = field(default_factory=lambda: _get_float("SMART_MONEY_EDGE_MULTIPLE", 2.5))
    # Нижняя граница на случай, если базовая частота почему-то не набралась
    SMART_MONEY_MIN_WINRATE: float = field(default_factory=lambda: _get_float("SMART_MONEY_MIN_WINRATE", 0.15))
    # Не рассматривать сделку, если её купило меньше стольких разных кошельков
    # за последние 30 минут (1 = без доп. фильтра, каждая покупка рассматривается)
    MIN_CLUSTER_SIZE_TO_CONSIDER: int = field(default_factory=lambda: _get_int("MIN_CLUSTER_SIZE_TO_CONSIDER", 5))

    # --- Скрининг кошельков против скам/дев-кошельков ---
    # Цель: не просто искать любой ранний "успешный" кошелёк, а отсеивать
    # заведомо нерелевантные/скамерские сигналы, прежде чем вообще давать
    # им повлиять на скоринг или на репутацию "смарт-мани".
    WALLET_SCREENING_ENABLED: bool = field(default_factory=lambda: _get_bool("WALLET_SCREENING_ENABLED", True))
    # Кошелёк младше этого числа дней считается "неподтверждённым" — это не
    # блокирует сделку жёстко, но снижает скор и не даёт кошельку получить
    # статус "проверенного" смарт-мани, пока не накопит историю
    MIN_WALLET_AGE_DAYS: float = field(default_factory=lambda: _get_float("MIN_WALLET_AGE_DAYS", 1.0))
    # Если кошелёк покупает больше стольких РАЗНЫХ новых токенов за 24 часа —
    # это похоже на бота, который покупает всё подряд, а не на селективного
    # инсайдера с инсайдерской информацией. Такой кошелёк никогда не получит
    # статус "проверенного" смарт-мани и получает штраф к скору
    MAX_NEW_MINTS_PER_WALLET_PER_DAY: int = field(default_factory=lambda: _get_int("MAX_NEW_MINTS_PER_WALLET_PER_DAY", 8))
    # Срок кэширования результата проверки истории кошелька (возраст,
    # источник финансирования) — чтобы не дёргать Helius повторно по одному
    # и тому же кошельку на каждой новой покупке
    WALLET_SCREEN_CACHE_TTL_SEC: int = field(default_factory=lambda: _get_int("WALLET_SCREEN_CACHE_TTL_SEC", 24 * 3600))
    # Срок хранения блок-листа скамерских/дев-кошельков — намеренно долгий:
    # однажды пойманный на выпуске своего токена или на финансировании от
    # известного дев-кошелька адрес не должен снова начать влиять на бота
    BLOCKLIST_TTL_SEC: int = field(default_factory=lambda: _get_int("BLOCKLIST_TTL_SEC", 180 * 24 * 3600))
    # --- Проверка исполнимости сделки на бондинг-кривой ---
    # Грубый порог "ликвидность в $" измеряет не то: у Pump.fun цена считается
    # по ВИРТУАЛЬНЫМ резервам (старт ~30 SOL), поэтому проскальзывание на малой
    # позиции мизерное даже у токена с копеечной реальной ликвидностью.
    # Проверяем два реальных вопроса по отдельности:
    #   1) сколько мы потеряем на входе — MAX_ENTRY_SLIPPAGE_PCT
    #   2) сможем ли выйти — реального SOL в кривой должно быть хотя бы
    #      MIN_EXIT_LIQUIDITY_MULTIPLE наших позиций
    MAX_ENTRY_SLIPPAGE_PCT: float = field(default_factory=lambda: _get_float("MAX_ENTRY_SLIPPAGE_PCT", 0.02))
    MIN_EXIT_LIQUIDITY_MULTIPLE: float = field(default_factory=lambda: _get_float("MIN_EXIT_LIQUIDITY_MULTIPLE", 15.0))

    # Минимальная ликвидность пула в $, ниже которой сделка НЕ совершается
    # (в отличие от MIN_BUY_SOL_TO_CONSIDER — это порог для реального входа,
    # а не для отслеживания токена под наблюдением). Слишком низкая ликвидность
    # означает огромный слип на входе и выходе — даже "правильный" сигнал
    # может обернуться потерей просто на исполнении сделки.
    MIN_LIQUIDITY_USD_TO_BUY: float = field(default_factory=lambda: _get_float("MIN_LIQUIDITY_USD_TO_BUY", 300.0))
    # Включить проверку mint/freeze-authority и концентрации держателей
    # перед покупкой (core/rug_checker.py)
    RUG_CHECK_ENABLED: bool = field(default_factory=lambda: _get_bool("RUG_CHECK_ENABLED", True))
    # Winrate, ниже которого кошелёк (накопивший минимум MIN_TRADES_FOR_TRUSTED_WINRATE
    # исходов) автоматически попадает в блок-лист — стабильно убыточный или
    # скам-паттерн, дальше не тратим на него ни скоринг, ни отслеживание
    BAD_WALLET_BLOCKLIST_WINRATE: float = field(default_factory=lambda: _get_float("BAD_WALLET_BLOCKLIST_WINRATE", 0.15))

    # --- Автоматический выход из позиций ---
    # Вместо одного жёсткого тейк-профита — лестница частичных фиксаций:
    # часть позиции продаётся на кратных уровнях (x2, x5 по умолчанию), чтобы
    # закрепить прибыль, а остаток едет дальше БЕЗ потолка — так бот не
    # обрубает потенциальные 20x/100x сам себе. Трейлинг-стоп на остаток
    # включается только после первой частичной фиксации.
    POSITION_CHECK_INTERVAL_SEC: int = field(default_factory=lambda: _get_int("POSITION_CHECK_INTERVAL_SEC", 20))
    STOP_LOSS_PCT: float = field(default_factory=lambda: _get_float("STOP_LOSS_PCT", 0.3))
    PARTIAL_TP_1_MULTIPLE: float = field(default_factory=lambda: _get_float("PARTIAL_TP_1_MULTIPLE", 2.0))
    PARTIAL_TP_1_FRACTION: float = field(default_factory=lambda: _get_float("PARTIAL_TP_1_FRACTION", 0.5))
    PARTIAL_TP_2_MULTIPLE: float = field(default_factory=lambda: _get_float("PARTIAL_TP_2_MULTIPLE", 5.0))
    PARTIAL_TP_2_FRACTION: float = field(default_factory=lambda: _get_float("PARTIAL_TP_2_FRACTION", 0.5))
    TRAILING_ACTIVATE_PCT: float = field(default_factory=lambda: _get_float("TRAILING_ACTIVATE_PCT", 0.3))
    TRAILING_STOP_PCT: float = field(default_factory=lambda: _get_float("TRAILING_STOP_PCT", 0.15))
    MAX_HOLD_MINUTES: int = field(default_factory=lambda: _get_int("MAX_HOLD_MINUTES", 180))

    # --- Пути ---
    DATA_DIR: Path = field(default_factory=lambda: BASE_DIR / "data")
    LOGS_DIR: Path = field(default_factory=lambda: BASE_DIR / "logs")

    def helius_rpc(self) -> str:
        return self.HELIUS_RPC_URL.format(key=self.HELIUS_API_KEY)

    def helius_ws(self) -> str:
        return self.HELIUS_WS_URL.format(key=self.HELIUS_API_KEY)

    @property
    def insider_experiment_decision(self) -> str:
        phase = (self.INSIDER_EXPERIMENT_PHASE or "v2").strip().lower()
        if phase in ("v1", "1", "legacy"):
            return "entry_ok_insider"
        return "entry_ok_insider_v2"

    @property
    def insider_strategy_tag(self) -> str:
        phase = (self.INSIDER_EXPERIMENT_PHASE or "v2").strip().lower()
        if phase in ("v1", "1", "legacy"):
            return "insider_exp"
        return "insider_exp_v2"

    @property
    def insider_lab_track(self) -> str:
        phase = (self.INSIDER_EXPERIMENT_PHASE or "v2").strip().lower()
        if phase in ("v1", "1", "legacy"):
            return "lab_insider"
        return "lab_insider_v2"

    def validate_for_live_trading(self) -> list[str]:
        """Возвращает список проблем, которые нужно исправить перед реальной торговлей."""
        problems = []
        if not self.PRIVATE_KEY:
            problems.append("PRIVATE_KEY не задан")
        if not self.HELIUS_API_KEY:
            problems.append("HELIUS_API_KEY не задан")
        if self.MAX_POSITION_SOL <= 0:
            problems.append("MAX_POSITION_SOL должен быть > 0")
        return problems


settings = Settings()
