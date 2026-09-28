#!/usr/bin/env bash
# Полный статус лаборатории. На «чекай» / «проверь» — этот скрипт.
set -euo pipefail
cd "$(dirname "$0")"
source venv/bin/activate 2>/dev/null || true

python - <<'PY'
from __future__ import annotations

import json
import os
import re
import sqlite3
import subprocess
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(".")
NOW = time.time()


def pgrep(pat: str) -> list[str]:
    try:
        out = subprocess.check_output(["pgrep", "-fl", pat], text=True, stderr=subprocess.DEVNULL)
        return [l for l in out.splitlines() if l.strip()]
    except subprocess.CalledProcessError:
        return []


def alive_pidfile(path: Path) -> tuple[bool, str]:
    if not path.exists():
        return False, "no pidfile"
    try:
        pid = int(path.read_text().strip())
    except Exception:
        return False, "bad pidfile"
    try:
        os.kill(pid, 0)
        return True, str(pid)
    except OSError:
        return False, str(pid)


print("=" * 60)
print("SNIPER STATUS", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
print("=" * 60)

# --- processes ---
bot = pgrep("sniper_bot/main.py") or pgrep("/main.py")
keep = pgrep("keepalive.py")
off = pgrep("offline_insider")
dash = pgrep("dashboard.app")
ok_bot, bot_pid = alive_pidfile(ROOT / "data/sniper_bot.pid")
ok_keep, keep_pid = alive_pidfile(ROOT / "data/keepalive.pid")
ok_off, off_pid = alive_pidfile(ROOT / "data/offline_insider.pid")
off_paused = (ROOT / "data/offline_insider.paused").exists()
# pgrep может врать (sandbox/sysmon) — pidfile + kill -0 надёжнее
if not bot and ok_bot:
    bot = [f"{bot_pid} (pidfile)"]
if not keep and ok_keep:
    keep = [f"{keep_pid} (pidfile)"]
if not off and ok_off:
    off = [f"{off_pid} (pidfile)"]
print("\n[procs]")
print("  bot      ", bot[0] if bot else "DEAD")
print("  keepalive", keep[0] if keep else "DEAD")
if off_paused:
    print("  offline  ", "PAUSED (accum_balance snap pace)" if not off else f"PAUSED but still running {off[0]}")
else:
    print("  offline  ", off[0] if off else "DEAD")
print("  dashboard", dash[0] if dash else "off")

print(f"  offline pidfile={'OK '+off_pid if ok_off else 'DEAD '+off_pid}")

# --- bot log since last start ---
print("\n[bot]")
log = ROOT / "logs/bot_run.log"
if log.exists():
    lines = log.read_text(errors="ignore").splitlines()
    starts = [i for i, l in enumerate(lines) if "Запуск снайпинг-бота" in l]
    if starts:
        post = lines[starts[-1]:]
        m = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", post[0])
        up_m = 0.0
        if m:
            start = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
            up_m = (NOW - time.mktime(start.timetuple())) / 60
            print(f"  start {m.group(1)}  uptime {up_m:.0f}m")
        err = sum(1 for l in post if "ERROR" in l or "Traceback" in l)
        n429 = sum(1 for l in post if "Helius 429" in l)
        print(f"  errors={err}  helius_429={n429}")
        print(
            f"  scoring={sum(1 for l in post if 'Скоринг' in l)}  "
            f"insider_detect_sig={sum(1 for l in post if 'Insider signal' in l)}  "
            f"buys={sum(1 for l in post if 'Сделка выполнена' in l)}"
        )
        print(f"  tail: {post[-1][11:110]}")
    else:
        print("  no start line in log")
else:
    print("  no bot_run.log")

# --- sqlite ---
print("\n[db]")
dbp = ROOT / "data/sniper.db"
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env", override=True)
    from config import settings as s
except Exception:
    s = None
if dbp.exists():
    conn = sqlite3.connect(str(dbp))
    ins_v1 = conn.execute(
        "SELECT COUNT(*) FROM signals WHERE decision='entry_ok_insider'"
    ).fetchone()[0]
    ins_v2 = conn.execute(
        "SELECT COUNT(*) FROM signals WHERE decision='entry_ok_insider_v2'"
    ).fetchone()[0]
    lim = getattr(s, "INSIDER_EXPERIMENT_MAX_SIGNALS", 200) if s else 200
    phase = getattr(s, "INSIDER_EXPERIMENT_PHASE", "?") if s else "?"
    print(f"  entry_ok_insider v1={ins_v1} (frozen)  v2={ins_v2}/{lim} phase={phase}")
    ins_demo = conn.execute(
        "SELECT COUNT(*) FROM signals WHERE decision='entry_ok_insider_demo'"
    ).fetchone()[0]
    demo_lim = getattr(s, "INSIDER_DEMO_MAX_SIGNALS", 200) if s else 200
    print(f"  entry_ok_insider_demo {ins_demo}/{demo_lim} (paper analysis, not v2)")
    cut = NOW - 3600
    rows = conn.execute(
        "SELECT decision, COUNT(*) FROM signals WHERE ts>=? AND decision IN "
        "('entry_ok_insider','entry_ok_insider_v2','entry_ok_insider_demo','entry_ok_score',"
        "'edge_reject_trade','accum_shadow','vol_spike_shadow','whale_sit_shadow',"
        "'accum_balance_signal','insider_experiment_done',"
        "'entry_reject_insider_young','entry_reject_untradeable') GROUP BY 1 ORDER BY 2 DESC",
        (cut,),
    ).fetchall()
    print("  last 1h:", dict(rows) if rows else "{}")
    wf = conn.execute(
        "SELECT COUNT(*), "
        "SUM(CASE WHEN funder IS NOT NULL AND length(funder)>0 THEN 1 ELSE 0 END), "
        "SUM(capped) FROM wallet_funders"
    ).fetchone()
    print(f"  wallet_funders n={wf[0]} with_funder={wf[1]} capped={wf[2]}")
    conn.close()
else:
    print("  no sniper.db")

# --- offline log ---
print("\n[offline]")
olog = ROOT / "logs/offline_insider.log"
if olog.exists():
    olines = olog.read_text(errors="ignore").splitlines()
    prog = [l for l in olines if "ok_rpc=" in l]
    if prog:
        last = prog[-1].strip()
        print(f"  last: {last}")
        m = re.search(r"(\d+)/(\d+)", last)
        if m:
            i, t = int(m.group(1)), int(m.group(2))
            pct = 100 * i / t if t else 0
            eta_h = (t - i) / 10 / 60 if t > i else 0
            print(f"  progress {pct:.2f}%  ETA ~{eta_h:.1f}h (@~10 wallets/min)")
    else:
        print("  no progress lines yet")
    n429 = sum(1 for l in olines if re.search(r"\b429\b", l) and "429=0" not in l)
    print(f"  real_429_lines={n429}")
else:
    print("  no offline log")

mints = ROOT / "data/insider_offline_mints.json"
if mints.exists():
    try:
        data = json.loads(mints.read_text())
        n_cl = sum(1 for v in data.values() if v.get("is_cluster"))
        print(f"  mints_file {len(data)} clusters={n_cl}")
    except Exception as e:
        print(f"  mints_file read err: {e}")

# --- accum_balance ---
print("\n[accum_balance]")
meta_p = ROOT / "data/accum_balance_meta.json"
windows_p = ROOT / "data/accum_balance_windows.jsonl"
if meta_p.exists():
    try:
        meta = json.loads(meta_p.read_text())
        svc = meta.get("_svc") if isinstance(meta.get("_svc"), dict) else {}
        accum_mints = meta.get("mints") if isinstance(meta.get("mints"), dict) else {}
        active = sum(1 for v in accum_mints.values() if isinstance(v, dict) and v.get("active"))
        dropped = sum(1 for v in accum_mints.values() if isinstance(v, dict) and v.get("dropped_ts"))
        compl = sum(int(v.get("completed_windows") or 0) for v in accum_mints.values() if isinstance(v, dict))
        print(f"  mints={len(accum_mints)} active={active} dropped={dropped} cursor={svc.get('cursor')}")
        print(f"  completed_windows={compl}")
    except Exception as e:
        print(f"  meta err: {e}")
else:
    print("  meta missing")
if windows_p.exists():
    try:
        rows = [json.loads(l) for l in windows_p.read_text().splitlines() if l.strip()]
        voids = sum(1 for r in rows if r.get("void"))
        sigs = sum(1 for r in rows if r.get("is_signal"))
        reasons: dict[str, int] = {}
        accum_vals = accum_mints.values() if 'accum_mints' in locals() and isinstance(accum_mints, dict) else []
        for v in accum_vals:
            if not isinstance(v, dict):
                continue
            dr = v.get("drop_reason")
            if dr:
                reasons[dr] = reasons.get(dr, 0) + 1
        print(f"  windows={len(rows)} void={voids} signals={sigs}")
        if reasons:
            print(f"  drop_reasons={reasons}")
    except Exception as e:
        print(f"  windows err: {e}")
else:
    print("  windows 0")

# --- graduates ---
print("\n[graduates]")
cohort_p = ROOT / "data/graduates_cohort.json"
snaps_p = ROOT / "data/graduates_snapshots.jsonl"
feed_p = ROOT / "data/graduations_feed.jsonl"
if cohort_p.exists():
    cohort = json.loads(cohort_p.read_text())
    print(f"  cohort {len(cohort)}")
else:
    print("  cohort missing")
if snaps_p.exists():
    n = sum(1 for _ in snaps_p.open())
    print(f"  snapshots {n}")
else:
    print("  snapshots missing")
if feed_p.exists():
    age_h = (time.time() - feed_p.stat().st_mtime) / 3600
    n_feed = sum(1 for _ in feed_p.open())
    watch = pgrep("watch_graduations.py")
    ok_g, gpid = alive_pidfile(ROOT / "data/grads_feed.pid")
    if not watch and ok_g:
        watch = [f"{gpid} (pidfile)"]
    # Основной писатель ленты — WsScanner в боте; отдельный watch опционален.
    print(f"  feed lines={n_feed} age={age_h:.1f}h  watch={'LIVE' if watch else 'off (bot WS writes)'}")
    if age_h > 6 and not bot:
        print("  WARN: graduations feed stale and bot down — cohort aging")
    elif age_h > 6:
        print("  WARN: feed mtime >6h — жди GRADUATION в bot log или ./run_grads_feed.sh из Terminal")
else:
    print("  feed missing")

# --- config ---
print("\n[config]")
try:
    from dotenv import load_dotenv
    load_dotenv(".env", override=True)
    from config import settings as s
    print(
        f"  DRY={s.DRY_RUN} ENTRY={s.ENTRY_MODE} EDGE={s.ENFORCE_EDGE_GATE} "
        f"RPC={s.HELIUS_RPC_MAX_PER_MIN}/min buyers_rpc≥{s.INSIDER_MIN_BUYERS_BEFORE_RPC} "
        f"shared≥{s.INSIDER_MIN_SHARED_FUNDER} age≥{s.INSIDER_MIN_MINT_AGE_SEC}s "
        f"fanout≤{s.INSIDER_MAX_FUNDER_FANOUT} alive={s.INSIDER_REQUIRE_MARKET_ALIVE}"
    )
except Exception as e:
    print(f"  config err: {e}")

# --- verdict ---
print("\n[verdict]")
problems = []
if not bot:
    problems.append("bot DEAD")
if not keep:
    problems.append("keepalive DEAD")
if not off_paused and not ok_off and not off:
    problems.append("offline_insider DEAD")
if off_paused and off:
    problems.append("offline PAUSED but still running — kill it")
if problems:
    print("  NEED FIX:", "; ".join(problems))
elif off_paused:
    print("  OK — bot + keepalive alive; offline PAUSED")
else:
    print("  OK — bot + keepalive + offline alive")
print("=" * 60)
PY

# авто-подъём упавших сторожей (тихо).
# pgrep в sandbox часто ломается — сначала pidfile + kill -0, иначе дубликаты.
alive_pidfile() {
  local f="$1"
  [[ -f "$f" ]] && kill -0 "$(cat "$f")" 2>/dev/null
}
pgrep_ok() {
  pgrep -f "$1" >/dev/null 2>&1
}
FIX=0
if ! alive_pidfile data/keepalive.pid && ! pgrep_ok 'keepalive.py'; then
  echo "[fix] starting keepalive..."
  ./run_keepalive.sh || true
  FIX=1
fi
if ! alive_pidfile data/offline_insider.pid && ! pgrep_ok 'offline_insider'; then
  if [[ -f data/offline_insider.paused ]]; then
    echo "[fix] offline_insider PAUSED — skip autostart (rm data/offline_insider.paused to resume)"
  else
    echo "[fix] starting offline_insider..."
    ./run_offline_insider.sh || true
    FIX=1
  fi
fi
# watch_graduations из Cursor-шелла часто ломает DNS; ленту пишет WsScanner в боте.
# Ручной запасной канал: ./run_grads_feed.sh из Terminal.app — не авто-старт здесь.
if [[ $FIX -eq 1 ]]; then
  sleep 3
  echo "[fix] re-check procs:"
  pgrep -fl 'keepalive.py|offline_insider|sniper_bot/main|watch_graduations' || true
fi
