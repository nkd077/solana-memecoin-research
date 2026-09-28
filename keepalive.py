"""
Сторож: держит bot + dashboard живыми.

Запуск:
  ./run_keepalive.sh
  # или: python keepalive.py --daemon
"""
from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
VENV_PYTHON = ROOT / "venv" / "bin" / "python"
PYTHON = str(VENV_PYTHON if VENV_PYTHON.exists() else sys.executable)
LOGS = ROOT / "logs"
DATA = ROOT / "data"
PID_FILE = DATA / "keepalive.pid"
BOT_PID_FILE = DATA / "sniper_bot.pid"
CHECK_SEC = 8
START_GRACE_SEC = 10


def daemonize() -> None:
    """Двойной fork — не умирает вместе с Cursor agent-шеллом."""
    DATA.mkdir(parents=True, exist_ok=True)
    if os.fork() > 0:
        sys.exit(0)
    os.setsid()
    if os.fork() > 0:
        sys.exit(0)
    sys.stdout.flush()
    sys.stderr.flush()
    PID_FILE.write_text(str(os.getpid()))


def _pids_matching(pattern: str) -> tuple[list[int], bool]:
    """(pids, pgrep_ok). pgrep_ok=False → sysmon/sandbox сломан, НЕ считать down."""
    try:
        out = subprocess.check_output(
            ["pgrep", "-f", pattern], text=True, stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError as exc:
        # 1 = нет совпадений; иное (напр. sysmon) = инструмент сломан
        if exc.returncode == 1:
            return [], True
        return [], False
    except Exception:  # noqa: BLE001
        return [], False
    me = os.getpid()
    pids = [int(x) for x in out.split() if x.isdigit() and int(x) != me]
    return pids, True


def _cmdline_has(pid: int, needle: str) -> bool:
    try:
        out = subprocess.check_output(
            ["ps", "-p", str(pid), "-o", "command="], text=True, stderr=subprocess.DEVNULL,
        )
    except Exception:  # noqa: BLE001
        return False
    return needle in out


def _pidfile_alive(path: Path, needle: str | None = None) -> bool:
    if not path.exists():
        return False
    try:
        pid = int(path.read_text().strip())
        os.kill(pid, 0)
    except (ValueError, OSError, ProcessLookupError):
        return False
    if needle and not _cmdline_has(pid, needle):
        return False
    return True


def _start(cmd: list[str], log_name: str) -> int:
    LOGS.mkdir(parents=True, exist_ok=True)
    DATA.mkdir(parents=True, exist_ok=True)
    log_path = LOGS / log_name
    logf = open(log_path, "a", encoding="utf-8")
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env["DASHBOARD_ENABLED"] = "false"
    env.setdefault("LOG_LEVEL", "INFO")
    proc = subprocess.Popen(
        cmd,
        cwd=str(ROOT),
        stdout=logf,
        stderr=subprocess.STDOUT,
        env=env,
        start_new_session=True,  # bot/dash переживают смерть keepalive
    )
    print(f"[keepalive] started pid={proc.pid} cmd={' '.join(cmd)}", flush=True)
    return proc.pid


def main():
    print(f"[keepalive] up pid={os.getpid()} — check every {CHECK_SEC}s", flush=True)
    stop = False

    def _stop(*_a):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    bot_pat = "sniper_bot/main.py|[ /]main\\.py"
    dash_pat = "dashboard.app"
    pgrep_warn_at = 0.0

    while not stop:
        try:
            # --- dashboard ---
            dash_pids, dash_ok = _pids_matching(dash_pat)
            if not dash_ok:
                if time.time() - pgrep_warn_at > 60:
                    print("[keepalive] pgrep broken — skip restart (avoid spawn storm)", flush=True)
                    pgrep_warn_at = time.time()
            elif not dash_pids:
                print("[keepalive] dashboard down → restart", flush=True)
                _start([PYTHON, "-u", "-m", "dashboard.app"], "dashboard.log")
                time.sleep(START_GRACE_SEC)

            # --- bot: сначала pidfile (надёжнее pgrep) ---
            if _pidfile_alive(BOT_PID_FILE, "main.py"):
                pass
            else:
                bot_pids, bot_ok = _pids_matching(bot_pat)
                if not bot_ok:
                    if time.time() - pgrep_warn_at > 60:
                        print("[keepalive] pgrep broken + no pidfile — skip bot restart", flush=True)
                        pgrep_warn_at = time.time()
                else:
                    live = [
                        p for p in bot_pids
                        if _cmdline_has(p, "main.py") and not _cmdline_has(p, "keepalive")
                    ]
                    if not live:
                        if BOT_PID_FILE.exists():
                            try:
                                BOT_PID_FILE.unlink()
                            except OSError:
                                pass
                        print("[keepalive] bot down → restart", flush=True)
                        _start([PYTHON, "-u", str(ROOT / "main.py")], "bot_run.log")
                        time.sleep(START_GRACE_SEC)
        except Exception as exc:  # noqa: BLE001
            print(f"[keepalive] error: {exc}", flush=True)
        time.sleep(CHECK_SEC)

    print("[keepalive] exit (children keep running)", flush=True)
    try:
        if PID_FILE.exists() and PID_FILE.read_text().strip() == str(os.getpid()):
            PID_FILE.unlink()
    except OSError:
        pass


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--daemon", action="store_true", help="двойной fork, переживает agent-шелл")
    args, _unknown = ap.parse_known_args()
    if args.daemon:
        daemonize()
    try:
        main()
    except Exception:
        import traceback
        traceback.print_exc()
        raise
