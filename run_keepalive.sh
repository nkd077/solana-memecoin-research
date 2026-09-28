#!/usr/bin/env bash
# Долгоживущий сторож bot+dashboard (демон через Python os.setsid).
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p logs data
PIDF=data/keepalive.pid

if [[ -f "$PIDF" ]] && kill -0 "$(cat "$PIDF")" 2>/dev/null; then
  echo "keepalive уже запущен pid=$(cat "$PIDF")"
  pgrep -fl 'keepalive.py' || true
  exit 0
fi
# на всякий случай старый nohup-процесс без pidfile
if pgrep -f 'python.*keepalive.py' >/dev/null 2>&1; then
  echo "keepalive уже запущен:"
  pgrep -fl 'python.*keepalive.py'
  exit 0
fi

echo "===== keepalive START $(date) =====" >> logs/keepalive.log
./venv/bin/python -u keepalive.py --daemon >> logs/keepalive.log 2>&1 </dev/null
sleep 1
if [[ -f "$PIDF" ]] && kill -0 "$(cat "$PIDF")" 2>/dev/null; then
  echo "keepalive pid $(cat "$PIDF")"
  echo "dashboard: http://127.0.0.1:8787/"
  echo "logs: tail -f logs/keepalive.log logs/bot_run.log"
else
  echo "FAILED — see logs/keepalive.log"
  tail -n 15 logs/keepalive.log || true
  exit 1
fi
