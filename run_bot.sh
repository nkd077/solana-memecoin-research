#!/usr/bin/env bash
# Стабильный запуск бота + dashboard. Держи окно терминала открытым
# ИЛИ запускай так — процесс отвязан от Cursor/IDE.
set -e
cd "$(dirname "$0")"
source venv/bin/activate
mkdir -p logs data
rm -f data/sniper_bot.pid

# Убиваем старые экземпляры на этом порту/скрипте
pkill -f 'python -u main.py' 2>/dev/null || true
pkill -f 'python main.py' 2>/dev/null || true
sleep 1

export LOG_LEVEL="${LOG_LEVEL:-INFO}"
nohup python -u main.py >> logs/bot_run.log 2>&1 &
echo $! > data/sniper_bot.pid
echo "bot PID $(cat data/sniper_bot.pid)"
echo "dashboard: http://127.0.0.1:8787/"
echo "log: tail -f logs/bot_run.log"

# Ждём порт
for i in 1 2 3 4 5 6 7 8 9 10; do
  if curl -sf --max-time 1 http://127.0.0.1:8787/api/summary >/dev/null; then
    echo "OK — dashboard отвечает"
    exit 0
  fi
  sleep 1
done
echo "WARN — dashboard ещё не ответил, смотри logs/bot_run.log"
exit 1
