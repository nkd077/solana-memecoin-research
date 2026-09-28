#!/usr/bin/env bash
# Только dashboard (читает SQLite). Работает даже если main.py упал.
set -e
cd "$(dirname "$0")"
source venv/bin/activate
export DASHBOARD_HOST="${DASHBOARD_HOST:-127.0.0.1}"
export DASHBOARD_PORT="${DASHBOARD_PORT:-8787}"
exec python -u -m dashboard.app
