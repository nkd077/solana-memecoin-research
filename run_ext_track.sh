#!/usr/bin/env bash
# Сетка досъёма внешних алертов: каждые 3 мин (GRID_FINE_SEC).
# Утверждение канала — касание −50% за сутки; редкий ручной track пропускает провалы.
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p logs data
exec ./venv/bin/python -u -m research.ext_signals track >> logs/ext_track.log 2>&1
