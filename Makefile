# Единая точка воспроизведения выводов research-стенда.
.PHONY: report honest graduates exec status

report: honest graduates exec
	@echo ""
	@echo "=== PROJECT_STATUS.md ==="
	@head -40 docs/PROJECT_STATUS.md

honest:
	./venv/bin/python -m research.recompute_honest

graduates:
	./venv/bin/python -m research.track_graduates --report

exec:
	./venv/bin/python -m research.executability_report

status:
	./check_status.sh
