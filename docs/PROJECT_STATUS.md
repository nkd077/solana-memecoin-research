# PROJECT STATUS — 2026-09-10

## Что это
Solana / Pump.fun **research-стенд**: paper (`DRY_RUN`) измеряет edge до live.
Live не включался и не планируется без нового вопроса.

## Синтез
1. **На рождении выйти нельзя** — strict honest ≈ −100% (продаваемость ~5%).
2. **После градации выйти можно**, типичный исход на торгуемых **сильно отрицательный**
   (med ≈ −75…−95% за ~3 дня).
3. Окон «исполнимо и med≥0» не видно.

Журнал: [`CLOSED_HYPOTHESES.md`](../CLOSED_HYPOTHESES.md) —
**11 закрыты замером**, **1 снята по априору**, **1 стройка отменена**.
Там же: измеренные константы и ошибки ассистента.

## Краткий указатель

| | ID | Суть |
|--|-----|------|
| Замер | A1–A11 | lift, birth, insider, accum-activity, post-grad, wallet factors, liq_at_trigger, deployer factories, early buyers, accum-balance/плато, fdv/liq «навес» |
| Априор | B1 | vol-spike / «кит сидит» |
| Стройка | C1 | DAS dual-path — отменена (≡ mcap/liq) |

## Живое
**Форвард алертов канала** (4/15 в зачёте). Dex/Robinhood OK.
Лаг алерт→t0: **0.67–0.85 ч** (зафиксировано). Cron track `*/3` → `run_ext_track.sh`.
`vol24` в снимках. Пререгистрация: ~81% half ≠ доказательство китов (база A11).
Кластер 4×18мин ≈ одно окно. Подробности: [`channel/README.md`](../channel/README.md).

`lab_accum_balance` poller — контроль; пороги/DAS не трогать.

## Методология (якорь)
`is_exit_tradeable` / `recompute_honest.py`. Непродаваемое = −100%.
Метка ≠ деньги.

## Команды
```bash
make report
make honest
./check_status.sh
PYTHONUNBUFFERED=1 ./venv/bin/python -m research.analyze_overhang_graduates
```

## Режим рантайма
`DRY_RUN=true`, paper buys выкл / demo off, offline insider PAUSED
(ради snap pace accum, пока poller жив).
