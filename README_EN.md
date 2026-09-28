# Why copying "whales" on Pump.fun doesn't pay — a Solana research bench

[Русская версия](README.md)

An async Python bench that tests memecoin trading hypotheses against real Solana on-chain
data (plus a Robinhood Chain probe). It has everything a live trading bot needs — chain
scanning, transaction parsing, scoring, risk management, execution via Jupiter/Jito and
PumpPortal — but runs in `DRY_RUN`: every idea is measured first, and money would only follow
if the numbers allowed it.

**They didn't.** Here is what was measured and how.

## Key results

| Measured | Result |
|---|---|
| Pump.fun launches that reach graduation | **0.52%** (155 of 29,814) |
| Positions actually exitable at token birth | **4.8%** |
| Honest expected value of entering at birth (unsellable = −100%) | **−94.3%** |
| Median exit pool | **$18** |
| Post-graduation median, tradeable tokens, ~3 days | **≈ −75…−95%** |
| Own latency to a signed transaction | floor 315 ms, realistic ~715 ms |
| Robinhood Chain, PONS factory, 58 launches, after 30 min | median ETH in curve **0**, 40% empty, max ≈ $321 |

**11 hypotheses closed by measurement** (scoring lift, insider funding clusters,
accumulation, post-graduation holding, wallet factors, deployer "factories", early-buyer
counts, holder concentration, fdv/liquidity overhang). None produced a window that is both
executable and has a non-negative median. Full log with instruments and verdicts:
[`CLOSED_HYPOTHESES.md`](CLOSED_HYPOTHESES.md) (in Russian).

## Methodology: not fooling yourself with numbers

The most valuable part is the measurement errors caught along the way — each of them alone
turned a losing strategy into a "winning" one:

- **Dead pools counted as alive.** "≥ 1 trade/hour" let $18 pools through and inflated win
  rate → strict `is_exit_tradeable`, unsellable = −100%.
- **Fake graduations.** 74% of "graduations" had physically impossible liquidity; the whole
  "deployer effect" came from them.
- **Wrong token decimals** in 38% of records; before the fix, one effect had the opposite sign.
- **Cohort medians** mixing tradeable and float-less pools showed "−4%" instead of −95%.
- **Label ≠ money.** A feature picked graduates 3.2× better than base, yet returns were worse.
- **Confident numbers from an AI assistant** — 5 documented cases, all recomputed on own data.

Hypotheses and failure criteria are pre-registered before the data comes in. Claims of an
external Telegram signal channel are archived at the moment of receipt, before the outcome
is known, and scored on the bench's own outcomes.

## Architecture

Helius WS/RPC → `chain_scanner` / `ws_scanner` → `tx_parser` + `pump_curve` (bonding-curve
decoding) → `wallet_screener` / `rug_checker` / `wallet_profiler` / `funding_graph` →
`scoring` / `edge_model` → `risk_manager` → executor (Jupiter + Jito bundle, or PumpPortal
with transaction verification before signing) → `position_monitor`, SQLite, Redis, Telegram
bot, aiohttp dashboard. `rhc/` reads Robinhood Chain (EVM) factory events directly.

**Stack:** Python 3.10+, asyncio, aiohttp, solana-py / solders, curl_cffi, Redis, SQLite,
pandas, scikit-learn. ~9k lines in the core.

## Running

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env          # set HELIUS_API_KEY; DRY_RUN=true by default
./run_bot.sh                  # paper mode, log in logs/bot_run.log
./run_dashboard.sh            # http://127.0.0.1:8787
python -m research.recompute_honest
python rhc/money.py           # Robinhood Chain, public RPC, no key
```

## Disclaimer

Research project, not trading advice. Live mode was never enabled. The main finding of this
repository is that the strategies described here lose money on the measured market.

## Author

**nkd077** — MSc student in Software Engineering, Peter the Great St. Petersburg Polytechnic University.
Telegram: [@nkdo77](https://t.me/nkdo77) · email: pollynleyna@gmail.com

**Hire me:** [@nkd077_orders_bot](https://t.me/nkd077_orders_bot) (Russian-language order bot) or write directly.
