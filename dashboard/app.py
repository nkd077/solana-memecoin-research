"""
Простой HTTP-дашборд статуса бота (aiohttp).

Запуск отдельно: python -m dashboard.app
Или поднимается из main.py, если DASHBOARD_ENABLED=true.
"""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path

from aiohttp import web

from config import settings
from core.db import get_db

logger = logging.getLogger("sniper.dashboard")

# Глобальная ссылка на live-состояние, выставляется из main.py
_runtime = {
    "health": None,
    "smart_money_count": 0,
    "dry_run": True,
    "paused": False,
    "open_positions": {},
}


def set_runtime(**kwargs):
    _runtime.update(kwargs)


HTML = """<!doctype html>
<html lang="ru"><head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Sniper Bot</title>
<style>
  :root { --bg:#0f1419; --card:#1a2332; --fg:#e7ecf3; --mut:#8b9bb4; --ok:#3dd68c; --bad:#ff6b6b; --acc:#5b9dff; }
  *{box-sizing:border-box} body{margin:0;font:15px/1.45 ui-sans-serif,system-ui;background:var(--bg);color:var(--fg)}
  header{padding:20px 24px;border-bottom:1px solid #243044;display:flex;gap:16px;align-items:baseline;flex-wrap:wrap}
  h1{margin:0;font-size:20px;letter-spacing:.02em} .pill{padding:4px 10px;border-radius:999px;background:#243044;color:var(--mut);font-size:12px}
  .ok{color:var(--ok)} .bad{color:var(--bad)}
  main{padding:20px 24px;display:grid;gap:16px;grid-template-columns:repeat(auto-fit,minmax(280px,1fr))}
  section{background:var(--card);border:1px solid #243044;border-radius:12px;padding:14px 16px}
  h2{margin:0 0 10px;font-size:13px;text-transform:uppercase;letter-spacing:.08em;color:var(--mut)}
  table{width:100%;border-collapse:collapse;font-size:13px}
  td,th{padding:6px 4px;border-bottom:1px solid #243044;text-align:left;vertical-align:top}
  th{color:var(--mut);font-weight:500} .mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px}
  a{color:var(--acc)}
</style></head><body>
<header>
  <h1>Sniper Bot</h1>
  <span class="pill" id="mode">…</span>
  <span class="pill" id="uptime">…</span>
  <span class="pill"><a href="/api/summary">API</a></span>
</header>
<main>
  <section><h2>Здоровье</h2><div id="health" class="mono">загрузка…</div></section>
  <section><h2>Сводка БД</h2><div id="db" class="mono">загрузка…</div></section>
  <section><h2>Позиции</h2><div id="pos" class="mono">загрузка…</div></section>
  <section style="grid-column:1/-1"><h2>Последние сигналы</h2>
    <table><thead><tr><th>время</th><th>mint</th><th>wallet</th><th>score</th><th>decision</th></tr></thead>
    <tbody id="signals"></tbody></table>
  </section>
  <section style="grid-column:1/-1"><h2>Health events</h2>
    <table><thead><tr><th>время</th><th>kind</th><th>detail</th></tr></thead>
    <tbody id="hev"></tbody></table>
  </section>
</main>
<script>
async function refresh(){
  const s = await (await fetch('/api/summary')).json();
  document.getElementById('mode').textContent =
    (s.dry_run?'DRY_RUN':'LIVE') + (s.paused?' · PAUSED':'');
  document.getElementById('uptime').textContent = 'uptime ' + (s.health?.uptime_sec||0) + 's';
  const h = s.health||{};
  const gap = h.ws_gap_sec;
  const gapCls = (gap!=null && gap > 90) ? 'bad' : 'ok';
  document.getElementById('health').innerHTML =
    `WS gap: <span class="${gapCls}">${gap??'—'}s</span><br>`+
    `reconnects: ${h.ws_reconnects||0}<br>`+
    `helius 429: ${h.helius_429||0}<br>`+
    `parsed: ${h.scanner_parsed||0} / notifications ${h.scanner_notifications||0}<br>`+
    `smart money: ${s.smart_money_count||0}`;
  const d = s.db||{};
  document.getElementById('db').innerHTML =
    `signals ${d.signals||0}<br>trades ${d.trades||0}<br>`+
    `outcomes ${d.outcomes||0} (wins ${d.outcome_wins||0})<br>`+
    `funders cached ${d.wallet_funders||0}`;
  const pos = s.open_positions||{};
  document.getElementById('pos').textContent = Object.keys(pos).length
    ? Object.entries(pos).map(([m,p])=>`${m.slice(0,8)}… ${p.size_sol??p.remaining_sol} SOL`).join('\\n')
    : 'нет открытых';
  document.getElementById('signals').innerHTML = (s.recent_signals||[]).map(r=>
    `<tr><td>${new Date(r.ts*1000).toLocaleTimeString()}</td>`+
    `<td class="mono">${(r.token_mint||'').slice(0,8)}…</td>`+
    `<td class="mono">${(r.wallet||'').slice(0,8)}…</td>`+
    `<td>${(r.score??0).toFixed?.(3)??r.score}</td>`+
    `<td>${r.decision||''}</td></tr>`).join('');
  document.getElementById('hev').innerHTML = (s.recent_health||[]).map(r=>
    `<tr><td>${new Date(r.ts*1000).toLocaleTimeString()}</td>`+
    `<td>${r.kind}</td><td>${r.detail||''}</td></tr>`).join('');
}
refresh(); setInterval(refresh, 5000);
</script></body></html>
"""


async def handle_index(_request: web.Request) -> web.Response:
    return web.Response(text=HTML, content_type="text/html")


async def handle_summary(_request: web.Request) -> web.Response:
    db = get_db()
    health = _runtime.get("health")
    health_snap = health.snapshot() if health and hasattr(health, "snapshot") else {}
    payload = {
        "ts": time.time(),
        "dry_run": _runtime.get("dry_run", True),
        "paused": _runtime.get("paused", False),
        "smart_money_count": _runtime.get("smart_money_count", 0),
        "open_positions": _runtime.get("open_positions") or {},
        "health": health_snap,
        "db": db.summary(),
        "recent_signals": db.recent_signals(40),
        "recent_trades": db.recent_trades(40),
        "recent_health": db.recent_health(30),
    }
    return web.json_response(payload)


async def create_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_get("/api/summary", handle_summary)
    return app


async def start_dashboard(host: str | None = None, port: int | None = None):
    host = host or settings.DASHBOARD_HOST
    port = port or settings.DASHBOARD_PORT
    app = await create_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    logger.info("Dashboard: http://%s:%d/", host, port)
    return runner


def main():
    logging.basicConfig(level=logging.INFO)
    import asyncio

    async def _run():
        await start_dashboard()
        while True:
            await asyncio.sleep(3600)

    asyncio.run(_run())


if __name__ == "__main__":
    main()
