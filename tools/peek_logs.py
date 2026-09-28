"""Что реально приходит в уведомлении подписки — смотрим сырьё."""
import asyncio, json, base64
import aiohttp
from config import settings

async def main():
    got = []
    deadline = asyncio.get_event_loop().time() + 30   # своё ограничение: timeout на macOS нет
    async with aiohttp.ClientSession() as s:
        async with s.ws_connect(settings.helius_ws(), heartbeat=30) as ws:
            await ws.send_json({"jsonrpc":"2.0","id":1,"method":"logsSubscribe",
                "params":[{"mentions":[settings.CHAIN_SCAN_PROGRAM_ID]},{"commitment":"confirmed"}]})
            async for msg in ws:
                if asyncio.get_event_loop().time() > deadline:
                    print('время вышло, покупок не поймали — попробуй ещё раз')
                    break
                if msg.type != aiohttp.WSMsgType.TEXT: continue
                d = json.loads(msg.data)
                v = ((d.get("params") or {}).get("result") or {}).get("value") or {}
                logs = v.get("logs") or []
                if not any("Instruction: Buy" in l for l in logs): continue
                got.append((v.get("signature"), logs))
                if len(got) >= 2: break

    for sig, logs in got:
        print(f"=== {sig[:24]}... ===")
        for l in logs:
            short = l if len(l) < 110 else l[:107] + "..."
            print(f"  {short}")
        # ищем данные события
        for l in logs:
            if l.startswith("Program data:"):
                blob = base64.b64decode(l.split("Program data:")[1].strip())
                print(f"\n  --> Program data: {len(blob)} байт, начало (hex): {blob[:16].hex()}")
        print()

asyncio.run(main())
