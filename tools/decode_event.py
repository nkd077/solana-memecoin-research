"""
Поиск раскладки события по инварианту бондинг-кривой.

Кривая Pump.fun — постоянное произведение: виртуальные резервы SOL,
умноженные на виртуальные резервы токенов, дают примерно одно и то же
число (30 SOL x 1073M токенов ~ 3.2e25). Это уникальная подпись: ищем
пару соседних u64, дающих такое произведение, и получаем точные смещения
резервов, не угадывая структуру целиком.
"""
import asyncio, base64, json, struct
from collections import defaultdict
import aiohttp
from config import settings

B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
def b58(raw):
    n = int.from_bytes(raw,"big"); o=""
    while n: n,r = divmod(n,58); o = B58[r]+o
    return "1"*(len(raw)-len(raw.lstrip(b"\x00")))+o

TARGET = 30e9 * 1.073e15          # инвариант кривой
MAIN = "bddb7fd34ee661ee"

def find_reserves(blob):
    """Пары соседних u64 с произведением, похожим на инвариант кривой."""
    out = []
    for o in range(0, len(blob)-16):
        a, b = struct.unpack_from("<QQ", blob, o)
        if a == 0 or b == 0: continue
        if not (25e9 < a < 200e9): continue         # SOL в разумных пределах
        if not (1e14 < b < 1.2e15): continue        # токены в разумных пределах
        ratio = (a*b) / TARGET
        if 0.7 < ratio < 1.5:
            out.append((o, a/1e9, b/1e6, ratio))
    return out

async def main():
    got = defaultdict(list)
    async with aiohttp.ClientSession() as s:
        async with s.ws_connect(settings.helius_ws(), heartbeat=30) as ws:
            await ws.send_json({"jsonrpc":"2.0","id":1,"method":"logsSubscribe",
                "params":[{"mentions":[settings.CHAIN_SCAN_PROGRAM_ID]},{"commitment":"confirmed"}]})
            dl = asyncio.get_event_loop().time() + 40
            async for msg in ws:
                if asyncio.get_event_loop().time() > dl: break
                if msg.type != aiohttp.WSMsgType.TEXT: continue
                v = ((json.loads(msg.data).get("params") or {}).get("result") or {}).get("value") or {}
                if v.get("err"): continue
                buy = any("Instruction: Buy" in l for l in (v.get("logs") or []))
                for l in (v.get("logs") or []):
                    if l.startswith("Program data:"):
                        try: blob = base64.b64decode(l.split("Program data:")[1].strip())
                        except Exception: continue
                        if blob[:8].hex() == MAIN:
                            got[MAIN].append((blob, buy))
                if len(got[MAIN]) >= 10: break

    items = got[MAIN]
    print(f"событий основного типа собрано: {len(items)}\n")
    if not items: return

    # где инвариант находится стабильно во всех экземплярах
    votes = defaultdict(int)
    for blob, _ in items:
        for o, a, b, r in find_reserves(blob):
            votes[o] += 1
    print("смещения, где инвариант сходится (смещение: в скольких событиях):")
    for o, n in sorted(votes.items(), key=lambda x: -x[1])[:5]:
        print(f"   смещение {o}: {n} из {len(items)}")
    if not votes:
        print("инвариант не найден — резервы лежат иначе"); return

    RES = max(votes.items(), key=lambda x: x[1])[0]
    print(f"\n=> резервы на смещении {RES}\n")

    # раскладываем остальное относительно найденного
    print("проверка раскладки на всех событиях:")
    for blob, is_buy_log in items[:6]:
        vsol, vtok = struct.unpack_from("<QQ", blob, RES)
        mint = b58(blob[8:40])
        sol_amt, tok_amt = struct.unpack_from("<QQ", blob, 40)
        flag = blob[56]
        user = b58(blob[57:89])
        price = (vsol/1e9)/(vtok/1e6)
        ok = (0 < sol_amt/1e9 < 100) and flag in (0,1) and (flag==1)==is_buy_log
        print(f"  {'✅' if ok else '❌'} {'ПОКУПКА' if flag else 'ПРОДАЖА'} (в логах покупка: {is_buy_log})"
              f"  {sol_amt/1e9:>8.4f} SOL -> {tok_amt/1e6:>13.2f} токенов")
        print(f"     mint={mint[:18]}... кошелёк={user[:14]}... цена={price:.12f} SOL"
              f"  резервы {vsol/1e9:.2f}/{vtok/1e6:.0f}")
    print(f"\nРаскладка: mint=8..40, объём SOL=40, объём токенов=48, флаг покупки=56, кошелёк=57..89, резервы={RES}")

asyncio.run(main())
