"""
Сбрасывает ТОЛЬКО репутацию кошельков, сохраняя остальной кэш.

Зачем: накопленные наблюдения считались по старому порогу победы (+200%).
После его замены на +30% старые и новые записи означают разное, и смешивать
их нельзя — винрейт получится мешаниной двух разных критериев.

Сохраняются: кэш скрининга кошельков, отметки виденных монет, результаты
rug-check, открытые позиции, дневной P&L.
Обнуляются: победы/поражения, статус смарт-мани, блок-лист, базовая частота.

ВАЖНО: запускать только при ОСТАНОВЛЕННОМ боте, иначе он перезапишет файл
своим состоянием из памяти.
"""
import json, shutil, sys
from pathlib import Path

PATH = Path("data/cache_state.json")

if not PATH.exists():
    print("Файла кэша нет — сбрасывать нечего"); sys.exit()

# Проверяем, ЖИВ ли процесс, а не просто есть ли файл: pkill файл после
# себя не удаляет, и проверка на существование давала ложный отказ.
pid_file = Path("data/sniper_bot.pid")
if pid_file.exists():
    import os
    try:
        pid = int(pid_file.read_text().strip())
    except (ValueError, OSError):
        pid = None
    alive = False
    if pid:
        try:
            os.kill(pid, 0)
            alive = True
        except ProcessLookupError:
            alive = False
        except PermissionError:
            alive = True
    if alive:
        print(f"⚠️  Бот запущен (PID {pid}) — он перезапишет сброс своим состоянием из памяти.")
        print(f"   Останови его: kill {pid}   (или pkill -f main.py)")
        sys.exit(1)
    print("Файл блокировки остался от завершённого процесса — продолжаю.")
    pid_file.unlink()

backup = PATH.with_name("cache_state.before_reset.json")
shutil.copy(PATH, backup)
print(f"Резервная копия: {backup}")

d = json.load(open(PATH))
before = len(d)

cleared_profiles = 0
removed = 0
out = {}
for k, v in d.items():
    if k.startswith("blocklist:") or k == "baseline:outcomes":
        removed += 1
        continue
    if k.startswith("wallet:") or k.startswith("profile:"):
        try:
            p = json.loads(v[0])
            if isinstance(p, dict) and (p.get("wins") or p.get("losses")):
                for f in ("wins", "losses", "is_proven_smart_money", "edge_vs_baseline",
                          "baseline_at_check", "required_winrate"):
                    p.pop(f, None)
                p["winrate"] = 0.5
                out[k] = [json.dumps(p, ensure_ascii=False), v[1]]
                cleared_profiles += 1
                continue
        except Exception:
            pass
    out[k] = v

json.dump(out, open(PATH, "w"), ensure_ascii=False)
print(f"Записей было {before}, стало {len(out)}")
print(f"Очищено профилей кошельков: {cleared_profiles}")
print(f"Удалено записей блок-листа и базовой частоты: {removed}")
print("\nОстальной кэш (скрининг, виденные монеты, rug-check) сохранён.")
