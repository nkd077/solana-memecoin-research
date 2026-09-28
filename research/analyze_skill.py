"""
Есть ли у кошельков навык, или всё это случайность?

Главный вопрос проекта. Расчёт показал: при случайном выборе токена
стратегия теряет ~9% на сделку, и вся прибыльность держится на допущении,
что отслеживаемые кошельки выбирают заметно лучше случайного. Здесь это
допущение проверяется.

Две независимые проверки:

1. ПЕРЕСТАНОВОЧНЫЙ ТЕСТ НА РАЗБРОС.
   Если навыка нет, все кошельки — броски одной монеты с вероятностью p.
   Перемешиваем метки побед/поражений между всеми наблюдениями много раз
   и смотрим, бывает ли случайно такой же разброс результатов, как
   наблюдаемый. Если реальный разброс шире почти всех перемешанных —
   значит кошельки различаются не случайно.

2. ПРОВЕРКА ПРЕДСКАЗУЕМОСТИ (главная).
   Прямо проверяет то, на чём построен бот: делим наблюдения каждого
   кошелька по времени пополам и смотрим, предсказывает ли результат
   первой половины результат второй. Если кошельки, удачные в прошлом,
   не оказываются удачнее среднего в будущем — копировать некого,
   сколько данных ни собирай.

Запуск: ./venv/bin/python -m research.analyze_skill
"""
import json
import random
from collections import defaultdict
from pathlib import Path
from statistics import mean

SRC = Path("data/outcomes.jsonl")
N_PERM = 2000


def load():
    if not SRC.exists():
        return []
    rows = []
    for line in SRC.open(encoding="utf-8"):
        try:
            r = json.loads(line)
            if r.get("wallet"):
                rows.append(r)
        except Exception:
            pass
    rows.sort(key=lambda r: r.get("ts", 0))
    return rows


def dispersion_stat(by_wallet):
    """Разброс долей побед по кошелькам, взвешенный по числу наблюдений.
    Чем больше — тем сильнее кошельки различаются между собой."""
    total = 0.0
    for wins, n in by_wallet:
        if n > 0:
            total += n * (wins / n) ** 2
    return total


def main():
    rows = load()
    print(f"Наблюдений в журнале: {len(rows)}")
    if len(rows) < 100:
        print("\nДанных пока мало — журнал начал заполняться только что.")
        print("Оставь бота поработать, вернись через несколько часов.")
        return

    p = mean(1 if r["is_win"] else 0 for r in rows)
    print(f"Базовая частота: {p*100:.2f}%\n")

    wallets = defaultdict(list)
    for r in rows:
        wallets[r["wallet"]].append(r)

    multi = {w: rs for w, rs in wallets.items() if len(rs) >= 2}
    print(f"Кошельков всего: {len(wallets)}, с 2+ наблюдениями: {len(multi)}")

    # ---------- 1. Перестановочный тест ----------
    print("\n" + "=" * 62)
    print("1. ЕСТЬ ЛИ РАЗБРОС СВЕРХ СЛУЧАЙНОГО")
    print("=" * 62)
    if len(multi) < 30:
        print(f"Кошельков с 2+ наблюдениями всего {len(multi)} — для теста нужно от 30.")
    else:
        obs = [(sum(1 for x in rs if x["is_win"]), len(rs)) for rs in multi.values()]
        actual = dispersion_stat(obs)

        labels = [1 if r["is_win"] else 0 for r in rows if r["wallet"] in multi]
        sizes = [len(rs) for rs in multi.values()]
        higher = 0
        for _ in range(N_PERM):
            random.shuffle(labels)
            perm, i = [], 0
            for n in sizes:
                perm.append((sum(labels[i:i + n]), n))
                i += n
            if dispersion_stat(perm) >= actual:
                higher += 1
        pval = higher / N_PERM
        print(f"наблюдаемый разброс: {actual:.2f}")
        print(f"случайность даёт такой же или больший в {pval*100:.1f}% случаев (p = {pval:.3f})")
        if pval < 0.05:
            print("-> Разброс ШИРЕ случайного: кошельки различаются не только везением.")
        else:
            print("-> Разброс НЕ отличим от случайного: признаков навыка пока не видно.")

    # ---------- 2. Предсказуемость ----------
    print("\n" + "=" * 62)
    print("2. ПРЕДСКАЗЫВАЕТ ЛИ ПРОШЛОЕ КОШЕЛЬКА ЕГО БУДУЩЕЕ  (главное)")
    print("=" * 62)
    usable = {w: rs for w, rs in wallets.items() if len(rs) >= 2}
    if len(usable) < 30:
        print(f"Кошельков с 2+ наблюдениями всего {len(usable)} — нужно от 30.")
    else:
        after_win, after_loss = [], []
        for rs in usable.values():
            half = len(rs) // 2
            first, second = rs[:half] or rs[:1], rs[half:] or rs[1:]
            if not second:
                continue
            first_won = any(x["is_win"] for x in first)
            later = [1 if x["is_win"] else 0 for x in second]
            (after_win if first_won else after_loss).extend(later)

        if not after_win or not after_loss:
            print("Пока не хватает кошельков с победами в первой половине.")
        else:
            rw, rl = mean(after_win), mean(after_loss)
            print(f"кошельки, выигравшие раньше:  дальше выигрывают {rw*100:.2f}%  ({len(after_win)} набл.)")
            print(f"кошельки, не выигравшие:      дальше выигрывают {rl*100:.2f}%  ({len(after_loss)} набл.)")
            print(f"базовая частота:              {p*100:.2f}%")
            lift = rw / rl if rl > 0 else float('inf')
            print(f"\nпревосходство прошлых победителей: {lift:.2f}x")

            # перестановочная проверка разницы
            allv = after_win + after_loss
            k = len(after_win)
            cnt = 0
            for _ in range(N_PERM):
                random.shuffle(allv)
                if mean(allv[:k]) - mean(allv[k:]) >= rw - rl:
                    cnt += 1
            pv = cnt / N_PERM
            print(f"случайность даёт такую же разницу в {pv*100:.1f}% случаев (p = {pv:.3f})")
            print()
            if pv < 0.05 and lift >= 2.5:
                print("-> ЕСТЬ СИГНАЛ и он достаточной силы. Копирование кошельков имеет смысл.")
            elif pv < 0.05:
                print(f"-> Сигнал есть, но слабый ({lift:.2f}x). Для выхода в ноль нужно ~3x.")
            else:
                print("-> Прошлое кошелька НЕ предсказывает будущее. Основания копировать нет.")


if __name__ == "__main__":
    import traceback
    try:
        main()
    except Exception:
        Path("data").mkdir(exist_ok=True)
        Path("data/skill_error.txt").write_text(traceback.format_exc(), encoding="utf-8")
        print(traceback.format_exc())
