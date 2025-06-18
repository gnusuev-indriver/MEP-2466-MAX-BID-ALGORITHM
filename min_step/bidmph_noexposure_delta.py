from typing import List

def is_applicable(steps: List[int], max_bit: int) -> bool:
    if not steps:                       
        return False
    return max_bit < max(steps)

def process_steps(
    steps: List[int],
    max_bit: int,
    delta: int,
) -> List[int]:
    """
    Реализация алгоритма.

    :param steps: исходный массив шагов-бедований
    :param max_bit: MaxBid (ввод пользователя)
    :param delta: Δ — минимальная допустимая разница
    :return: итоговый массив
    """
    # 1. Проверка применимости
    if not is_applicable(steps, max_bit):
        return steps.copy()

    # 2. Построение промежуточного списка T
    t = [x for x in steps if x < max_bit]
    t.append(max_bit)

    # 3. Контроль Δ
    if len(t) >= 2:
        prev = t[-2]
        if (max_bit - prev) < delta:
            t.pop(-2)                   # Удаляем prev, оставляя только MaxBid

    return t