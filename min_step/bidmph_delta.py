import argparse
from typing import List

def criterion_applies(default_steps: List[int], MaxBid: int) -> bool:
    """
    Новый алгоритм применяем, только если
        MaxBid < max(default_steps)
    """
    return MaxBid < max(default_steps)

def compute_steps(
    default_steps: List[int], MaxBid: int, delta_param: float
) -> List[float]:
    if not criterion_applies(default_steps, MaxBid):
        return default_steps

    a = float(min(default_steps))
    b = float(MaxBid)

    if (b - a) < delta_param:
        return [float(MaxBid)]

    # ── Шаг 6: начальная длина ────────────────────────────────────────────────
    N = len(default_steps)

    # ── Шаг 7: коррекция длины под ограничение Δ_param ────────────────────────
    while N > 2 and ((b - a) / (N - 1) < delta_param):
        N -= 1

    # ── Шаг 8: равномерное разрезание отрезка [a, b] ──────────────────────────
    delta_final = (b - a) / (N - 1) if N > 1 else 0.0
    result_steps = [a + i * delta_final for i in range(N)]

    return result_steps


# ────────────────────────────────────────────────────────────────────────────────
def main() -> None:
    args = parse_args()

    # Парсинг списка шагов
    try:
        default_steps = [int(x.strip()) for x in args.steps.split(",") if x.strip()]
        if not default_steps:
            raise ValueError
    except ValueError:
        raise SystemExit("Ошибка: --steps должен содержать хотя бы одно целое число")

    result = compute_steps(default_steps, args.MaxBid, args.delta)

    # Вывод результата
    print(result)


if __name__ == "__main__":
    main()
