"""Точка входа в программу базы данных."""
from .engine import run


def main() -> None:
    """Основная функция запуска программы."""
    run()


if __name__ == "__main__":
    main()