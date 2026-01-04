"""Декораторы для обработки ошибок, логирования и подтверждения действий."""
import functools
import time
from typing import Any, Callable, Dict

import prompt

from .constants import LOG_TIME_THRESHOLD


def handle_db_errors(func: Callable) -> Callable:
    """
    Декоратор для обработки ошибок базы данных.

    Перехватывает:
    - FileNotFoundError: файлы данных не найдены
    - KeyError: таблицы или столбцы не найдены
    - ValueError: ошибки валидации
    - Exception: все остальные ошибки

    Args:
        func: Декорируемая функция

    Returns:
        Обернутая функция с обработкой ошибок
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        try:
            return func(*args, **kwargs)
        except FileNotFoundError:
            error_msg = "Ошибка: Файл данных не найден."
            error_msg += " Возможно, база данных не инициализирована."
            print(error_msg)
            return None
        except KeyError as e:
            print(f"Ошибка: Таблица или столбец '{e}' не найден.")
            return None
        except ValueError as e:
            print(f"Ошибка валидации: {e}")
            return None
        except Exception as e:
            print(f"Произошла непредвиденная ошибка: {e}")
            return None

    return wrapper


def confirm_action(action_name: str) -> Callable:
    """
    Фабрика декораторов для запроса подтверждения действий.

    Args:
        action_name: Название действия для отображения пользователю

    Returns:
        Декоратор, запрашивающий подтверждение
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            # Получаем имя таблицы из аргументов (обычно второй аргумент)
            table_name = ""
            if len(args) > 1:
                table_name = args[1] if isinstance(args[1], str) else ""

            # Формируем сообщение
            message = f'Вы уверены, что хотите выполнить "{action_name}"'
            if table_name:
                message += f' таблицы "{table_name}"'
            message += "? [y/n]: "

            # Запрашиваем подтверждение
            try:
                response = prompt.string(message).strip().lower()
            except (KeyboardInterrupt, EOFError):
                print("\nОперация отменена.")
                return None

            if response != 'y':
                print("Операция отменена.")
                return None

            # Выполняем функцию
            return func(*args, **kwargs)

        return wrapper

    return decorator


def log_time(func: Callable) -> Callable:
    """
    Декоратор для измерения времени выполнения функции.

    Args:
        func: Декорируемая функция

    Returns:
        Обернутая функция с логированием времени
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        start_time = time.monotonic()
        result = func(*args, **kwargs)
        end_time = time.monotonic()

        elapsed = end_time - start_time
        if elapsed > LOG_TIME_THRESHOLD:
            print(f"Функция {func.__name__} выполнилась за {elapsed:.3f} секунд")

        return result

    return wrapper


def create_cacher() -> Callable:
    """
    Создает функцию для кэширования результатов.

    Returns:
        Функция cache_result(key, value_func) с замыканием на кэш
    """
    cache: Dict[str, Any] = {}

    def cache_result(key: str, value_func: Callable[[], Any]) -> Any:
        """
        Возвращает значение из кэша или вычисляет и сохраняет его.

        Args:
            key: Ключ для кэша
            value_func: Функция для вычисления значения

        Returns:
            Значение из кэша или вычисленное значение
        """
        if key in cache:
            return cache[key]

        value = value_func()
        cache[key] = value
        return value

    return cache_result


# Создаем глобальный кэшер для использования в проекте
cacher = create_cacher()