import functools
import time
from typing import Any, Callable, Dict

import prompt


def handle_db_errors(func: Callable) -> Callable:

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

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            table_name = ""
            if len(args) > 1:
                table_name = args[1] if isinstance(args[1], str) else ""

            message = f'Вы уверены, что хотите выполнить "{action_name}"'
            if table_name:
                message += f' таблицы "{table_name}"'
            message += "? [y/n]: "

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

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        start_time = time.monotonic()
        result = func(*args, **kwargs)
        end_time = time.monotonic()

        elapsed = end_time - start_time
        if elapsed > 0.1:
            print(f"Функция {func.__name__} выполнилась за {elapsed:.3f} секунд")

        return result

    return wrapper


def create_cacher() -> Callable:
    cache: Dict[str, Any] = {}

    def cache_result(key: str, value_func: Callable[[], Any]) -> Any:
        if key in cache:
            return cache[key]

        value = value_func()
        cache[key] = value
        return value

    return cache_result


cacher = create_cacher()