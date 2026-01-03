"""Движок базы данных - обработка команд и основной цикл."""
import shlex
from typing import List

import prompt

from .core import create_table, drop_table, list_tables, validate_column_format
from .utils import load_metadata, save_metadata


def print_help() -> None:
    print("\n***База данных***")
    print("\nФункции работы с таблицами:")
    print("<command> create_table <имя_таблицы> <столбец1:тип> <столбец2:тип> ..")
    print("   - создать таблицу")
    print("<command> list_tables - показать список всех таблиц")
    print("<command> drop_table <имя_таблицы> - удалить таблицу")

    print("\nОбщие команды:")
    print("<command> exit - выход из программы")
    print("<command> help - справочная информация\n")


def print_welcome() -> None:
    print("Первая попытка запустить проект!")
    print("***")
    print_help()


def parse_command(user_input: str) -> tuple[str, List[str]]:
    try:
        parts = shlex.split(user_input)
        if not parts:
            return "", []
        command = parts[0].lower()
        args = parts[1:] if len(parts) > 1 else []
        return command, args
    except ValueError:
        return "", []


def handle_create_table(args: List[str]) -> str:
    if len(args) < 2:
        return "Ошибка: Недостаточно аргументов."

    table_name = args[0]
    columns = args[1:]

    for col in columns:
        if not validate_column_format(col):
            return f'Некорректное значение: "{col}". Используйте формат "имя:тип".'

    metadata = load_metadata()
    success, message, new_metadata = create_table(metadata, table_name, columns)

    if success:
        save_metadata(new_metadata)

    return message


def handle_drop_table(args: List[str]) -> str:
    if len(args) != 1:
        return "Ошибка: Неверное количество аргументов."

    table_name = args[0]
    metadata = load_metadata()
    success, message, new_metadata = drop_table(metadata, table_name)

    if success:
        save_metadata(new_metadata)

    return message


def handle_list_tables() -> str:
    metadata = load_metadata()
    return list_tables(metadata)


def run() -> None:
    print_welcome()

    while True:
        try:
            user_input = prompt.string("Введите команду: ").strip()

            command, args = parse_command(user_input)

            if not command:
                print("Пожалуйста, введите команду. Используйте 'help' для справки.")
                continue

            if command == "exit":
                print("Выход из программы...")
                break

            elif command == "help":
                print_help()

            elif command == "create_table":
                result = handle_create_table(args)
                print(result)

            elif command == "drop_table":
                result = handle_drop_table(args)
                print(result)

            elif command == "list_tables":
                result = handle_list_tables()
                print(result)

            else:
                print(f"Функции '{command}' нет. Попробуйте снова.")

        except KeyboardInterrupt:
            print("\n\nПрограмма прервана. Используйте 'exit' для выхода.")
            break
        except EOFError:
            print("\nВыход из программы...")
            break
        except Exception as e:
            print(f"Неожиданная ошибка: {e}")