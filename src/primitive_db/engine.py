"""Движок базы данных - обработка команд и основной цикл."""
import shlex
from typing import List

import prompt

from .core import (
    create_table,
    delete,
    drop_table,
    format_table_output,
    insert,
    list_tables,
    select,
    table_info,
    update,
    validate_column_format,
)
from .parser import parse_insert_values, parse_set_clause, parse_where_condition
from .utils import load_metadata, save_metadata


def print_help() -> None:
    print("\n***Операции с данными***")
    print("\nФункции:")
    print("<command> insert into <имя_таблицы> values (<значение1>, ...)")
    print("   - создать запись")
    print("<command> select from <имя_таблицы> where <столбец> = <значение>")
    print("   - прочитать записи по условию")
    print("<command> select from <имя_таблицы>")
    print("   - прочитать все записи")
    print("<command> update <имя_таблицы> set <столбец1> = <новое_значение1>")
    print("   where <столбец_условия> = <значение_условия> - обновить запись")
    print("<command> delete from <имя_таблицы> where <столбец> = <значение>")
    print("   - удалить запись")
    print("<command> info <имя_таблицы> - вывести информацию о таблице")
    print("<command> create_table <имя_таблицы> <столбец1:тип> ...")
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


def parse_complex_command(full_command: str) -> tuple[str, List[str]]:
    parts = full_command.lower().split()
    if not parts:
        return "", []

    if len(parts) >= 4 and parts[0] == "insert" and parts[1] == "into":
        return "insert", parts[2:]
    elif len(parts) >= 3 and parts[0] == "select" and parts[1] == "from":
        return "select", parts[2:]
    elif len(parts) >= 2 and parts[0] == "update":
        return "update", parts[1:]
    elif len(parts) >= 4 and parts[0] == "delete" and parts[1] == "from":
        return "delete", parts[2:]
    elif len(parts) >= 2 and parts[0] == "info":
        return "info", parts[1:]
    elif len(parts) >= 2 and parts[0] == "create_table":
        return "create_table", parts[1:]
    elif len(parts) >= 2 and parts[0] == "drop_table":
        return "drop_table", parts[1:]
    elif len(parts) == 1 and parts[0] == "list_tables":
        return "list_tables", []
    elif len(parts) == 1 and parts[0] == "help":
        return "help", []
    elif len(parts) == 1 and parts[0] == "exit":
        return "exit", []
    else:
        return parts[0], parts[1:]


def handle_insert(args: List[str]) -> str:
    if len(args) < 2:
        return "Ошибка: Неверный формат команды."

    table_name = args[0]
    values_part = " ".join(args[1:])

    if not values_part.lower().startswith("values"):
        return "Ошибка: Отсутствует ключевое слово VALUES."

    values_str = values_part[6:].strip()  # удаляем "values"

    try:
        values = parse_insert_values(values_str)
    except ValueError as e:
        return f"Ошибка: {e}"

    metadata = load_metadata()
    success, message = insert(metadata, table_name, values)

    return message


def handle_select(args: List[str]) -> str:
    if len(args) < 1:
        return "Ошибка: Неверный формат команды."

    table_name = args[0]
    where_clause = None

    if len(args) > 1:
        if args[1].lower() != "where":
            return "Ошибка: Неверный формат условия WHERE."

        where_str = " ".join(args[2:])
        try:
            where_clause = parse_where_condition(where_str)
        except ValueError as e:
            return f"Ошибка: {e}"

    metadata = load_metadata()
    success, message, data = select(metadata, table_name, where_clause)

    if not success:
        return message

    if not data:
        return "Нет данных для отображения."

    columns = metadata[table_name]["columns"]
    return format_table_output(data, columns)


def handle_update(args: List[str]) -> str:
    if len(args) < 4:
        return "Ошибка: Неверный формат команды."

    table_name = args[0]

    try:
        set_idx = args.index("set")
    except ValueError:
        return "Ошибка: Отсутствует ключевое слово SET."

    try:
        where_idx = args.index("where")
    except ValueError:
        return "Ошибка: Отсутствует ключевое слово WHERE."

    if where_idx <= set_idx + 1:
        return "Ошибка: Неверный формат команды."

    set_str = " ".join(args[set_idx + 1:where_idx])
    try:
        set_clause = parse_set_clause(set_str)
    except ValueError as e:
        return f"Ошибка в SET: {e}"

    where_str = " ".join(args[where_idx + 1:])
    try:
        where_clause = parse_where_condition(where_str)
    except ValueError as e:
        return f"Ошибка в WHERE: {e}"

    if where_clause is None:
        return "Ошибка: Отсутствует условие WHERE."

    metadata = load_metadata()
    success, message = update(metadata, table_name, set_clause, where_clause)

    return message


def handle_delete(args: List[str]) -> str:
    if len(args) < 3:
        return "Ошибка: Неверный формат команды."

    table_name = args[0]

    if args[1].lower() != "where":
        return "Ошибка: Отсутствует ключевое слово WHERE."

    where_str = " ".join(args[2:])
    try:
        where_clause = parse_where_condition(where_str)
    except ValueError as e:
        return f"Ошибка: {e}"

    if where_clause is None:
        return "Ошибка: Отсутствует условие WHERE."

    metadata = load_metadata()
    success, message = delete(metadata, table_name, where_clause)

    return message


def handle_info(args: List[str]) -> str:
    if len(args) != 1:
        return "Ошибка: Используйте: info <имя_таблицы>"

    table_name = args[0]
    metadata = load_metadata()
    success, message = table_info(metadata, table_name)

    return message


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


def run() -> None:
    print_welcome()

    while True:
        try:
            user_input = prompt.string("Введите команду: ").strip()

            if not user_input:
                print("Пожалуйста, введите команду. Используйте 'help' для справки.")
                continue

            command, args = parse_complex_command(user_input)

            if not command:
                print("Неизвестная команда. Используйте 'help' для справки.")
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
                metadata = load_metadata()
                result = list_tables(metadata)
                print(result)

            elif command == "insert":
                result = handle_insert(args)
                print(result)

            elif command == "select":
                result = handle_select(args)
                print(result)

            elif command == "update":
                result = handle_update(args)
                print(result)

            elif command == "delete":
                result = handle_delete(args)
                print(result)

            elif command == "info":
                result = handle_info(args)
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