"""Основная логика работы с таблицами и данными базы данных."""
from typing import Any, Dict, List, Optional, Tuple

from prettytable import PrettyTable

from src.constants import (
    CONFIRM_DELETE_RECORDS,
    CONFIRM_DROP_TABLE,
    ERROR_INVALID_FORMAT,
    ERROR_TABLE_EXISTS,
    ERROR_TABLE_NOT_EXISTS,
    ERROR_TYPE_MISMATCH,
    ERROR_UNSUPPORTED_TYPE,
    ERROR_WRONG_VALUE_COUNT,
    SUCCESS_RECORD_ADDED,
    SUCCESS_RECORD_DELETED,
    SUCCESS_RECORD_UPDATED,
    SUCCESS_TABLE_CREATED,
    SUCCESS_TABLE_DROPPED,
    VALID_TYPES,
)
from src.decorators import cacher, confirm_action, handle_db_errors, log_time

from .utils import load_table_data, save_table_data


@handle_db_errors
def create_table(
        metadata: Dict[str, Any],
        table_name: str,
        columns: List[str]
) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Создает новую таблицу в базе данных.

    Args:
        metadata: Текущие метаданные БД
        table_name: Имя таблицы
        columns: Список столбцов в формате "имя:тип"

    Returns:
        Tuple: (успех, сообщение, новые_метаданные)
    """
    if table_name in metadata:
        return False, ERROR_TABLE_EXISTS.format(table_name), metadata

    parsed_columns = []
    for col in columns:
        if ':' not in col:
            error_msg = ERROR_INVALID_FORMAT.format(col)
            return False, error_msg, metadata

        col_name, col_type = col.split(':', 1)
        col_type = col_type.lower()

        if col_type not in VALID_TYPES:
            types_str = ", ".join(VALID_TYPES)
            error_msg = ERROR_UNSUPPORTED_TYPE.format(col_type, types_str)
            return False, error_msg, metadata

        parsed_columns.append({"name": col_name, "type": col_type})

    full_columns = [{"name": "ID", "type": "int"}] + parsed_columns

    metadata[table_name] = {"columns": full_columns}

    save_table_data(table_name, [])

    columns_str = ", ".join([f"{c['name']}:{c['type']}" for c in full_columns])
    success_msg = SUCCESS_TABLE_CREATED.format(table_name, columns_str)

    return True, success_msg, metadata


@handle_db_errors
@confirm_action(CONFIRM_DROP_TABLE)
def drop_table(
        metadata: Dict[str, Any],
        table_name: str
) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Удаляет таблицу из базы данных.

    Args:
        metadata: Текущие метаданные БД
        table_name: Имя таблицы для удаления

    Returns:
        Tuple: (успех, сообщение, новые_метаданные)
    """
    if table_name not in metadata:
        return False, ERROR_TABLE_NOT_EXISTS.format(table_name), metadata

    del metadata[table_name]

    from .utils import delete_table_file
    delete_table_file(table_name)

    return True, SUCCESS_TABLE_DROPPED.format(table_name), metadata


@handle_db_errors
def list_tables(metadata: Dict[str, Any]) -> str:
    """
    Возвращает строку со списком таблиц.

    Args:
        metadata: Текущие метаданные БД

    Returns:
        Строка с именами таблиц
    """
    if not metadata:
        return "В базе данных нет таблиц."

    tables = list(metadata.keys())
    result = []
    for i, table in enumerate(tables, 1):
        columns = metadata[table]["columns"]
        columns_str = ", ".join([f"{c['name']}:{c['type']}" for c in columns])
        result.append(f"{i}. {table} ({columns_str})")

    return "\n".join(result)


def validate_column_format(column: str) -> bool:
    """
    Проверяет формат столбца.

    Args:
        column: Строка в формате "имя:тип"

    Returns:
        True если формат корректный
    """
    if ':' not in column:
        return False

    _, col_type = column.split(':', 1)
    return col_type.lower() in VALID_TYPES


def get_column_types(metadata: Dict[str, Any], table_name: str) -> Dict[str, str]:
    """
    Возвращает словарь с типами столбцов таблицы.

    Args:
        metadata: Метаданные БД
        table_name: Имя таблицы

    Returns:
        Словарь {имя_столбца: тип}
    """
    if table_name not in metadata:
        return {}

    return {col["name"]: col["type"] for col in metadata[table_name]["columns"]}


def validate_data_types(values: List[Any], column_types: Dict[str, str]) -> bool:
    """
    Проверяет соответствие значений типам столбцов.

    Args:
        values: Список значений
        column_types: Словарь с типами столбцов (без ID)

    Returns:
        True если типы соответствуют
    """
    data_columns = list(column_types.keys())[1:]  # без ID

    if len(values) != len(data_columns):
        return False

    for value, col_name in zip(values, data_columns):
        expected_type = column_types[col_name]
        if expected_type == "int" and not isinstance(value, int):
            return False
        elif expected_type == "str" and not isinstance(value, str):
            return False
        elif expected_type == "bool" and not isinstance(value, bool):
            return False

    return True


@handle_db_errors
@log_time
def insert(
        metadata: Dict[str, Any],
        table_name: str,
        values: List[Any]
) -> Tuple[bool, str]:
    """
    Вставляет новую запись в таблицу.

    Args:
        metadata: Метаданные БД
        table_name: Имя таблицы
        values: Список значений (без ID)

    Returns:
        Tuple: (успех, сообщение)
    """
    if table_name not in metadata:
        return False, ERROR_TABLE_NOT_EXISTS.format(table_name)

    table_data = load_table_data(table_name)

    column_types = get_column_types(metadata, table_name)
    data_columns = list(column_types.keys())[1:]  # без ID

    if len(values) != len(data_columns):
        expected = len(data_columns)
        actual = len(values)
        error_msg = ERROR_WRONG_VALUE_COUNT.format(expected, actual)
        return False, error_msg

    if not validate_data_types(values, column_types):
        return False, ERROR_TYPE_MISMATCH

    new_id = 1
    if table_data:
        ids = [record.get("ID", 0) for record in table_data]
        new_id = max(ids) + 1

    new_record = {"ID": new_id}
    for col_name, value in zip(data_columns, values):
        new_record[col_name] = value

    table_data.append(new_record)

    save_table_data(table_name, table_data)

    return True, SUCCESS_RECORD_ADDED.format(new_id, table_name)


@handle_db_errors
@log_time
def select(
        metadata: Dict[str, Any],
        table_name: str,
        where_clause: Optional[Dict[str, Any]] = None
) -> Tuple[bool, str, List[Dict[str, Any]]]:
    """
    Выполняет SELECT запрос.

    Args:
        metadata: Метаданные БД
        table_name: Имя таблицы
        where_clause: Условие фильтрации

    Returns:
        Tuple: (успех, сообщение, данные)
    """
    cache_key = f"select_{table_name}_{str(where_clause)}"

    def execute_select():
        if table_name not in metadata:
            return False, ERROR_TABLE_NOT_EXISTS.format(table_name), []

        table_data = load_table_data(table_name)

        if not table_data:
            return True, f'Таблица "{table_name}" пуста.', []

        if where_clause:
            filtered_data = []
            for record in table_data:
                match = True
                for column, value in where_clause.items():
                    if record.get(column) != value:
                        match = False
                        break
                if match:
                    filtered_data.append(record)

            if not filtered_data:
                return True, "Записи не найдены.", []

            return True, "", filtered_data

        return True, "", table_data

    return cacher(cache_key, execute_select)


def format_table_output(
        data: List[Dict[str, Any]],
        columns: List[Dict[str, str]]
) -> str:
    """
    Форматирует данные таблицы для вывода.

    Args:
        data: Данные для вывода
        columns: Столбцы таблицы

    Returns:
        Отформатированная строка таблицы
    """
    if not data:
        return "Нет данных для отображения."

    table = PrettyTable()

    column_names = [col["name"] for col in columns]
    table.field_names = column_names

    for col in columns:
        if col["type"] == "int":
            table.align[col["name"]] = "r"
        elif col["type"] == "str":
            table.align[col["name"]] = "l"
        elif col["type"] == "bool":
            table.align[col["name"]] = "c"

    for record in data:
        row = [record.get(col["name"], "") for col in columns]
        table.add_row(row)

    return str(table)


@handle_db_errors
def update(
        metadata: Dict[str, Any],
        table_name: str,
        set_clause: Dict[str, Any],
        where_clause: Dict[str, Any]
) -> Tuple[bool, str]:
    """
    Обновляет записи в таблице.

    Args:
        metadata: Метаданные БД
        table_name: Имя таблицы
        set_clause: Что обновлять
        where_clause: Условие для выбора записей

    Returns:
        Tuple: (успех, сообщение)
    """
    if table_name not in metadata:
        return False, ERROR_TABLE_NOT_EXISTS.format(table_name)

    column_types = get_column_types(metadata, table_name)
    for col in list(set_clause.keys()) + list(where_clause.keys()):
        if col not in column_types:
            return False, f'Столбец "{col}" не существует в таблице "{table_name}".'

    table_data = load_table_data(table_name)

    if not table_data:
        return False, f'Таблица "{table_name}" пуста.'

    updated_count = 0
    updated_ids = []

    for record in table_data:
        match = True
        for column, value in where_clause.items():
            if record.get(column) != value:
                match = False
                break

        if match:
            for column, new_value in set_clause.items():
                record[column] = new_value
            updated_count += 1
            updated_ids.append(record["ID"])

    if updated_count == 0:
        return False, "Записи не найдены по заданному условию."

    save_table_data(table_name, table_data)

    ids_str = ", ".join(map(str, updated_ids))
    return True, SUCCESS_RECORD_UPDATED.format(ids_str, table_name)


@handle_db_errors
@confirm_action(CONFIRM_DELETE_RECORDS)
def delete(
        metadata: Dict[str, Any],
        table_name: str,
        where_clause: Dict[str, Any]
) -> Tuple[bool, str]:
    """
    Удаляет записи из таблицы.

    Args:
        metadata: Метаданные БД
        table_name: Имя таблицы
        where_clause: Условие для выбора записей

    Returns:
        Tuple: (успех, сообщение)
    """
    if table_name not in metadata:
        return False, ERROR_TABLE_NOT_EXISTS.format(table_name)

    table_data = load_table_data(table_name)

    if not table_data:
        return False, f'Таблица "{table_name}" пуста.'

    records_to_keep = []
    deleted_ids = []

    for record in table_data:
        match = True
        for column, value in where_clause.items():
            if record.get(column) != value:
                match = False
                break

        if match:
            deleted_ids.append(record["ID"])
        else:
            records_to_keep.append(record)

    if not deleted_ids:
        return False, "Записи не найдены по заданному условию."

    save_table_data(table_name, records_to_keep)

    ids_str = ", ".join(map(str, deleted_ids))
    return True, SUCCESS_RECORD_DELETED.format(ids_str, table_name)


@handle_db_errors
def table_info(
        metadata: Dict[str, Any],
        table_name: str
) -> Tuple[bool, str]:
    """
    Возвращает информацию о таблице.

    Args:
        metadata: Метаданные БД
        table_name: Имя таблицы

    Returns:
        Tuple: (успех, сообщение)
    """
    if table_name not in metadata:
        return False, ERROR_TABLE_NOT_EXISTS.format(table_name)

    table_data = load_table_data(table_name)
    record_count = len(table_data)

    columns = metadata[table_name]["columns"]
    columns_str = ", ".join([f"{c['name']}:{c['type']}" for c in columns])

    info_lines = [
        f"Таблица: {table_name}",
        f"Столбцы: {columns_str}",
        f"Количество записей: {record_count}"
    ]

    return True, "\n".join(info_lines)