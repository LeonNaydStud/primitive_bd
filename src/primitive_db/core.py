"""Основная логика работы с таблицами базы данных."""
from typing import Any, Dict, List, Tuple

# Поддерживаемые типы данных
SUPPORTED_TYPES = {"int", "str", "bool"}


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
    # Проверяем, существует ли уже таблица
    if table_name in metadata:
        return False, f'Таблица "{table_name}" уже существует.', metadata

    # Парсим столбцы
    parsed_columns = []
    for col in columns:
        if ':' not in col:
            error_msg = f'Некорректный формат столбца: "{col}".'
            error_msg += ' Используйте "имя:тип".'
            return False, error_msg, metadata

        col_name, col_type = col.split(':', 1)
        col_type = col_type.lower()

        # Проверяем тип данных
        if col_type not in SUPPORTED_TYPES:
            types_str = ", ".join(SUPPORTED_TYPES)
            error_msg = f'Неподдерживаемый тип данных: "{col_type}".'
            error_msg += f' Допустимые: {types_str}.'
            return False, error_msg, metadata

        parsed_columns.append(f"{col_name}:{col_type}")

    # Добавляем автоматический ID столбец
    full_columns = ["ID:int"] + parsed_columns

    # Сохраняем в метаданные
    metadata[table_name] = {"columns": full_columns}

    columns_str = ", ".join(full_columns)
    success_msg = f'Таблица "{table_name}" успешно создана.'
    success_msg += f' Столбцы: {columns_str}'

    return True, success_msg, metadata


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
        return False, f'Таблица "{table_name}" не существует.', metadata

    # Удаляем таблицу
    del metadata[table_name]
    return True, f'Таблица "{table_name}" успешно удалена.', metadata


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
        result.append(f"{i}. {table} ({', '.join(columns)})")

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
    return col_type.lower() in SUPPORTED_TYPES