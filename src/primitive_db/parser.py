"""Парсеры для разбора условий WHERE и SET."""
import shlex
from typing import Any, Dict, Optional

from src.decorators import handle_db_errors


@handle_db_errors
def parse_where_condition(where_str: str) -> Optional[Dict[str, Any]]:
    """
    Парсит условие WHERE в формате "column = value".

    Args:
        where_str: Строка условия

    Returns:
        Словарь {column: value} или None если условие пустое
    """
    if not where_str:
        return None

    try:
        parts = shlex.split(where_str)
        if len(parts) != 3 or parts[1] != '=':
            raise ValueError(f"Неверный формат условия WHERE: {where_str}")

        column = parts[0]
        value_str = parts[2]

        value = parse_value(value_str)

        return {column: value}
    except Exception as e:
        raise ValueError(f"Ошибка парсинга условия WHERE: {e}")


@handle_db_errors
def parse_set_clause(set_str: str) -> Dict[str, Any]:
    """
    Парсит предложение SET в формате "column = value".

    Args:
        set_str: Строка SET

    Returns:
        Словарь {column: value}
    """
    try:
        parts = shlex.split(set_str)
        if len(parts) != 3 or parts[1] != '=':
            raise ValueError(f"Неверный формат SET: {set_str}")

        column = parts[0]
        value_str = parts[2]

        value = parse_value(value_str)

        return {column: value}
    except Exception as e:
        raise ValueError(f"Ошибка парсинга SET: {e}")


@handle_db_errors
def parse_value(value_str: str) -> Any:
    """
    Парсит строковое значение в соответствующий тип Python.

    Args:
        value_str: Строковое представление значения

    Returns:
        Значение соответствующего типа
    """
    value_str = value_str.strip()

    if value_str.lower() == 'true':
        return True
    elif value_str.lower() == 'false':
        return False

    try:
        return int(value_str)
    except ValueError:
        pass

    if (value_str.startswith('"') and value_str.endswith('"')) or \
            (value_str.startswith("'") and value_str.endswith("'")):
        return value_str[1:-1]

    return value_str


@handle_db_errors
def parse_insert_values(values_str: str) -> list:
    """
    Парсит значения для INSERT в формате "(value1, value2, ...)".

    Args:
        values_str: Строка значений

    Returns:
        Список значений
    """
    try:
        values_str = values_str.strip()
        if values_str.startswith('(') and values_str.endswith(')'):
            values_str = values_str[1:-1]

        parts = shlex.split(values_str.replace(',', ' '))
        return [parse_value(p) for p in parts]
    except Exception as e:
        raise ValueError(f"Ошибка парсинга значений: {e}")