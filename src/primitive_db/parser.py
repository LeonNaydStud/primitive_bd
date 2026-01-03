import shlex
from typing import Any, Dict, Optional


def parse_where_condition(where_str: str) -> Optional[Dict[str, Any]]:
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


def parse_set_clause(set_str: str) -> Dict[str, Any]:
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


def parse_value(value_str: str) -> Any:
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


def parse_insert_values(values_str: str) -> list:
    try:
        values_str = values_str.strip()
        if values_str.startswith('(') and values_str.endswith(')'):
            values_str = values_str[1:-1]

        parts = shlex.split(values_str.replace(',', ' '))
        return [parse_value(p) for p in parts]
    except Exception as e:
        raise ValueError(f"Ошибка парсинга значений: {e}")