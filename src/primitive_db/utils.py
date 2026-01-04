"""Вспомогательные функции для работы с файлами."""
import json
from pathlib import Path
from typing import Any, Dict, List

from src.constants import DATA_DIR, META_FILE
from src.decorators import handle_db_errors


@handle_db_errors
def ensure_data_dir() -> Path:
    """Создает директорию data, если она не существует."""
    data_dir = Path(DATA_DIR)
    data_dir.mkdir(exist_ok=True)
    return data_dir


@handle_db_errors
def load_metadata(filepath: str = META_FILE) -> Dict[str, Any]:
    """
    Загружает метаданные из JSON-файла.

    Args:
        filepath: Путь к файлу с метаданными

    Returns:
        Словарь с метаданными или пустой словарь
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


@handle_db_errors
def save_metadata(data: Dict[str, Any], filepath: str = META_FILE) -> None:
    """
    Сохраняет метаданные в JSON-файл.

    Args:
        data: Словарь с метаданными
        filepath: Путь к файлу для сохранения
    """
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_table_filepath(table_name: str) -> Path:
    """
    Возвращает путь к файлу данных таблицы.

    Args:
        table_name: Имя таблицы

    Returns:
        Path к файлу данных
    """
    ensure_data_dir()
    return Path(DATA_DIR) / f"{table_name}.json"


@handle_db_errors
def load_table_data(table_name: str) -> List[Dict[str, Any]]:
    """
    Загружает данные таблицы из JSON-файла.

    Args:
        table_name: Имя таблицы

    Returns:
        Список записей таблицы или пустой список
    """
    filepath = get_table_filepath(table_name)
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return []


@handle_db_errors
def save_table_data(table_name: str, data: List[Dict[str, Any]]) -> None:
    """
    Сохраняет данные таблицы в JSON-файл.

    Args:
        table_name: Имя таблицы
        data: Данные для сохранения
    """
    filepath = get_table_filepath(table_name)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


@handle_db_errors
def delete_table_file(table_name: str) -> None:
    """
    Удаляет файл данных таблицы.

    Args:
        table_name: Имя таблицы
    """
    filepath = get_table_filepath(table_name)
    try:
        filepath.unlink()
    except FileNotFoundError:
        pass