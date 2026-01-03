import json
from pathlib import Path
from typing import Any, Dict, List


def ensure_data_dir() -> Path:
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    return data_dir


def load_metadata(filepath: str = "db_meta.json") -> Dict[str, Any]:
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_metadata(data: Dict[str, Any], filepath: str = "db_meta.json") -> None:
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_table_filepath(table_name: str) -> Path:
    ensure_data_dir()
    return Path("data") / f"{table_name}.json"


def load_table_data(table_name: str) -> List[Dict[str, Any]]:
    filepath = get_table_filepath(table_name)
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return []


def save_table_data(table_name: str, data: List[Dict[str, Any]]) -> None:
    filepath = get_table_filepath(table_name)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def delete_table_file(table_name: str) -> None:
    filepath = get_table_filepath(table_name)
    try:
        filepath.unlink()
    except FileNotFoundError:
        pass