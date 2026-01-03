import json
from typing import Any, Dict


def load_metadata(filepath: str = "db_meta.json") -> Dict[str, Any]:
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_metadata(data: Dict[str, Any], filepath: str = "db_meta.json") -> None:
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)