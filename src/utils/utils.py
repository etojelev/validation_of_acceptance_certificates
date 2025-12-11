import json
from pathlib import Path
from typing import Any


def get_tokens() -> Any:
    tokens_path = Path(__file__).parents[2] / "tokens.json"
    with tokens_path.open("r", encoding="utf-8") as file:
        return json.load(file)
