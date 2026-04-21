import json
from pathlib import Path


def load_schema(file_path: str) -> dict:
    path = Path(file_path)
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


if __name__ == "__main__":
    schema = load_schema("../metadata/orders.json")
    print(schema)