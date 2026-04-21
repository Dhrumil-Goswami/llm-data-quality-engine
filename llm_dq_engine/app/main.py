import json
from pathlib import Path

from llm_generator import try_generate_checks_from_schema

BASE_DIR = Path(__file__).resolve().parent.parent


def main():
    schema_files = [
        BASE_DIR / "metadata" / "orders.json",
        BASE_DIR / "metadata" / "transactions.json",
    ]

    for schema_path in schema_files:
        print(f"\nRunning Day 3 flow for: {schema_path.name}")
        result = try_generate_checks_from_schema(schema_path)
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()