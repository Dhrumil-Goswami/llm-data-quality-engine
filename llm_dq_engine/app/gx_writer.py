from pathlib import Path
from typing import Any, Dict, List, Optional
import json

from schema_reader import load_schema

BASE_DIR = Path(__file__).resolve().parent.parent

ALLOWED_GX_EXPECTATIONS = {
    "expect_column_values_to_not_be_null",
    "expect_column_values_to_be_unique",
    "expect_column_values_to_be_in_set",
}


def load_validated_llm_output(table_name: str) -> Optional[dict]:
    path = BASE_DIR / "generated" / "validated" / f"{table_name}_validated_output.json"
    if not path.exists():
        return None

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def normalize_llm_expectations(llm_output: Optional[dict]) -> Dict[str, List[dict]]:
    result: Dict[str, List[dict]] = {}

    if not llm_output:
        return result

    for item in llm_output.get("gx_expectations", []):
        expectation_type = item.get("expectation_type")
        column = item.get("column")
        kwargs = item.get("kwargs", {})

        if not expectation_type or not column:
            continue

        if expectation_type not in ALLOWED_GX_EXPECTATIONS:
            continue

        if not isinstance(kwargs, dict):
            kwargs = {}

        if column not in result:
            result[column] = []

        result[column].append(
            {
                "expectation_type": expectation_type,
                "kwargs": kwargs,
            }
        )

    return result


def build_metadata_expectations(column: dict) -> List[dict]:
    expectations: List[dict] = []

    column_name = column["name"]
    is_primary_key = column.get("is_primary_key", False)
    nullable = column.get("nullable", True)
    accepted_values = column.get("accepted_values")

    if is_primary_key:
        expectations.append(
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": column_name},
            }
        )
        expectations.append(
            {
                "expectation_type": "expect_column_values_to_be_unique",
                "kwargs": {"column": column_name},
            }
        )

    elif nullable is False:
        expectations.append(
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": column_name},
            }
        )

    if accepted_values:
        expectations.append(
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {
                    "column": column_name,
                    "value_set": accepted_values,
                },
            }
        )

    return expectations


def merge_llm_expectations(
    metadata_expectations: List[dict],
    llm_expectations: List[dict],
    column: dict,
) -> List[dict]:
    existing_keys = set()

    for item in metadata_expectations:
        key = (
            item["expectation_type"],
            json.dumps(item["kwargs"], sort_keys=True),
        )
        existing_keys.add(key)

    for item in llm_expectations:
        expectation_type = item["expectation_type"]
        kwargs = item.get("kwargs", {})

        if expectation_type == "expect_column_values_to_be_unique" and not column.get("is_primary_key", False):
            continue

        if expectation_type == "expect_column_values_to_be_in_set":
            if "value_set" not in kwargs:
                continue

        final_item = {
            "expectation_type": expectation_type,
            "kwargs": {"column": column["name"], **kwargs},
        }

        key = (
            final_item["expectation_type"],
            json.dumps(final_item["kwargs"], sort_keys=True),
        )

        if key not in existing_keys:
            metadata_expectations.append(final_item)
            existing_keys.add(key)

    return metadata_expectations


def build_gx_output(schema: dict, llm_output: Optional[dict] = None) -> dict:
    table_name = schema["table_name"]
    llm_map = normalize_llm_expectations(llm_output)

    final_expectations: List[dict] = []

    for column in schema.get("columns", []):
        column_name = column["name"]

        metadata_expectations = build_metadata_expectations(column)
        llm_expectations = llm_map.get(column_name, [])

        merged = merge_llm_expectations(metadata_expectations, llm_expectations, column)
        final_expectations.extend(merged)

    return {
        "table_name": table_name,
        "expectations": final_expectations,
    }


def save_gx_output(table_name: str, gx_data: dict) -> Path:
    output_path = BASE_DIR / "generated" / "gx" / f"{table_name}_expectations.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(gx_data, f, indent=2)

    return output_path


def generate_gx_file_from_schema(schema_path: Path) -> Path:
    schema = load_schema(schema_path)
    table_name = schema["table_name"]

    llm_output = load_validated_llm_output(table_name)
    gx_data = build_gx_output(schema, llm_output)

    return save_gx_output(table_name, gx_data)


if __name__ == "__main__":
    schema_files = [
        BASE_DIR / "metadata" / "orders.json",
        BASE_DIR / "metadata" / "transactions.json",
    ]

    for schema_path in schema_files:
        output = generate_gx_file_from_schema(schema_path)
        print(f"Generated: {output}")