from pathlib import Path
from typing import Dict, List, Optional, Any
import json
import yaml

from schema_reader import load_schema

BASE_DIR = Path(__file__).resolve().parent.parent

ALLOWED_DBT_TESTS = {"not_null", "unique", "accepted_values", "relationships"}


def load_validated_llm_output(table_name: str) -> Optional[dict]:
    path = BASE_DIR / "generated" / "validated" / f"{table_name}_validated_output.json"
    if not path.exists():
        return None

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def normalize_llm_tests(llm_output: Optional[dict]) -> Dict[str, List[str]]:
    result: Dict[str, List[str]] = {}

    if not llm_output:
        return result

    for item in llm_output.get("dbt_tests", []):
        column = item.get("column")
        tests = item.get("tests", [])

        if not column or not isinstance(tests, list):
            continue

        safe_tests = []
        for test_name in tests:
            if isinstance(test_name, str) and test_name in ALLOWED_DBT_TESTS:
                safe_tests.append(test_name)

        if safe_tests:
            result[column] = sorted(set(safe_tests))

    return result


def build_metadata_tests(column: dict) -> List[Any]:
    tests: List[Any] = []

    is_primary_key = column.get("is_primary_key", False)
    nullable = column.get("nullable", True)
    accepted_values = column.get("accepted_values")

    if is_primary_key:
        tests.append("not_null")
        tests.append("unique")
    elif nullable is False:
        tests.append("not_null")

    if accepted_values:
        tests.append(
            {
                "accepted_values": {
                    "arguments": {
                        "values": accepted_values
                    }
                }
            }
        )

    relationship = column.get("relationship")
    if relationship and isinstance(relationship, dict):
        to_model = relationship.get("to")
        field = relationship.get("field")

        if to_model and field:
            tests.append(
                {
                    "relationships": {
                        "arguments": {
                            "to": f"ref('{to_model}')",
                            "field": field
                        }
                    }
                }
            )

    return tests


def merge_llm_tests(metadata_tests: List[Any], llm_tests: List[str], column: dict) -> List[Any]:
    existing_scalar_tests = {t for t in metadata_tests if isinstance(t, str)}

    for test_name in llm_tests:
        if test_name not in {"not_null", "unique"}:
            continue

        if test_name == "unique" and not column.get("is_primary_key", False):
            continue

        if test_name not in existing_scalar_tests:
            metadata_tests.append(test_name)

    return metadata_tests


def build_model_yaml(schema: dict, llm_output: Optional[dict] = None) -> dict:
    table_name = schema["table_name"]
    llm_test_map = normalize_llm_tests(llm_output)

    model_entry = {
        "name": table_name,
        "columns": []
    }

    for column in schema.get("columns", []):
        column_name = column["name"]

        metadata_tests = build_metadata_tests(column)
        llm_tests = llm_test_map.get(column_name, [])

        final_tests = merge_llm_tests(metadata_tests, llm_tests, column)

        column_entry = {"name": column_name}

        if final_tests:
            column_entry["data_tests"] = final_tests

        model_entry["columns"].append(column_entry)

    return {
        "version": 2,
        "models": [model_entry]
    }


def save_dbt_yaml(table_name: str, yaml_data: dict) -> Path:
    output_path = BASE_DIR / "generated" / "dbt" / f"{table_name}.yml"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(yaml_data, f, sort_keys=False, allow_unicode=True)

    return output_path


def generate_dbt_yaml_from_schema(schema_path: Path) -> Path:
    schema = load_schema(schema_path)
    table_name = schema["table_name"]

    llm_output = load_validated_llm_output(table_name)
    yaml_data = build_model_yaml(schema, llm_output)

    return save_dbt_yaml(table_name, yaml_data)


if __name__ == "__main__":
    schema_files = [
        BASE_DIR / "metadata" / "orders.json",
        BASE_DIR / "metadata" / "transactions.json",
    ]

    for schema_path in schema_files:
        out = generate_dbt_yaml_from_schema(schema_path)
        print(f"Generated: {out}")