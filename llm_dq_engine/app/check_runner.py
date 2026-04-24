from pathlib import Path
from typing import Any, Dict, List
import json

import pandas as pd
import yaml

BASE_DIR = Path(__file__).resolve().parent.parent


def load_dbt_yaml(table_name: str) -> dict:
    yaml_path = BASE_DIR / "generated" / "dbt" / f"{table_name}.yml"
    with yaml_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_sample_data(table_name: str) -> pd.DataFrame:
    csv_path = BASE_DIR / "sample_data" / f"{table_name}.csv"
    return pd.read_csv(csv_path)


def extract_checks_from_dbt_yaml(yaml_data: dict) -> List[Dict[str, Any]]:
    checks: List[Dict[str, Any]] = []

    models = yaml_data.get("models", [])
    if not models:
        return checks

    model = models[0]

    for column in model.get("columns", []):
        column_name = column["name"]

        for test in column.get("data_tests", []):
            if isinstance(test, str):
                checks.append(
                    {
                        "column": column_name,
                        "check_type": test,
                        "arguments": {},
                    }
                )
            elif isinstance(test, dict):
                test_name = next(iter(test))
                test_body = test[test_name] or {}
                arguments = test_body.get("arguments", {})

                checks.append(
                    {
                        "column": column_name,
                        "check_type": test_name,
                        "arguments": arguments,
                    }
                )

    return checks


def run_not_null_check(df: pd.DataFrame, column: str) -> dict:
    failed = df[df[column].isna()]

    return {
        "column": column,
        "check_type": "not_null",
        "passed": failed.empty,
        "failed_count": int(len(failed)),
        "failed_rows": failed.index.tolist(),
    }


def run_unique_check(df: pd.DataFrame, column: str) -> dict:
    duplicate_mask = df[column].notna() & df.duplicated(subset=[column], keep=False)
    failed = df[duplicate_mask]

    return {
        "column": column,
        "check_type": "unique",
        "passed": failed.empty,
        "failed_count": int(len(failed)),
        "failed_rows": failed.index.tolist(),
    }


def run_accepted_values_check(df: pd.DataFrame, column: str, values: List[Any]) -> dict:
    allowed = set(values)
    invalid_mask = df[column].notna() & ~df[column].isin(allowed)
    failed = df[invalid_mask]

    return {
        "column": column,
        "check_type": "accepted_values",
        "passed": failed.empty,
        "failed_count": int(len(failed)),
        "failed_rows": failed.index.tolist(),
        "allowed_values": values,
    }


def run_one_check(df: pd.DataFrame, check: dict) -> dict:
    column = check["column"]
    check_type = check["check_type"]
    arguments = check.get("arguments", {})

    if column not in df.columns:
        return {
            "column": column,
            "check_type": check_type,
            "passed": False,
            "failed_count": None,
            "failed_rows": [],
            "error": f"Column '{column}' not found in data",
        }

    if check_type == "not_null":
        return run_not_null_check(df, column)

    if check_type == "unique":
        return run_unique_check(df, column)

    if check_type == "accepted_values":
        values = arguments.get("values", [])
        return run_accepted_values_check(df, column, values)

    return {
        "column": column,
        "check_type": check_type,
        "passed": False,
        "failed_count": None,
        "failed_rows": [],
        "error": f"Unsupported check type: {check_type}",
    }


def summarize_results(table_name: str, check_results: List[dict]) -> dict:
    total_checks = len(check_results)
    passed_checks = sum(1 for item in check_results if item.get("passed") is True)
    failed_checks = sum(1 for item in check_results if item.get("passed") is False)

    return {
        "table_name": table_name,
        "total_checks": total_checks,
        "passed_checks": passed_checks,
        "failed_checks": failed_checks,
        "all_passed": failed_checks == 0,
        "checks": check_results,
    }


def save_results(table_name: str, result_data: dict) -> Path:
    output_path = BASE_DIR / "results" / f"{table_name}_validation_results.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(result_data, f, indent=2)

    return output_path


def validate_table(table_name: str) -> Path:
    yaml_data = load_dbt_yaml(table_name)
    df = load_sample_data(table_name)
    checks = extract_checks_from_dbt_yaml(yaml_data)

    check_results = []
    for check in checks:
        result = run_one_check(df, check)
        check_results.append(result)

    summary = summarize_results(table_name, check_results)
    return save_results(table_name, summary)


if __name__ == "__main__":
    for table_name in ["orders", "transactions"]:
        output = validate_table(table_name)
        print(f"Validation results written to: {output}")