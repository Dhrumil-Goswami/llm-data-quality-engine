import json
from pathlib import Path

from llm_generator import try_generate_checks_from_schema
from dbt_writer import generate_dbt_yaml_from_schema
from check_runner import validate_table
from gx_writer import generate_gx_file_from_schema

BASE_DIR = Path(__file__).resolve().parent.parent


def main():
    schema_files = [
        BASE_DIR / "metadata" / "orders.json",
        BASE_DIR / "metadata" / "transactions.json",
    ]

    for schema_path in schema_files:
        table_name = schema_path.stem

        print(f"\nRunning full flow for: {schema_path.name}")

        llm_result = try_generate_checks_from_schema(schema_path)
        print(json.dumps(llm_result, indent=2))

        dbt_yaml_path = generate_dbt_yaml_from_schema(schema_path)
        print(f"dbt YAML written to: {dbt_yaml_path}")

        result_path = validate_table(table_name)
        print(f"validation results written to: {result_path}")

        gx_path = generate_gx_file_from_schema(schema_path)
        print(f"GX file written to: {gx_path}")


if __name__ == "__main__":
    main()