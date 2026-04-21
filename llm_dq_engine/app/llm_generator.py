import json
from pathlib import Path
from urllib import request, error

from schema_reader import load_schema
from validator import (
    validate_llm_output,
    format_validation_error,
    sanitize_llm_output
)

BASE_DIR = Path(__file__).resolve().parent.parent
OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL_NAME = "qwen2.5:1.5b"


def build_prompt(schema: dict) -> str:
    return f"""
You are a data quality assistant.

Given the following table schema metadata, generate:
1. suggested dbt tests
2. suggested Great Expectations checks

Return ONLY valid JSON in this exact format:

{{
  "table_name": "string",
  "dbt_tests": [
    {{
      "column": "string",
      "tests": ["not_null", "unique"]
    }}
  ],
  "gx_expectations": [
    {{
      "expectation_type": "string",
      "column": "string",
      "kwargs": {{}}
    }}
  ]
}}

Rules:
- dbt_tests must always be a list
- gx_expectations must always be a list
- every dbt test item must have column and tests
- every GX item must have expectation_type and column
- do not include explanation text
- output only JSON

Schema metadata:
{json.dumps(schema, indent=2)}
""".strip()


def call_local_llm(prompt: str, model: str = MODEL_NAME) -> str:
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
    }

    req = request.Request(
        OLLAMA_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    try:
        with request.urlopen(req) as response:
            result = json.loads(response.read().decode("utf-8"))
            return result["response"]
    except error.URLError as e:
        raise RuntimeError(
            "Could not connect to Ollama. Make sure Ollama is running."
        ) from e


def extract_json_block(text: str) -> str:
    text = text.strip()

    if text.startswith("{") and text.endswith("}"):
        return text

    start = text.find("{")
    end = text.rfind("}")

    if start == -1 or end == -1 or start >= end:
        raise ValueError("Could not find JSON object in LLM response.")

    return text[start:end + 1]


def save_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def save_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")



def generate_checks_from_schema(schema_path: Path) -> dict:
    schema = load_schema(schema_path)
    prompt = build_prompt(schema)

    raw_response = call_local_llm(prompt)
    raw_output_path = BASE_DIR / "generated" / "raw" / f"{schema['table_name']}_llm_output.json"
    save_text(raw_output_path, raw_response)

    json_text = extract_json_block(raw_response)

    parsed_output = json.loads(json_text)
    parsed_output = sanitize_llm_output(parsed_output)

    validated = validate_llm_output(parsed_output)


    validated_dict = validated.model_dump()

    validated_output_path = BASE_DIR / "generated" / "validated" / f"{schema['table_name']}_validated_output.json"
    save_json(validated_output_path, validated_dict)

    return validated_dict


def try_generate_checks_from_schema(schema_path: Path) -> dict:
    try:
        return {
            "success": True,
            "data": generate_checks_from_schema(schema_path),
            "error": None,
        }
    except Exception as exc:
        return {
            "success": False,
            "data": None,
            "error": str(exc),
        }


if __name__ == "__main__":
    orders_path = BASE_DIR / "metadata" / "orders.json"
    result = try_generate_checks_from_schema(orders_path)

    print(json.dumps(result, indent=2))