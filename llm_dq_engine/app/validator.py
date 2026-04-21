from typing import Any, Dict, List
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator


ALLOWED_DBT_TESTS = {
    "not_null",
    "unique",
    "accepted_values",
    "relationships",
    "expression_is_true",
}


class DBTTestItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    column: str = Field(min_length=1)
    tests: List[str] = Field(min_length=1)

    @field_validator("tests")
    @classmethod
    def validate_tests(cls, value: List[str]) -> List[str]:
        if not isinstance(value, list):
            raise ValueError("tests must be a list")

        cleaned = []
        for test_name in value:
            if not isinstance(test_name, str):
                raise ValueError("each dbt test must be a string")
            test_name = test_name.strip()
            if not test_name:
                raise ValueError("dbt test name cannot be empty")
            cleaned.append(test_name)

        return cleaned


class GXExpectationItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expectation_type: str = Field(min_length=1)
    column: str = Field(min_length=1)
    kwargs: Dict[str, Any] = Field(default_factory=dict)


class LLMValidationOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    table_name: str = Field(min_length=1)
    dbt_tests: List[DBTTestItem] = Field(default_factory=list)
    gx_expectations: List[GXExpectationItem] = Field(default_factory=list)


def validate_llm_output(data: dict) -> LLMValidationOutput:
    return LLMValidationOutput.model_validate(data)


def validate_llm_output_json(raw_json: str) -> LLMValidationOutput:
    return LLMValidationOutput.model_validate_json(raw_json)


def format_validation_error(exc: ValidationError) -> str:
    lines = ["Validation failed:"]
    for err in exc.errors():
        location = " -> ".join(str(x) for x in err.get("loc", []))
        message = err.get("msg", "Unknown validation error")
        lines.append(f"- {location}: {message}")
    return "\n".join(lines)


def sanitize_llm_output(data: dict) -> dict:
    cleaned_dbt_tests = []

    for item in data.get("dbt_tests", []):
        column = item.get("column")
        tests = item.get("tests", [])

        if not column:
            continue
        if not isinstance(tests, list):
            continue
        if len(tests) == 0:
            continue

        cleaned_dbt_tests.append({
            "column": column,
            "tests": tests
        })

    data["dbt_tests"] = cleaned_dbt_tests
    return data