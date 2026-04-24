# LLM-Augmented Data Quality Engine

## Overview
This project uses an LLM to generate structured data quality suggestions from schema metadata, validates that output, and converts it into dbt YAML and Great Expectations style expectation files.

## Problem
Writing data quality checks manually for every dataset takes time. This project automates the first draft of those checks using schema-driven prompting and validation logic.

## Features
- Reads table metadata from JSON
- Sends schema context to a local LLM
- Validates LLM output using Pydantic
- Generates dbt YAML files
- Generates GX expectation JSON files
- Executes selected checks locally on sample CSV data
- Saves pass/fail validation results

## Project Flow
Metadata -> LLM -> Validated structured output -> dbt YAML / GX JSON -> Local validation results

## Example Inputs
- metadata/orders.json
- metadata/transactions.json

## Example Outputs
- generated/dbt/orders.yml
- generated/gx/orders_expectations.json
- results/orders_validation_results.json

## Tech Stack
- Python
- Pandas
- Pydantic
- PyYAML
- Ollama
- qwen2.5:1.5b

## Future Improvements
- Add profiling stats to prompts
- Add custom SQL/dbt tests
- Add confidence scoring for LLM rules
- Add cloud execution flow with Databricks/dbt Cloud

## Architecture explanation

-	metadata JSON
	    ->
	schema_reader.py
	    ->
	llm_generator.py
	    ->
	validator.py
	    ->
	dbt_writer.py / gx_writer.py
	    ->
	check_runner.py
	    ->
	results JSON

## Flow explanation

-	1. Read table metadata
	2. Send metadata to local LLM
	3. Get structured rule suggestions
	4. Validate LLM output
	5. Generate dbt YAML and GX expectation files
	6. Run checks on sample data
	7. Save pass/fail results
