# AGENTS.md

This repo uses Databricks CLI to deploy a Databricks Asset Bundle.

## Project Structure
- `databricks.yml`: Databricks Asset Bundle configuration file
- `dbt`: dbt project
- `src/dab_project`: Python project using PySpark and DeltaTable
- `resources`: Databricks workflow resources
- `tests`: Unit tests for the Python project

 ## Setup commands
- Install deps: `uv sync --locked --group dev-spark --no-dev`
- Run code checks: `uv run --no-dev ruff check`
- Check code formatting: `uv run --no-dev ruff format --check`
- Run tests: `uv run --no-dev pytest -v`
 