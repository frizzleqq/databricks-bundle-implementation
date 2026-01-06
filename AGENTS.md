# AGENTS.md

This repo uses Databricks CLI to deploy a Databricks Asset Bundle.

## Project Structure
- `databricks.yml`: Databricks Asset Bundle configuration file
- `dbt`: dbt project
- `src/dab_project`: Python project using PySpark and DeltaTable
- `resources`: Databricks workflow resources
- `tests`: Unit tests for the Python project

 ## Setup commands
- Install deps: `uv sync --locked --all-extras`
- Run code checks: `uv run ruff check`
- Check code formatting: `uv run ruff format --check`
- Run tests: `uv run pytest -v`
 
## Unit-Tests
- On Linux systems, run: `uv run pytest -v`
- On Windows systems, run:
    ```powershell
    uv pip uninstall pyspark
    uv pip install databricks-connect==17.2.*
    uv run --no-sync pytest -v
    ```