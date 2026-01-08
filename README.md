# Databricks Bundle Example

This project is an example implementation of a [Databricks Asset Bundle](https://docs.databricks.com/aws/en/dev-tools/bundles/) using a [Databricks Free Edition](https://www.databricks.com/learn/free-edition) workspace.

Included is a Python (PySpark/Delta) project, a dbt project and Databricks Workflows using these resources. Additionally CI/CD workflows (github) are included to test and deploy the Asset Bundle to different targets.

The project is configured using `pyproject.toml` (Python specifics) and `databricks.yaml` (Databricks Bundle specifics) and uses [uv](https://docs.astral.sh/uv/) to manage the Python project and dependencies.

## Repository Structure

| Directory | Description |
|-----------|-------------|
| `.github/workflows` | CI/CD jobs to test and deploy bundle |
| `src/dab_project` | Python project (Used in Databricks Workflow as Python-Wheel-Task) |
| `dbt` | [dbt](https://github.com/dbt-labs/dbt-core) project<br/>* Used in Databricks Workflow as dbt-Task<br/>* dbt-Models used from https://github.com/dbt-labs/jaffle_shop_duckdb |
| `resources` | Resources such as Databricks Workflows or Databricks Volumes/Schemas<br/>* Python-based workflow: https://docs.databricks.com/aws/en/dev-tools/bundles/python<br/>* YAML-based Workflow: https://docs.databricks.com/aws/en/dev-tools/bundles/resources#job |
| `scripts` | Python script to setup groups, service principals and catalogs used in a Databricks (Free Edition) workspace |
| `tests` | Unit-tests running on Databricks (via Connect) or locally<br/>* Used in [ci.yml](.github/workflows/ci.yml) jobs |

## Databricks Workspace

For this example we use a Databricks Free Edition workspace https://www.databricks.com/learn/free-edition with all resources and identities managed in the Workspace (no external connections or Cloud Identity Management).

### Setup

This Databricks Asset Bundle expects pre-existing Catalogs, Groups and Service Principals to showcase providing permissions on resources such as catalogs or workflows.

A script exists to set up the Workspace (Free Edition) as described in the [Setup Databricks Workspace](#setup-databricks-workspace) section.

* **Serverless environment**: [Version 4](https://docs.databricks.com/aws/en/release-notes/serverless/environment-version/four) which is similar to Databricks Runtime ~17.*
* **Catalogs**: `lake_dev`, `lake_test` and `lake_prod`
* **Service principals** (for CI/CD and Workflow runners)
  * `sp_etl_dev` (for dev and test) and `sp_etl_prod` (for prod)
  * Make sure the User used to deploy Workflows has `Service principal: User` on the used service principals
  * For CI/CD workflows we generated the Databricks secrets `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET`
* **Groups**
  * `group_etl` group with `ALL PRIVILEGES` and `group_reader` with limited permissions on catalogs
  * These are mostly to test applying grants using Asset Bundle resources


## Development

### Requirements

* uv: https://docs.astral.sh/uv/getting-started/installation/
  * `uv` will default to Python version specified in [.python-version](.python-version)
* Databricks CLI: https://docs.databricks.com/aws/en/dev-tools/cli/install
  * ">=0.270.0" due to 'databricks bundle plan' command

### Setup environment

Sync `uv` environment with `dev` (includes databricks-connect) dependencies:
```bash
uv sync --locked --group dev
```

> **Note:** For local Spark use the `dev-spark` dependency group instead.

#### (Optional) Activate virtual environment

Bash:
```bash
source .venv/bin/activate
```

Windows:
```powershell
.venv\Scripts\activate
```

### Databricks Connect

The `dev` dependency group includes `databricks-connect` for remote Spark execution. This requires authentication being set up via Databricks CLI.

See https://docs.databricks.com/aws/en/dev-tools/vscode-ext/ for using Databricks Connect extension in VS Code.

### Unit-Tests

```bash
uv run pytest -v
```

Based on whether Databricks Connect or local Spark is installed, the Unit-Tests use a Databricks Cluster or start a local Spark session with Delta support.
* On Databricks the unit-tests currently assume the catalog `lake_dev` exists.

### Checks

```bash
# Linting
uv run ruff check --fix
# Formatting
uv run ruff format
```

### Setup Databricks Workspace

The following script sets up a Databricks (Free Edition) Workspace for this project with additional catalogs, groups and service principals. It uses both Databricks-SDK and Databricks Connect (Serverless).

```bash
# Authenticate to your Databricks workspace, if you have not done so already:
# databricks configure

uv run ./scripts/setup_workspace.py
```

## Databricks CLI

1. Authenticate to your Databricks workspace, if you have not done so already:
    ```
    $ databricks configure
    ```

2. To deploy a development copy of this project, type:
    ```
    $ databricks bundle deploy --target dev
    ```

3. Similarly, to deploy a production copy, type:
   ```
   $ databricks bundle deploy --target prod
   ```

4. Deploy with custom variables
   ```
   $ databricks bundle deploy --target dev --var "catalog_name=workspace"
   ```

## FAQ

* Service Principals

   For this example, the targets `test` and `prod` use a group and service principals.

   The group `group_etl` can manage the workflow, ideally your user and the service principal are part of it. This group should also have sufficient permissions on the used Catalogs.

   Make sure the User used to deploy has `Service principal: User` permissions. `Service principal: Manager` is not enough.
* dbt project

   The `dbt` project is based on https://github.com/dbt-labs/jaffle_shop_duckdb with following changes:

   * Schema bronze, silver, gold
   * documented materialization `use_materialization_v2`
   * Primary, Foreign Key Constraints

## TODO:

* Streaming example
* Logging
  * Logging to volume