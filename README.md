# Databricks Bundle Example

This project is an example of a Databricks Asset bundle that deploys the following:

* Python Project as Wheel
* Databricks Workflow examples
   * Python-based workflow
   * YAML-based Workflow

Uses Databricks Free Edition: https://www.databricks.com/learn/free-edition
* Using serverless environment version 3, which is similar to Databricks Runtime 16.3
* For this example we created in the Workspace:
   * `lake_dev`, `lake_test` and `lake_prod` catalog
   * service principals (for assigning Workflow runners)
      * Make sure the User used to deploy has `Service principal: User`
   * `group_etl` group with `ALL PRIVILEGES` on the catalogs
      * your user and the service principals should be members

## TODO:

* Logging
   * Logging to volume?
* Github Action for deployment
* maybe dbt

## Development

### Requirements

* uv: https://docs.astral.sh/uv/getting-started/installation/
   * `uv` will default to Python version specified in [.python-version](.python-version)
* Databricks CLI: https://docs.databricks.com/aws/en/dev-tools/cli/install
   * ">=0.259.0" for Python based workflows with `environment_version`

### Setup environment

Sync entire `uv` environment:
```bash
uv sync --extra dev
```

Alternatively create virtual environment and install dependencies:
```bash
uv venv
uv pip install --editable .[dev]
```

#### (Optional) Activate virtual environment

Bash:
```bash
source .venv/bin/activate
```

Windows:
```powershell
.venv\Scripts\activate
```

#### (Optional) Sync with local spark/delta instead of Databricks-connect

```bash
uv sync --extra dev_local
```

### Unit-Tests

```bash
uv run pytest -v
```

Based on whether Databricks Connect (the `dev` default) is enabled or not the Unit-Tests try to use a Databricks Cluster or start a local Spark session with Delta support.
* On Databricks the unit-tests currently assume the catalog `unit_tests` exists (not ideal).

> **Note:** For local Spark Java is required. On Windows Spark/Delta requires HADOOP libraries and generally does not run well.

## Databricks Connect

See https://docs.databricks.com/aws/en/dev-tools/vscode-ext/ for enabling Databricks Connect in VS Code.

## Databricks CLI

1. Install the Databricks CLI from https://docs.databricks.com/dev-tools/cli/databricks-cli.html

2. Authenticate to your Databricks workspace, if you have not done so already:
    ```
    $ databricks configure
    ```

3. To deploy a development copy of this project, type:
    ```
    $ databricks bundle deploy --target dev
    ```

4. Similarly, to deploy a production copy, type:
   ```
   $ databricks bundle deploy --target prod
   ```

## FAQ

* Why no `src` directory?

   Working in Databricks Git Repos automatically adds the root of the Git Repo to Python `sys.path`.

   This way Notebooks in the Git Repo can run `import dbx_example` to import the local Python package during development without explicitly installing the package on the Cluster.

   A Notebook outside the Git Repo can do `import os; os.chdir("/Workspace/Users/...")` to act like it is within the Git Repo.

   Using a `src` directory requires changing the `sys.path` during development (without package installed) in a Databricks Git Repo.
   ```python
   import sys
   sys.path.append("../src")
   ```
* Service Principals

   For this example, the targets `test` and `prod` use a group and service principals.

   The group `group_etl` can manage the workflow, ideally your user and the service principal are part of it. This group should also have sufficient permissions on the used Catalogs.

   Make sure the User used to deploy has `Service principal: User` permissions. `Service principal: Manager` is not enough.
