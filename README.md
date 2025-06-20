# Databricks Bundle Example

This project is an example of a Databricks Asset bundle that deploys the following:
* Python Project as Wheel
* Python Notebooks (TODO)
* Databricks Workflow examples
   * Python Wheel Task
   * Notebook Task (TODO)
   * Python based workflow (TODO)

Uses Databricks Free Edition: https://www.databricks.com/learn/free-edition
* This seems to use Clusters with Databricks Runtime 15.1, which the dependencies are based on

## TODO:

* README (local vs Databricks Connect)
* Logging
   * Logging to volume?
* Catalog parameter in Workflow
* Python based workflow
* Workflow with Notebook tasks
* Workflow calling Workflows

## Development

### Requirements

* uv: https://docs.astral.sh/uv/getting-started/installation/
* Databricks CLI: https://docs.databricks.com/aws/en/dev-tools/cli/install

### Setup environment

Create virtual environment
```bash
uv venv
```

Install Python dependencies:
```bash
uv pip install --editable .[dev]
```

Alternatively sync entire `uv` environment:
```bash
uv sync --extra dev
```

### Activate virtual environment

Bash:
```bash
source .venv/bin/activate
```

Windows:
```powershell
.venv\Scripts\activate
```

## Databricks Connect

Example `.databrickscfg` configuration for connecting to Serverless Clusters:
```
[DEFAULT]
host  = https://....databricks.com
token = ...
serverless_compute_id = auto
```

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

   Make sure the User used to deploy has `Service principal: User` permissions. `Service principal: Manager` is not enough.
