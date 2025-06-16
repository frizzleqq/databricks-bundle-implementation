"""Utilities based on Databricks SDK."""

import json
import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.dbutils import RemoteDbUtils


def get_databricks_runtime() -> str:
    """
    Get Databricks Runtime version.

    Returns
    -------
    str
        Databricks Runtime version.
    """
    return os.environ.get("DATABRICKS_RUNTIME_VERSION", "NOT_DATABRICKS")


def get_dbutils() -> RemoteDbUtils:
    """
    Get dbutils from databricks SDK.

    https://databricks-sdk-py.readthedocs.io/en/latest/dbutils.html

    Returns
    -------
    RemoteDbUtils
        dbutils
    """
    return WorkspaceClient().dbutils


def get_notebook_context() -> dict:
    """
    Get Databricks context as a json string.

    Returns
    -------
    dict
        json string containing the databricks context.
    """
    return json.loads(
        get_dbutils().notebook.entry_point.getDbutils().notebook().getContext().safeToJson()
    )


def is_databricks_runtime() -> bool:
    """
    Check if the code is running in a Databricks environment.

    Returns
    -------
    bool
        True if running in Databricks, False otherwise.
    """
    return "DATABRICKS_RUNTIME_VERSION" in os.environ
