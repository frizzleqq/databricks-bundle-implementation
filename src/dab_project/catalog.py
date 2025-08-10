"""Utilities using Databricks SDK for Unity Catalog operations."""

from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import BadRequest
from databricks.sdk.service.catalog import CatalogInfo, SchemaInfo, TableInfo


def create_schema_if_not_exists(
    catalog_name: str,
    schema_name: str,
    storage_root: Optional[str] = None,
) -> SchemaInfo:
    """Create Unity Catalog schema if it does not yet exist.

    Parameters
    ----------
    catalog_name: str
        Databricks Catalog name.
    schema_name: str
        Schema name.
    storage_root: Optional[str]
        Storage root URL for managed tables within schema

    Returns
    -------
    SchemaInfo
        schema info object
    """
    w = WorkspaceClient()
    try:
        return w.schemas.create(
            name=schema_name, catalog_name=catalog_name, storage_root=storage_root
        )
    except BadRequest as e:
        if e.error_code == "SCHEMA_ALREADY_EXISTS":
            return w.schemas.get(f"{catalog_name}.{schema_name}")
        raise


def get_table_info(catalog_name: str, schema_name: str, table_name: str) -> TableInfo:
    """Get a TableInfo object from Unity Catalog.

    Parameters
    ----------
    catalog_name: str
        Databricks Catalog name.
    schema_name: str
        Schema name.
    table_name: str
        Table name.

    Returns
    -------
    TableInfo
        table info object from Unity Catalog
    """
    w = WorkspaceClient()
    return w.tables.get(full_name=f"{catalog_name}.{schema_name}.{table_name}")


def list_catalogs() -> list[CatalogInfo]:
    w = WorkspaceClient()
    return w.catalogs.list()


def list_schemas(catalog_name: str) -> list[SchemaInfo]:
    """
    Get a list of schemas in the specified catalog.

    Parameters
    ----------
    catalog_name : str
        The name of the catalog.

    Returns
    -------
    list[SchemaInfo]
        A list of SchemaInfo objects representing the schemas in the catalog.
    """
    w = WorkspaceClient()
    return list(w.schemas.list(catalog_name=catalog_name))


def list_tables(catalog_name: str, schema_name: str) -> list[TableInfo]:
    """
    Get a list of tables in the specified schema and catalog.

    Parameters
    ----------
    catalog_name : str
        The name of the catalog.
    schema_name : str
        The name of the schema.

    Returns
    -------
    list[TableInfo]
        A list of TableInfo objects representing the tables in the schema.
    """
    w = WorkspaceClient()
    return list(w.tables.list(catalog_name=catalog_name, schema_name=schema_name))
