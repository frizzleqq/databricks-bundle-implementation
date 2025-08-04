# TODO: test with newest sdk (using uvx)

from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import BadRequest, ResourceConflict
from databricks.sdk.service.catalog import CatalogInfo
from databricks.sdk.service.iam import Group, ServicePrincipal


def get_or_create_catalog(
    catalog_name: str,
    storage_root: Optional[str] = None,
) -> CatalogInfo:
    """Create Unity Catalog if it does not yet exist.

    Parameters
    ----------
    catalog_name: str
        Databricks Catalog name.
    storage_root: Optional[str]
        Storage root URL for managed tables within catalog.

    Returns
    -------
    CatalogInfo
        catalog info object
    """
    w = WorkspaceClient()
    try:
        c = w.catalogs.create(name=catalog_name, storage_root=storage_root)
        print(f"Catalog {catalog_name} created successfully.")
        return c
    except BadRequest as e:
        if e.error_code == "CATALOG_ALREADY_EXISTS":
            print(f"Catalog {catalog_name} already exists.")
            return w.catalogs.get(catalog_name)
        raise


def get_or_create_group(display_name: str) -> Group:
    """Create a group if it does not yet exist.

    Parameters
    ----------
    display_name: str
        Name of the group to create.

    Returns
    -------
    Group
        Group object.
    """
    w = WorkspaceClient()
    try:
        g = w.groups.create(display_name=display_name)
        print(f"Group '{display_name}' created successfully.")
        return g
    except ResourceConflict:
        print(f"Group '{display_name}' already exists.")
        return _get_group(display_name)


def get_or_create_service_principal(
    display_name: str, application_id: str
) -> ServicePrincipal:
    """Create a service principal if it does not yet exist.

    Parameters
    ----------
    display_name: str
        Display name of the service principal.
    application_id: str
        Application ID of the service principal.

    Returns
    -------
    ServicePrincipal
        Service principal object.
    """
    w = WorkspaceClient()
    try:
        sp = w.service_principals.create(
            active=True,
            display_name=display_name,
            application_id=application_id,
        )
        print(f"Service Principal '{display_name}' created successfully.")
        return sp
    except ResourceConflict:
        print(f"Service Principal '{display_name}' already exists.")
        return _get_service_principal(display_name)


def _get_group(
    display_name: str,
) -> Group:
    """Get the group ID for a given group name.

    Parameters
    ----------
    display_name: str
        Name of the group to retrieve.

    Returns
    -------
    Group
        Group object.
    """
    w = WorkspaceClient()
    for g in w.groups.list():
        if g.display_name == display_name:
            return w.groups.get(g.id)
    raise ValueError(f"Group '{display_name}' not found.")


def _get_service_principal(
    display_name: str,
) -> ServicePrincipal:
    """Get the service principal ID for a given display name.

    Parameters
    ----------
    display_name: str
        Display name of the service principal to retrieve.

    Returns
    -------
    ServicePrincipal
        Service principal object.
    """
    w = WorkspaceClient()
    for sp in w.service_principals.list():
        if sp.display_name == display_name:
            return w.service_principals.get(sp.id)
    raise ValueError(f"Service Principal '{display_name}' not found.")

### Catalogs

# Workspace exists on creation of Databricks Free edition
# ERROR: Please use the UI to create a catalog with Default Storage

# workspace_storage_root = w.catalogs.get("workspace").storage_root
# get_or_create_catalog("lake_dev", storage_root=workspace_storage_root)
# get_or_create_catalog("lake_test", storage_root=workspace_storage_root)
# get_or_create_catalog("lake_prod", storage_root=workspace_storage_root)

### Groups

get_or_create_group("group_etl")
get_or_create_group("group_reader")


### Service Principals

get_or_create_service_principal(
    display_name="sp_etl_dev", application_id="280a0e2e-369a-440f-8bf1-8da8c975e077"
)
get_or_create_service_principal(
    display_name="sp_etl_prod", application_id="255b38e1-a8ec-40cf-8e27-e640276bef5d"
)
