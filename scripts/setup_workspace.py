# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "databricks-sdk==0.61.0",
#   "databricks-connect~=16.3.0",
# ]
# ///
"""Sets up the Databricks Workspace with necessary catalogs, groups and service principals.

* Uses both Databricks SDK and Databricks Serverless to perform operations.
* Serverless as not all operations seemd to be supported by the SDK (could be a Free Edition limitation).

Items created:
- Groups: `group_etl`, `group_reader`
- Service Principals: `sp_etl_dev`, `sp_etl_prod`
- Catalogs: `lake_dev`, `lake_test`, `lake_prod`

Permissions granted:
- `group_etl` and `sp_etl_dev` have ALL PRIVILEGES on `lake_dev` and `lake_test`.
- `group_etl` and `sp_etl_prod` have ALL PRIVILEGES on `lake_prod`.
- Current user is added to `group_etl`.
"""

from typing import Optional

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import BadRequest, ResourceConflict
from databricks.sdk.service.catalog import CatalogInfo
from databricks.sdk.service.iam import Group, ServicePrincipal


def add_current_user_to_group(spark, group_name: str) -> None:
    w = WorkspaceClient()
    emails = w.current_user.me().emails
    if emails:
        email = emails[0].value
        spark.sql(f"ALTER GROUP `{group_name}` ADD USER `{email}`;")
        print(f"Added current user ('{email}') to group '{group_name}'.")
    else:
        print(f"No email found for current user. Cannot add to group '{group_name}'.")


def create_catalog(spark: DatabricksSession, catalog_name: str) -> None:
    """Create a catalog if it does not yet exist.

    Parameters
    ----------
    spark: DatabricksSession
        Spark session to execute SQL commands.
    catalog_name: str
        Name of the catalog to create.
    """
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog_name}`")
    print(f"Catalog '{catalog_name}' created.")


def grant_catalog_privileges(
    spark: DatabricksSession,
    catalog_name: str,
    identity: str,
    privileges: str,
) -> None:
    """Grant all privileges on a catalog to a specified entity.

    Parameters
    ----------
    spark: DatabricksSession
        Spark session to execute SQL commands.
    catalog_name: str
        Name of the catalog.
    identity: str
        Identity (group name or service principal AppId) to grant privileges to.
    privileges: str
        Privileges to grant.
    """
    spark.sql(f"GRANT {privileges} ON CATALOG `{catalog_name}` TO `{identity}`")
    print(f"Granted {privileges} on catalog '{catalog_name}' to '{identity}'.")


def get_or_create_catalog(
    catalog_name: str,
    storage_root: Optional[str] = None,
) -> CatalogInfo:
    """Create Unity Catalog if it does not yet exist.

    This did not work on Databricks Free Edition using the Default Storage
    of the workspace.

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


def get_or_create_service_principal(display_name: str, application_id: str) -> ServicePrincipal:
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


### Groups

group_etl = get_or_create_group("group_etl")
group_reader = get_or_create_group("group_reader")


### Service Principals

sp_etl_dev = get_or_create_service_principal(
    display_name="sp_etl_dev", application_id="280a0e2e-369a-440f-8bf1-8da8c975e077"
)
sp_etl_prod = get_or_create_service_principal(
    display_name="sp_etl_prod", application_id="255b38e1-a8ec-40cf-8e27-e640276bef5d"
)


### Databricks Serverless Session

print("Starting Databricks Serverless Spark session...")
spark = DatabricksSession.builder.serverless(True).getOrCreate()

### Catalogs

# Creating catalog with "default storage" (Free Edition) did not work:
#   ERROR: Please use the UI to create a catalog with Default Storage
# workspace_storage_root = WorkspaceClient().catalogs.get("workspace").storage_root
# get_or_create_catalog("lake_dev")
# get_or_create_catalog("lake_test")
# get_or_create_catalog("lake_prod")

create_catalog(spark, "lake_dev")
create_catalog(spark, "lake_test")
create_catalog(spark, "lake_prod")

### Catalog Permissions

grant_catalog_privileges(spark, "lake_dev", "group_etl", privileges="ALL PRIVILEGES")
grant_catalog_privileges(spark, "lake_dev", sp_etl_dev.application_id, privileges="ALL PRIVILEGES")
grant_catalog_privileges(spark, "lake_test", "group_etl", privileges="ALL PRIVILEGES")
grant_catalog_privileges(spark, "lake_test", sp_etl_dev.application_id, privileges="ALL PRIVILEGES")
grant_catalog_privileges(spark, "lake_prod", "group_etl", privileges="ALL PRIVILEGES")
grant_catalog_privileges(
    spark, "lake_prod", sp_etl_prod.application_id, privileges="ALL PRIVILEGES"
)

### Group Membership

add_current_user_to_group(spark, "group_etl")

print("Stopping Databricks Serverless Spark session...")
spark.stop()
