"""This file contains constants used across worfklows"""

from databricks.bundles.core import Variable, variables
from databricks.bundles.jobs import (
    Environment,
    JobEnvironment,
    PeriodicTriggerConfiguration,
    TriggerSettings,
)

PACKAGE_NAME = "dbx_example"
ENTRY_POINT = "dbx-example"


@variables
class Variables:
    catalog_name: Variable[str]
    serverless_environment_version: Variable[str]


DEFAULT_ENVIRONMENT = JobEnvironment(
    environment_key="default",
    spec=Environment(
        client=Variables.serverless_environment_version, dependencies=["./dist/*.whl"]
    ),
)

DAILY_TRIGGER = TriggerSettings(periodic=PeriodicTriggerConfiguration(interval=1, unit="DAYS"))
