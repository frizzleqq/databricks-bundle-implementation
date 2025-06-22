"""This file contains constants used across worfklows"""

from databricks.bundles.jobs import (
    Environment,
    JobEnvironment,
    PeriodicTriggerConfiguration,
    TriggerSettings,
)

PACKAGE_NAME = "dbx_example"
ENTRY_POINT = "dbx-example"

DEFAULT_ENVIRONMENT = JobEnvironment(
    environment_key="default", spec=Environment(client="2", dependencies=["./dist/*.whl"])
)

DAILY_TRIGGER = TriggerSettings(periodic=PeriodicTriggerConfiguration(interval=1, unit="DAYS"))
