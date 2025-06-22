import json

from databricks.bundles.core import Variable, variables
from databricks.bundles.jobs import (
    Job,
    PythonWheelTask,
    RunIf,
    Task,
)

from dbx_example import tasks

from .constants import DAILY_TRIGGER, DEFAULT_ENVIRONMENT, ENTRY_POINT, PACKAGE_NAME

# Workflow Settings
JOB_NAME = "py_dbx_example_job"


@variables
class Variables:
    catalog_name: Variable[str]


py_dbx_example_job = Job(
    name=JOB_NAME,
    trigger=DAILY_TRIGGER,
    tasks=[
        Task(
            task_key="bronze_taxi",
            python_wheel_task=PythonWheelTask(
                package_name=PACKAGE_NAME,
                entry_point=ENTRY_POINT,
                parameters=[
                    tasks.BronzeTaxiTask.get_class_name(),
                    "--catalog",
                    Variables.catalog_name,
                ],
            ),
            environment_key=DEFAULT_ENVIRONMENT.environment_key,
        ),
        Task(
            task_key="bronze_accuweather",
            python_wheel_task=PythonWheelTask(
                package_name=PACKAGE_NAME,
                entry_point=ENTRY_POINT,
                parameters=[
                    tasks.BronzeAccuweatherTask.get_class_name(),
                    "--catalog",
                    Variables.catalog_name,
                ],
            ),
            environment_key=DEFAULT_ENVIRONMENT.environment_key,
        ),
        Task(
            task_key="silver_nyctaxi_aggregate",
            python_wheel_task=PythonWheelTask(
                package_name=PACKAGE_NAME,
                entry_point=ENTRY_POINT,
                parameters=[
                    tasks.SilverTaxiAggTask.get_class_name(),
                    "--catalog",
                    Variables.catalog_name,
                ],
            ),
            depends_on=[{"task_key": "bronze_taxi"}, {"task_key": "bronze_accuweather"}],
            run_if=RunIf.ALL_SUCCESS,
            environment_key=DEFAULT_ENVIRONMENT.environment_key,
        ),
    ],
    environments=[DEFAULT_ENVIRONMENT],
)


if __name__ == "__main__":
    print(json.dumps(py_dbx_example_job.as_dict(), indent=2))
