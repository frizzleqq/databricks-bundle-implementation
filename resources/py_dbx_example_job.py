import json

from databricks.bundles.core import Variable, variables
from databricks.bundles.jobs import (
    Environment,
    Job,
    JobEnvironment,
    PeriodicTriggerConfiguration,
    PythonWheelTask,
    Task,
    TriggerSettings,
)

from dbx_example import tasks


@variables
class Variables:
    catalog_name: Variable[str]


default_environment = JobEnvironment(
    environment_key="default", spec=Environment(client="2", dependencies=["./dist/*.whl"])
)

py_dbx_example_job = Job(
    name="py_dbx_example_job",
    trigger=TriggerSettings(periodic=PeriodicTriggerConfiguration(interval=1, unit="DAYS")),
    tasks=[
        Task(
            task_key="bronze_taxi",
            python_wheel_task=PythonWheelTask(
                package_name="dbx_example",
                entry_point="dbx-example",
                parameters=[
                    tasks.BronzeTaxiTask.get_class_name(),
                    "--catalog",
                    Variables.catalog_name,
                ],
            ),
            environment_key="default",
        ),
        Task(
            task_key="bronze_accuweather",
            python_wheel_task=PythonWheelTask(
                package_name="dbx_example",
                entry_point="dbx-example",
                parameters=[
                    tasks.BronzeAccuweatherTask.get_class_name(),
                    "--catalog",
                    Variables.catalog_name,
                ],
            ),
            environment_key="default",
        ),
        Task(
            task_key="silver_nyctaxi_aggregate",
            python_wheel_task=PythonWheelTask(
                package_name="dbx_example",
                entry_point="dbx-example",
                parameters=[
                    tasks.SilverTaxiAggTask.get_class_name(),
                    "--catalog",
                    Variables.catalog_name,
                ],
            ),
            depends_on=[{"task_key": "bronze_taxi"}, {"task_key": "bronze_accuweather"}],
            run_if="ALL_SUCCESS",
            environment_key="default",
        ),
    ],
    environments=[default_environment],
)


if __name__ == "__main__":
    print(json.dumps(py_dbx_example_job.as_dict(), indent=2))
