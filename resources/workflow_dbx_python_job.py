import json

from databricks.bundles.jobs import (
    Job,
    PerformanceTarget,
    PythonWheelTask,
    RunIf,
    Task,
)

from dbx_example import tasks
from resources.constants import (
    DAILY_TRIGGER,
    DEFAULT_ENVIRONMENT,
    ENTRY_POINT,
    PACKAGE_NAME,
    Variables,
)

# Workflow Settings
JOB_NAME = "dbx_python_job"


def create_generic_task(
    task_key: str,
    task_class: tasks.Task,
    depends_on: list[dict] = None,
    run_if: RunIf = None,
):
    """Generic function to create a task with common parameters."""
    task = Task(
        task_key=task_key,
        python_wheel_task=PythonWheelTask(
            package_name=PACKAGE_NAME,
            entry_point=ENTRY_POINT,
            parameters=[
                task_class.get_class_name(),
                "--catalog",
                Variables.catalog_name,
            ],
        ),
        environment_key=DEFAULT_ENVIRONMENT.environment_key,
    )

    if depends_on:
        task.depends_on = depends_on
    if run_if:
        task.run_if = run_if

    return task


py_dbx_example_job = Job(
    name=JOB_NAME,
    trigger=DAILY_TRIGGER,
    tasks=[
        create_generic_task("bronze_taxi", tasks.BronzeTaxiTask),
        create_generic_task("bronze_accuweather", tasks.BronzeAccuweatherTask),
        create_generic_task(
            "silver_nyctaxi_aggregate",
            tasks.SilverTaxiAggTask,
            depends_on=[{"task_key": "bronze_taxi"}, {"task_key": "bronze_accuweather"}],
            run_if=RunIf.ALL_SUCCESS,
        ),
    ],
    environments=[DEFAULT_ENVIRONMENT],
    performance_target=PerformanceTarget.STANDARD,
)


if __name__ == "__main__":
    print(json.dumps(py_dbx_example_job.as_dict(), indent=2))
