from dab_project.delta import DeltaWorker
from dab_project.tasks.base_task import Task


def generate_test_task(schema_name: str, table_name: str):
    class TestTask(Task):
        def _perform_task(self, catalog_name: str) -> None:
            df = self.spark.createDataFrame([(1, "test")], schema=["id", "name"])

            target_table = DeltaWorker(
                catalog_name=catalog_name,
                schema_name=schema_name,
                table_name=table_name,
            )

            target_table.drop_table_if_exists()

            target_table.create_table_if_not_exists(df.schema)
            target_table.write(df, mode="append")

    return Task.create_task_factory("TestTask")


def test_etl_task_run(spark, catalog_name, create_schema, table_name):
    task = generate_test_task(
        schema_name=create_schema,
        table_name=table_name,
    )
    task.run(catalog_name)

    # Verify that the data was written to the Delta table
    delta_table = DeltaWorker(
        catalog_name=catalog_name,
        schema_name=create_schema,
        table_name=table_name,
    )

    assert task.get_class_name() == "TestTask"
    assert task.name == "TestTask"

    assert delta_table.df.columns == ["id", "name"]
    assert delta_table.df.count() == 1
