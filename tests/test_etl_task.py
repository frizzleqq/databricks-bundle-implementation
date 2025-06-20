from dbx_example.delta import DeltaWorker
from dbx_example.etl_task import Task


def generate_test_task(catalog_name: str, schema_name: str, table_name: str):
    class TestTask(Task):
        def _write_data(self):
            df = self.spark.createDataFrame([(1, "test")], schema=["id", "name"])

            target_table = DeltaWorker(
                catalog_name=catalog_name,
                schema_name=schema_name,
                table_name=table_name,
            )

            target_table.create_table_if_not_exists(df.schema)
            target_table.write(df, mode="overwrite")

    return Task.create_etl_task("TestTask")


def test_etl_task_run(spark, catalog_name, request):
    task = generate_test_task(
        catalog_name=catalog_name,
        schema_name=__name__,
        table_name=f"table_{request.node.name}",
    )
    task.run()

    # Verify that the data was written to the Delta table
    delta_table = DeltaWorker(
        catalog_name=catalog_name,
        schema_name=__name__,
        table_name=f"table_{request.node.name}",
    )

    assert delta_table.df.columns == ["id", "name"]
    assert delta_table.df.count() == 1
