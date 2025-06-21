from dbx_example.delta import DeltaWorker
from dbx_example.tasks.base_task import Task


class BronzeTaxiTask(Task):
    """
    Ingest `samples.nyctaxi.trips` from the Databricks sample data into a bronze table.
    """

    def _write_data(self, catalog_name: str) -> None:
        # Use Databricks sample data for demonstration
        df = self.spark.read.table("samples.nyctaxi.trips")

        target_table = DeltaWorker(
            catalog_name=catalog_name,
            schema_name="bronze",
            table_name="nyctaxi_trips",
        )

        target_table.create_table_if_not_exists(df.schema)
        target_table.write(df, mode="overwrite")
