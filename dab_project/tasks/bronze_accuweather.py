from dab_project.delta import DeltaWorker
from dab_project.tasks.base_task import Task


class BronzeAccuweatherTask(Task):
    """
    Ingest `samples.accuweather.forecast_daily_calendar_metric` from the Databricks sample data into a bronze table.
    """

    def _write_data(self, catalog_name: str) -> None:
        # Use Databricks sample data for demonstration
        df = self.spark.read.table("samples.accuweather.forecast_daily_calendar_metric")

        target_table = DeltaWorker(
            catalog_name=catalog_name,
            schema_name="bronze_accuweather",
            table_name="forecast_daily_calendar_metric",
        )

        target_table.create_table_if_not_exists(df.schema)
        target_table.write(df, mode="overwrite")
