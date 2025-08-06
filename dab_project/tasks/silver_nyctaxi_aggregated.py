from pyspark.sql import functions as F

from dab_project.delta import DeltaWorker
from dab_project.tasks.base_task import Task


class SilverTaxiAggTask(Task):
    """
    Aggregate `bronze_nyctaxi.trips` into a silver table `silver.nyctaxi_aggregate`.
    """

    def _perform_task(self, catalog_name: str) -> None:
        # Use Databricks sample data for demonstration
        bronze_table = DeltaWorker(
            catalog_name=catalog_name,
            schema_name="bronze_nyctaxi",
            table_name="trips",
        )

        target_table = DeltaWorker(
            catalog_name=catalog_name,
            schema_name="silver",
            table_name="nyctaxi_aggregate",
        )

        df_trips_grouped = (
            bronze_table.df.withColumn("pickup_date", F.to_date(F.col("tpep_pickup_datetime")))
            .withColumn("dropoff_date", F.to_date(F.col("tpep_dropoff_datetime")))
            .groupBy("pickup_date", "dropoff_date")
            .agg(
                F.count("*").alias("trip_count"),
                F.sum("trip_distance").alias("total_trip_distance"),
                F.sum("fare_amount").alias("total_fare_amount"),
            )
        )

        target_table.create_table_if_not_exists(df_trips_grouped.schema)
        target_table.write(df_trips_grouped, mode="overwrite")
