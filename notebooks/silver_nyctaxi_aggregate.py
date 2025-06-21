# Databricks notebook source
from pyspark.sql import functions as F

from dbx_example import catalog
from dbx_example.delta import DeltaWorker

# COMMAND ----------

dbutils.widgets.text("catalog_name", "lake_dev")

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")

# COMMAND ----------

target_table = DeltaWorker(
    catalog_name=catalog_name,
    schema_name="silver",
    table_name="nyctaxi_aggregate",
)

# COMMAND ----------

target_table.drop_table_if_exists()

# COMMAND ----------

bronze_table = DeltaWorker(
    catalog_name=catalog_name,
    schema_name="bronze_nyctaxi",
    table_name="trips",
)

# COMMAND ----------

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

# COMMAND ----------

target_table.create_table_if_not_exists(df_trips_grouped.schema)
target_table.write(df_trips_grouped, mode="overwrite")

# COMMAND ----------

catalog.get_table_info(target_table.catalog_name, target_table.schema_name, target_table.table_name)
