import shutil
import tempfile
from pathlib import Path
from typing import Generator, Optional
from unittest.mock import patch

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

try:
    from databricks.connect import DatabricksSession  # type: ignore  # noqa: I001

    DATABRICKS_CONNECT_AVAILABLE = True
except ImportError:
    DATABRICKS_CONNECT_AVAILABLE = False


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    if DATABRICKS_CONNECT_AVAILABLE:
        # For serverless compute, we need to set the flag explicitly
        yield DatabricksSession.builder.serverless(True).getOrCreate()
    else:
        # If databricks-connect is not installed, we use use local Spark session
        warehouse_dir = tempfile.TemporaryDirectory().name
        _builder = (
            SparkSession.builder.master("local[*]")
            .config("spark.hive.metastore.warehouse.dir", Path(warehouse_dir).as_uri())
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            # https://github.com/delta-io/delta/blob/master/python/delta/testing/utils.py
            .config("spark.databricks.delta.snapshotPartitions", 2)
            .config("spark.sql.shuffle.partitions", 5)
            .config("delta.log.cacheSize", 3)
            .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", 5)
        )
        spark = configure_spark_with_delta_pip(_builder).getOrCreate()
        yield spark
        spark.stop()
        if Path(warehouse_dir).exists():
            shutil.rmtree(warehouse_dir)


@pytest.fixture(scope="session")
def catalog_name() -> Generator[Optional[str], None, None]:
    """Fixture to provide the catalog name for tests.

    In Databricks, we use the "unit_tests" catalog.
    Locally we run without a catalog, so we return None.
    """
    if DATABRICKS_CONNECT_AVAILABLE:
        yield "unit_tests"
    else:
        yield None


@pytest.fixture(autouse=True)
def mock_catalog_create_schema(spark: SparkSession):
    """Mock catalog.create_schema_if_not_exists to use Spark SQL instead."""

    def create_schema_with_spark_sql(catalog_name, schema_name: str):
        """Create schema using Spark SQL CREATE SCHEMA IF NOT EXISTS."""
        if catalog_name:
            full_schema_name = f"{catalog_name}.{schema_name}"
        else:
            full_schema_name = schema_name

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema_name}")

    if not DATABRICKS_CONNECT_AVAILABLE:
        # Mock the catalog.create_schema_if_not_exists method
        with patch(
            "dbx_example.catalog.create_schema_if_not_exists",
            side_effect=create_schema_with_spark_sql,
        ):
            yield
    else:
        # If running on Databricks, we don't need to mock anything
        yield
