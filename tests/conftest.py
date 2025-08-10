import shutil
import tempfile
import uuid
from pathlib import Path
from typing import Generator, Optional

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
        spark = DatabricksSession.builder.serverless(True).getOrCreate()
        yield spark
    else:
        # If databricks-connect is not installed, we use use local Spark session
        warehouse_dir = tempfile.mkdtemp()
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
def catalog_name() -> Optional[str]:
    """Fixture to provide the catalog name for tests.

    In Databricks, we use the "lake_dev" catalog.
    Locally we run without a catalog, so we return None.
    """
    if DATABRICKS_CONNECT_AVAILABLE:
        return "lake_dev"
    else:
        return None


@pytest.fixture(scope="module")
def create_schema(spark, catalog_name, request) -> Generator[str, None, None]:
    """Fixture to provide a schema for tests.

    Creates a schema with a random name prefixed with the test module name and cleans it up after tests.
    """
    module_name = request.module.__name__.split(".")[-1]  # Get just the module name without path
    schema_name = f"pytest_{module_name}_{uuid.uuid4().hex[:8]}"

    if catalog_name is not None:
        full_schema_name = f"{catalog_name}.{schema_name}"
    else:
        full_schema_name = schema_name

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema_name}")
    yield schema_name
    spark.sql(f"DROP SCHEMA IF EXISTS {full_schema_name} CASCADE")


@pytest.fixture(scope="function")
def table_name(request) -> str:
    """Fixture to provide a table name based on the test function name."""
    return request.node.name
