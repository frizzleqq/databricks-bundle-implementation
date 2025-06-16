from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    """
    Get the running Spark session.

    If running in Databricks or using databricks-connect, use the DatabricksSession.
    Otherwise, try the default SparkSession.

    Returns
    -------
    SparkSession
        The current Spark session.
    """
    try:
        from databricks.connect import DatabricksSession  # type: ignore  # noqa: I001

        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate()
