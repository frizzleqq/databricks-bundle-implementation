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

    # Without this additional check, the deployed Python Wheel Task on serverless failed
    # Strangely the DAB examples do not include this.
    spark = SparkSession.getActiveSession()
    if spark is not None:
        return spark

    try:
        from databricks.connect import DatabricksSession  # type: ignore  # noqa: I001

        return DatabricksSession.builder.getOrCreate()
        # return DatabricksSession.builder.serverless(True).getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate()
