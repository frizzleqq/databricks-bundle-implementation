from pyspark.sql import types as T

from dbx_example import catalog
from dbx_example.delta import DeltaWorker


def test_deltawriter_create_table_if_not_exists(spark, catalog_name, request):
    catalog.create_schema_if_not_exists(
        catalog_name=catalog_name,
        schema_name=f"schema_{request.node.name}",
    )

    schema = T.StructType(
        [
            T.StructField("key", T.IntegerType()),
            T.StructField("value", T.IntegerType()),
        ]
    )
    delta_writer = DeltaWorker(
        catalog_name=catalog_name,
        schema_name=f"schema_{request.node.name}",
        table_name=f"table_{request.node.name}",
    )
    delta_writer.create_table_if_not_exists(schema)
    assert [
        "key",
        "value",
    ] == delta_writer.delta_table.toDF().columns
    # assert [] == table.detail().collect()[0]["partitionColumns"]
    # assert table.detail().collect()[0]["properties"].get("delta.enableChangeDataFeed") is None
