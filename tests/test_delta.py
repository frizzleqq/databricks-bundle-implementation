from pyspark.sql import types as T

from dbx_example.delta import DeltaWorker

# @pytest.fixture(scope="module", autouse=True)
# def drop_schema_cascade(spark, catalog_name):
#     schema_name = __name__
#     if catalog_name:
#         schema_name = f"{catalog_name}.{schema_name}"
#         spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{schema_name} CASCADE")
#     else:
#         schema_name = f"{schema_name}"
#         spark.sql(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")


def test_deltawriter_create_table_if_not_exists(spark, catalog_name, request):
    schema = T.StructType(
        [
            T.StructField("key", T.IntegerType()),
            T.StructField("value", T.IntegerType()),
        ]
    )
    delta_writer = DeltaWorker(
        catalog_name=catalog_name,
        schema_name=__name__,
        table_name=f"table_{request.node.name}",
    )

    delta_writer.drop_table_if_exists()
    delta_writer.create_table_if_not_exists(schema)
    assert delta_writer.delta_table is not None
    assert delta_writer.df.columns == [
        "key",
        "value",
    ]
