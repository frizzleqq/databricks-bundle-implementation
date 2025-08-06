"""Operations for Delta tables using PySpark."""

from dataclasses import dataclass
from typing import Optional, Union

from delta import DeltaTable
from pydantic import BaseModel, Field
from pyspark.sql import DataFrame
from pyspark.sql import types as T

from dab_project.spark import get_spark


@dataclass
class GeneratedColumn:
    name: str
    type: Union[str, T.DataType]
    expression: str


class DeltaWorker(BaseModel):
    table_name: str = Field(
        ...,
        description="Table name.",
    )
    schema_name: str = Field(
        ...,
        description="Schema name.",
    )
    catalog_name: Optional[str] = Field(
        default=None,
        description="Catalog name. "
        "Note: Can be ignored if using a SparkCatalog that does not support catalog notation (e.g. Hive)",
    )

    @property
    def full_table_name(self) -> str:
        return ".".join([n for n in [self.catalog_name, self.schema_name, self.table_name] if n])

    @property
    def full_schema_name(self) -> str:
        return ".".join([n for n in [self.catalog_name, self.schema_name] if n])

    @property
    def delta_table(self) -> DeltaTable:
        return DeltaTable.forName(get_spark(), self.full_table_name)

    @property
    def df(self) -> DataFrame:
        return get_spark().table(self.full_table_name)

    def create_schema_if_not_exists(self) -> None:
        """Create schema if it does not exist."""
        get_spark().sql(f"CREATE SCHEMA IF NOT EXISTS {self.full_schema_name}")

    def create_table_if_not_exists(
        self,
        schema: T.StructType,
        cluster_by: Optional[list] = None,
        partition_by: Optional[list] = None,
        table_properties: Optional[dict] = None,
        generated_columns: Optional[list[GeneratedColumn]] = None,
    ) -> DeltaTable:
        """Create a Delta table if it does not exist.

        Parameters
        ----------
        schema: StructType
            Column-schema of the table.
        cluster_by: Optional[list]
            Columns to cluster the table by.
        partition_by: Optional[list]
            Columns to partition the table by.
        table_properties: Optional[dict]
            Delta table properties.
        generated_columns: Optional[list[GeneratedColumn]]
            Generated columns of the table.

        Returns
        -------
        DeltaTable
            The Delta table object.
        """
        if cluster_by and partition_by:
            raise ValueError("Clustering and partitioning cannot both be specified")

        cluster_by = cluster_by or []
        partition_by = partition_by or []
        table_properties = table_properties or {}
        generated_columns = generated_columns or []

        table_builder = (
            DeltaTable.createIfNotExists(get_spark())
            .tableName(self.full_table_name)
            .addColumns(schema)
        )
        for column in generated_columns:
            table_builder.addColumn(column.name, column.type, generatedAlwaysAs=column.expression)
        if cluster_by:
            table_builder = table_builder.clusterBy(*cluster_by)
        if partition_by:
            table_builder = table_builder.partitionedBy(*partition_by)

        for k, v in table_properties.items():
            table_builder = table_builder.property(k, v)

        self.create_schema_if_not_exists()

        return table_builder.execute()

    def optimize(self, where: Optional[str] = None) -> DataFrame:
        """
        Optimize the Delta table.

        Parameters
        ----------
        where : Optional[str], optional
            Filter condition for optimization, by default None

        Returns
        -------
        DataFrame
            DataFrame containing the OPTIMIZE execution metrics
        """
        if where:
            return self.delta_table.optimize().where(where).executeCompaction()
        else:
            return self.delta_table.optimize().executeCompaction()

    def drop_table_if_exists(self) -> None:
        get_spark().sql(f"DROP TABLE IF EXISTS {self.full_table_name}")

    def write(self, df: DataFrame, mode: str = "append", options: Optional[dict] = None):
        """
        Write dataframe to delta table.

        Parameters
        ----------
        df : DataFrame
            the dataframe to be written
        mode : str, optional
            the write mode, by default "append"
        options : Optional[dict], optional
            additional options, by default None

        Returns
        -------
        None
        """
        if options is None:
            options = {}
        return (
            df.write.format("delta").mode(mode).options(**options).saveAsTable(self.full_table_name)
        )
