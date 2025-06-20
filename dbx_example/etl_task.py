import logging
from abc import ABC, abstractmethod
from typing import Optional

from pyspark.sql import SparkSession

from dbx_example.delta import DeltaWorker
from dbx_example.spark import get_spark


class Task(ABC):
    """
    Abstract base class for ETL tasks.

    Provides a standard execution way for child classes to implement
    actual data processing.
    """

    @classmethod
    def create_etl_task(cls, task_name: str) -> "Task":
        """
        Factory method to create a task instance by type.

        Parameters
        ----------
        task_name : str
            Identifier for the task class name to create

        Returns
        -------
        ETLTask
            An instance of the requested task type
        """
        # Find the requested task class by name
        for subclass in cls.__subclasses__():
            if subclass.__name__ == task_name:
                return subclass()

        raise ValueError(f"Unknown EtlTask: '{task_name}'. Available types: {cls.__subclasses__()}")

    def __init__(self):
        """
        Initialize an ETL task.
        """
        self.logger = logging.getLogger(__name__)
        self.spark: SparkSession

    @property
    def name(self) -> str:
        return self.__class__.__name__

    def run(self, catalog_name: str) -> None:
        """
        Execute the ETL task with proper logging.
        """
        self._log_start()
        self.spark = get_spark()
        try:
            self._write_data(catalog_name=catalog_name)
            self._log_exit(success=True)
        except Exception as e:
            self._log_exit(success=False, error=e)
            raise

    def _log_start(self) -> None:
        self.logger.info(f"Starting ETL task: {self.name}")

    def _log_exit(self, success: bool, error: Optional[Exception] = None) -> None:
        if success:
            self.logger.info(f"Completed ETL task: {self.name}")
        else:
            self.logger.error(f"Failed ETL task: {self.name} - {str(error)}")

    @abstractmethod
    def _write_data(self, catalog_name: str):
        """
        Process and write data.

        This method must be implemented by child classes.
        """
        pass


class BronzeTaxiTask(Task):
    """
    Ingest from the Databricks sample data into a bronze table.
    """

    def _write_data(self, catalog_name: str) -> None:
        # Use Databricks sample data for demonstration
        df = self.spark.read.table("samples.nyctaxi.trips")

        target_table = DeltaWorker(
            catalog_name=catalog_name,
            schema_name="bronze",
            table_name="nyctaxi_trips",
        )

        target_table.create_table_if_not_exists(df.schema)
        target_table.write(df, mode="overwrite")
