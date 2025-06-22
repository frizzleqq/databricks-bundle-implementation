"""Simple ETL Task class to showcase Python Wheel Task (CLI entry-point)"""

import logging
from abc import ABC, abstractmethod
from typing import Optional

from pyspark.sql import SparkSession

from dbx_example.spark import get_spark


class Task(ABC):
    """
    Abstract base class for ETL tasks.

    Provides a standard execution way for child classes to implement
    actual data processing.
    """

    @classmethod
    def get_class_name(cls) -> str:
        return cls.__name__

    @classmethod
    def create_task_factory(cls, task_name: str) -> "Task":
        """
        Factory method to create a task instance by type.

        Parameters
        ----------
        task_name : str
            Identifier for the task class name to create

        Returns
        -------
        Task
            An instance of the requested task type
        """
        # Find the requested task class by name
        for subclass in cls.__subclasses__():
            if subclass.get_class_name() == task_name:
                return subclass()

        raise ValueError(f"Unknown Task: '{task_name}'. Available types: {cls.__subclasses__()}")

    def __init__(self):
        """
        Initialize an ETL task.
        """
        self.logger = logging.getLogger(__name__)
        self.spark: SparkSession

    @property
    def name(self) -> str:
        """
        Get the name of the task.
        """
        return self.get_class_name()

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
