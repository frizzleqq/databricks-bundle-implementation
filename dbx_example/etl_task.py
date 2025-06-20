import logging
from abc import ABC, abstractmethod
from typing import Optional

from pyspark.sql import SparkSession

from dbx_example.spark import get_spark


class Task(ABC):
    """
    Abstract base class for ETL tasks.

    Provides a standard execution flow with logging and requires
    child classes to implement the data processing logic.
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

        Parameters
        ----------
        name : str
            A descriptive name for the task
        """
        self.logger = logging.getLogger(__name__)
        self.spark: SparkSession

    def run(self):
        """
        Execute the ETL task with proper logging.
        """
        self._log_entry()
        self.spark = get_spark()
        try:
            self._write_data()
            self._log_exit(success=True)
        except Exception as e:
            self._log_exit(success=False, error=e)
            raise

    @property
    def name(self) -> str:
        """
        Get the name of the task.

        Returns
        -------
        str
            The name of the task class
        """
        return self.__class__.__name__

    def _log_entry(self) -> None:
        """Log the start of task execution."""
        self.logger.info(f"Starting ETL task: {self.name}")

    def _log_exit(self, success: bool, error: Optional[Exception] = None) -> None:
        """
        Log the completion of task execution.

        Parameters
        ----------
        success : bool
            Whether the task completed successfully
        error : Exception, optional
            Exception that occurred, if any
        """
        if success:
            self.logger.info(f"Completed ETL task: {self.name}")
        else:
            self.logger.error(f"Failed ETL task: {self.name} - {str(error)}")

    @abstractmethod
    def _write_data(self):
        """
        Process and write data.

        This method must be implemented by child classes.

        Returns
        -------
        dict
            Dictionary containing the results of the operation
        """
        pass
