import logging
from abc import ABC, abstractmethod
from typing import Optional


class Task(ABC):
    """
    Abstract base class for ETL tasks.

    Provides a standard execution flow with logging and requires
    child classes to implement the data processing logic.
    """

    @classmethod
    def create_etl_task(cls, task_name: str, name: Optional[str] = None) -> "Task":
        """
        Factory method to create a task instance by type.

        Parameters
        ----------
        task_name : str
            Identifier for the task class name to create
        name : str, optional
            Name for the task instance (defaults to task_type if not provided)

        Returns
        -------
        ETLTask
            An instance of the requested task type
        """
        # Find the requested task class by name
        for subclass in cls.__subclasses__():
            if subclass.__name__ == task_name:
                return subclass(name=name)

        raise ValueError(f"Unknown EtlTask: '{task_name}'. Available types: {cls.__subclasses__()}")

    def __init__(self, name: str):
        """
        Initialize an ETL task.

        Parameters
        ----------
        name : str
            A descriptive name for the task
        """
        self.name = name
        self.logger = logging.getLogger(__name__)

    def run(self):
        """
        Execute the ETL task with proper logging.
        """
        self._log_entry()
        try:
            self._write_data()
            self._log_exit(success=True)
        except Exception as e:
            self._log_exit(success=False, error=e)
            raise

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


# Example implementation - no registration needed
class SimpleETLTask(Task):
    """Example implementation of an ETL task."""

    def _write_data(self):
        """
        Simple implementation that processes the provided data.

        Returns
        -------
        dict
            Dictionary with processing results
        """
        self.logger.info(f"Processing data in {self.name}")
        # Implement actual data processing logic here
