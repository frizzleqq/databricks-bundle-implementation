"""Logging configuration for dab_project."""

import logging
import sys


def setup_logging(verbose: bool = False) -> None:
    """
    Configure logging for the dab_project application.

    Parameters
    ----------
    verbose : bool, optional
        If True, set logging level to DEBUG. Otherwise, set to INFO (default: False)

    Notes
    -----
    Log format: YYYY-MM-DD HH:MM:SS,mmm - LEVEL - module - message
    Example: 2025-04-23 10:35:42,789 - INFO - auth - User login successful

    Currently logs to console only, but can be extended to log to files in the future.
    """
    log_level = logging.DEBUG if verbose else logging.INFO

    # Create formatter with the specified format
    # Note: %(asctime)s defaults to 'YYYY-MM-DD HH:MM:SS,mmm' format
    formatter = logging.Formatter(
        fmt="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Remove any existing handlers to avoid duplicate logs
    root_logger.handlers.clear()

    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)

    # Add handler to root logger
    root_logger.addHandler(console_handler)
