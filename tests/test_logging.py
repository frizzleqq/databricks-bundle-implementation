"""Tests for logging configuration."""

import logging
import re

import pytest

from dab_project.logging_config import setup_logging


@pytest.fixture(autouse=True)
def reset_logging():
    """Reset logging configuration after each test."""
    yield
    # Clear all handlers after each test
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.WARNING)


def test_setup_logging_default_level(capsys):
    """Test that default logging level is INFO."""
    setup_logging(verbose=False)

    logger = logging.getLogger("test_module")
    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")

    captured = capsys.readouterr()
    # DEBUG should not be logged
    assert "This is a debug message" not in captured.out
    # INFO and higher should be logged
    assert "This is an info message" in captured.out
    assert "This is a warning message" in captured.out


def test_setup_logging_verbose_level(capsys):
    """Test that verbose flag enables DEBUG logging."""
    setup_logging(verbose=True)

    logger = logging.getLogger("test_module")
    logger.debug("This is a debug message")
    logger.info("This is an info message")

    captured = capsys.readouterr()
    # Both DEBUG and INFO should be logged
    assert "This is a debug message" in captured.out
    assert "This is an info message" in captured.out


def test_logging_format(capsys):
    """Test that log format matches expected pattern."""
    setup_logging(verbose=False)

    logger = logging.getLogger("test_module")
    logger.info("Test message")

    captured = capsys.readouterr()
    # Expected format: YYYY-MM-DD HH:MM:SS,mmm - LEVEL - module - message
    # Example: 2025-04-23 10:35:42,789 - INFO - test_module - Test message
    log_pattern = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} - INFO - test_module - Test message"

    assert re.search(log_pattern, captured.out) is not None


def test_logging_different_levels(capsys):
    """Test logging at different levels."""
    setup_logging(verbose=False)

    logger = logging.getLogger("test_module")
    logger.info("Info message")
    logger.warning("Warning message")
    logger.error("Error message")
    logger.critical("Critical message")

    captured = capsys.readouterr()
    assert "Info message" in captured.out
    assert "Warning message" in captured.out
    assert "Error message" in captured.out
    assert "Critical message" in captured.out
    assert "- INFO -" in captured.out
    assert "- WARNING -" in captured.out
    assert "- ERROR -" in captured.out
    assert "- CRITICAL -" in captured.out


def test_logging_no_duplicate_handlers(capsys):
    """Test that calling setup_logging multiple times doesn't create duplicate handlers."""
    # Call setup_logging twice
    setup_logging(verbose=False)
    setup_logging(verbose=False)

    logger = logging.getLogger("test_module")
    logger.info("Test message")

    captured = capsys.readouterr()
    # Count occurrences - should appear only once
    assert captured.out.count("Test message") == 1


def test_setup_logging_root_logger_level():
    """Test that root logger level is set correctly."""
    setup_logging(verbose=False)
    root_logger = logging.getLogger()
    assert root_logger.level == logging.INFO

    setup_logging(verbose=True)
    root_logger = logging.getLogger()
    assert root_logger.level == logging.DEBUG


def test_setup_logging_handler_count():
    """Test that setup_logging creates exactly one handler."""
    setup_logging(verbose=False)
    root_logger = logging.getLogger()
    assert len(root_logger.handlers) == 1

    # Calling again should still result in only one handler
    setup_logging(verbose=False)
    assert len(root_logger.handlers) == 1
