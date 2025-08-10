# Databricks notebook source
# MAGIC %pip install pytest

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import sys
import pytest

# Prevent writing .pyc files
sys.dont_write_bytecode = True
# Add src to python path
sys.path.append("../src")

# COMMAND ----------

pytest_result = pytest.main(
    [
        ".",
        "-v",
    ]
)

assert pytest_result == 0, f"Tests failed with code {pytest_result}"
