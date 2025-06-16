import sys

import pytest

sys.dont_write_bytecode = True  # Prevent writing .pyc files

pytest_result = pytest.main(
    [
        ".",
        "-v",
        "-x",
    ]
)

assert pytest_result == 0, f"Tests failed with code {pytest_result}"
