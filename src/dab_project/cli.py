import argparse
import logging
import sys

from dab_project.logging_config import setup_logging
from dab_project.tasks.base_task import Task


def main():
    parser = argparse.ArgumentParser(description="Execute ETL tasks.")
    parser.add_argument(
        "components", nargs="+", type=str, help="The name(s) of the ETL task(s) to execute"
    )
    parser.add_argument(
        "--catalog", type=str, default="lake_dev", help="The catalog to use (default: lake_dev)"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose (DEBUG) logging"
    )

    args = parser.parse_args()

    # Setup logging based on verbose flag
    setup_logging(verbose=args.verbose)
    logger = logging.getLogger(__name__)

    for component in args.components:
        try:
            task = Task.create_task_factory(component)
            task.run(catalog_name=args.catalog)
        except ValueError as e:
            logger.error(f"Error with component '{component}': {e}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"An unexpected error occurred with component '{component}': {e}")
            sys.exit(1)
