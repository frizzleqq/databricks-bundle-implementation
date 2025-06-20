import argparse
import sys

from dbx_example.etl_task import Task


def main():
    parser = argparse.ArgumentParser(description="Execute ETL tasks.")
    parser.add_argument(
        "components", nargs="+", type=str, help="The name(s) of the ETL task(s) to execute"
    )
    parser.add_argument(
        "--catalog", type=str, default="lake_dev", help="The catalog to use (default: lake_dev)"
    )

    args = parser.parse_args()

    for component in args.components:
        try:
            task = Task.create_etl_task(component)
            task.run(catalog_name=args.catalog)
        except ValueError as e:
            print(f"Error with component '{component}': {e}")
            sys.exit(1)
        except Exception as e:
            print(f"An unexpected error occurred with component '{component}': {e}")
            sys.exit(1)
