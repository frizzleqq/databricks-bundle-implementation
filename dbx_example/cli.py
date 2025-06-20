import argparse
import sys

from dbx_example.etl_task import Task


def main():
    parser = argparse.ArgumentParser(description="Execute an ETL task.")
    parser.add_argument("component", type=str, help="The name of the ETL task to execute")

    args = parser.parse_args()

    try:
        task = Task.create_etl_task(args.component)
        task.run()
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)
