import logging
from airflow.operators.bash import BashOperator

logger = logging.getLogger(__name__)

SODA_PATH = "/opt/airflow/include/soda"
DATASOURCE = "pg_datasource"


def elt_data_quality_check(schema):
    """
    Perform data quality checks using Soda SQL for the specified schema.

    Args:
        schema (str): The database schema to check.
    """
    try:
        task = BashOperator(
            task_id=f"soda_test_{schema}",
            bash_command= f"soda scan -d {DATASOURCE} -c {SODA_PATH}/configuration.yml -v SCHEMA={schema} {SODA_PATH}/checks.yml"
        )
        return task
    except Exception as e:
        logger.error(f"Failed to run data quality check task for schema {schema}")
        raise e