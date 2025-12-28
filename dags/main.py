from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from api.videos_data import (
    get_playlist_id,
    get_videos_id,
    extract_video_details,
    save_to_json
)
from datawarehouse.dwh_init import populate_staging_table, populate_core_table

from dataquality.soda import elt_data_quality_check


# Define the local timezone
local_tz = pendulum.timezone("Africa/Cairo")

# Default Args (task-level defaults)
default_args = {
    "owner": "bisho",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "data@engineers.com",
}

with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="DAG to produce JSON file with raw data",
    schedule_interval="0 14 * * *",  # cron at 14:00 daily
    catchup=False,
    max_active_runs=1,              # DAG-level setting
    dagrun_timeout=timedelta(hours=1),
    start_date=datetime(2025, 12, 24, tzinfo=local_tz),  # <- in the past relative to 2025-12-25
) as dag_produce:

    # TaskFlow tasks (imported from api.videos_data)
    playlist_id = get_playlist_id()
    video_ids = get_videos_id(playlist_id)
    extract_data = extract_video_details(video_ids)
    save_to_json_task = save_to_json(extract_data)


    trigger_update_datawarehouse = TriggerDagRunOperator(
        task_id="trigger_update_datawarehouse_dag",
        trigger_dag_id="update_datawarehouse"
    )
    # Explicit dependency graph)
    playlist_id >> video_ids >> extract_data >> save_to_json_task >> trigger_update_datawarehouse


with DAG(
    dag_id="update_datawarehouse",
    default_args=default_args,
    description="DAG to process json file and insert data into both staging and core schemas",
    schedule_interval=None,  # cron at 14:00 daily
    catchup=False,
    max_active_runs=1,              # DAG-level setting
    dagrun_timeout=timedelta(hours=1),
    start_date=datetime(2025, 12, 25, tzinfo=local_tz),  # <- in the past relative to 2025-12-25
) as dag_update:

    update_staging = populate_staging_table()
    update_core = populate_core_table()

    trigger_data_quality_checks = TriggerDagRunOperator(
        task_id="trigger_data_quality_checks_dag",
        trigger_dag_id="data_quality_checks"
    )

    update_staging >> update_core >> trigger_data_quality_checks


with DAG(
    dag_id="data_quality_checks",
    default_args=default_args,
    description="DAG to perform data quality checks using Soda postgres",
    catchup=False,
    schedule=None,
) as dag_quality:
    
    staging_quality_check = elt_data_quality_check("staging")
    core_quality_check = elt_data_quality_check("core")

    staging_quality_check >> core_quality_check