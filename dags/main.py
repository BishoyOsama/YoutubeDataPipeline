from airflow import DAG
import pendulum
from datetime import datetime, timedelta

from api.videos_data import (
    get_playlist_id,
    get_videos_id,
    extract_video_details,
    save_to_json
)

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

    # Explicit dependency graph (optional; TaskFlow already orders them)
    playlist_id >> video_ids >> extract_data >> save_to_json_task