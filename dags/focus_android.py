from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from airflow.operators.bash import BashOperator

docs = """
Airflow DAG example using dbt core
"""


default_args = {
    "owner": "example@mozilla.com",
    "start_date": datetime.datetime(2018, 11, 27, 0, 0),
    "end_date": None,
    "email": ["telemetry-alerts@mozilla.com", "example@mozilla.com"],
    "depends_on_past": False,
    "retry_delay": datetime.timedelta(seconds=1800),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
}

tags = ["impact/tier_3", "repo/bigquery-etl"]

with DAG(
    "focus_android",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    doc_md=docs,
    tags=tags,
) as dag:

    params = '{"submission_date": "{{ ds }}"}'

    focus_android_derived__metrics_clients_daily_v1 = BashOperator(
        task_id='focus_android_derived__metrics_clients_daily_v1',
        bash_command=f'dbt run --select focus_android_derived__metrics_clients_daily_v1 --vars {params}',
        depends_on_past=True,
    )

    focus_android_derived__metrics_clients_daily_v1_test = BashOperator(
        task_id='focus_android_derived__metrics_clients_daily_v1_test',
        bash_command='dbt test --select focus_android_derived__metrics_clients_daily_v1'
    )

    focus_android_derived__metrics_clients_daily_v1 >> focus_android_derived__metrics_clients_daily_v1_test

    focus_android_derived__metrics_clients_last_seen_v1 = BashOperator(
        task_id='focus_android_derived__metrics_clients_last_seen_v1',
        bash_command=f'dbt run --select focus_android_derived__metrics_clients_last_seen_v1 --vars {params}', 
        depends_on_past=True,
    )

    focus_android_derived__metrics_clients_last_seen_v1_test = BashOperator(
        task_id='focus_android_derived__metrics_clients_last_seen_v1_test',
        bash_command='dbt test --select focus_android_derived__metrics_clients_last_seen_v1'
    )

    focus_android_derived__metrics_clients_last_seen_v1 >> focus_android_derived__metrics_clients_last_seen_v1_test

    focus_android_derived__metrics_clients_daily_v1 >> focus_android_derived__metrics_clients_last_seen_v1
