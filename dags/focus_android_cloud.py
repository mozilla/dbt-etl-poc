from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
import datetime
from utils.constants import ALLOWED_STATES, FAILED_STATES
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook, DbtCloudJobRunStatus
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.utils.edgemodifier import Label

docs = """
Airflow DAG example using dbt cloud
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

DBT_CLOUD_CONN_ID = "dbt"

def _check_job_not_running(job_id):
    """
    Retrieves the last run for a given dbt Cloud job and checks to see if the job is not currently running.
    """
    hook = DbtCloudHook(DBT_CLOUD_CONN_ID)
    runs = hook.list_job_runs(job_definition_id=job_id, order_by="-id")
    latest_run = runs[0].json()["data"][0]

    return DbtCloudJobRunStatus.is_terminal(latest_run["status"])


with DAG(
    "focus_android_cloud",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    doc_md=docs,
    tags=tags,
) as dag:

    params = '{"submission_date": "{{ ds }}"}'

    # jobs need to be configured in dbt cloud

    check_focus_android_derived__metrics_clients_daily_v1 = ShortCircuitOperator(
        task_id="check_focus_android_derived__metrics_clients_daily_v1_not_running",
        python_callable=_check_job_not_running,
        op_kwargs={"job_id": "focus_android_derived__metrics_clients_daily_v1"},
    )

    # no option to pass parameters!!!!!

    focus_android_derived__metrics_clients_daily_v1 = DbtCloudRunJobOperator(
        task_id="focus_android_derived__metrics_clients_daily_v1",
        dbt_cloud_conn_id=DBT_CLOUD_CONN_ID,
        job_id="focus_android_derived__metrics_clients_daily_v1",
        check_interval=600,
        timeout=3600,
    )

    check_focus_android_derived__metrics_clients_daily_v1 >> focus_android_derived__metrics_clients_daily_v1

    check_focus_android_derived__metrics_clients_daily_v1 = ShortCircuitOperator(
        task_id="check_focus_android_derived__metrics_clients_daily_v1_not_running",
        python_callable=_check_job_not_running,
        op_kwargs={"job_id": "focus_android_derived__metrics_clients_daily_v1"},
    )

    focus_android_derived__metrics_clients_last_seen_v1 = DbtCloudRunJobOperator(
        task_id="focus_android_derived__metrics_clients_last_seen_v1",
        dbt_cloud_conn_id=DBT_CLOUD_CONN_ID,
        job_id="focus_android_derived__metrics_clients_last_seen_v1",
        check_interval=600,
        timeout=3600,
    )

    check_focus_android_derived__metrics_clients_daily_v1 >> focus_android_derived__metrics_clients_last_seen_v1

    check_focus_android_derived__metrics_clients_daily_v1 >> check_focus_android_derived__metrics_clients_daily_v1
