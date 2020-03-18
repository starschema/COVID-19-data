import airflow
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator

import requests
from datetime import datetime, timedelta

ts_format = "%Y-%m-%dT%H:%M:%SZ"

args = {
    'owner': 'admin',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='github_poller_jhu',
    default_args=args,
    schedule_interval="*/10 * * * *"
)

# [START howto_operator_python]


def get_last_commit(ds, **kwargs):
    since = kwargs.get('execution_date', None)
    url = 'https://api.github.com/repos/CSSEGISandData/COVID-19/commits?since={}&path=csse_covid_19_data/csse_covid_19_time_series'.format(
        since)

    response = requests.get(url)
    commits = response.json()

    if len(commits) > 0:
        print("We should run now.")
        return "trigger_etl"
    else:
        return "stop"


check_github_op = BranchPythonOperator (
    task_id='check_if_commit_happened',
    python_callable=get_last_commit,
    provide_context=True,
    dag=dag,
)

trigger_etl_op = TriggerDagRunOperator(
    task_id="trigger_etl",
    trigger_dag_id="etl_JHU_COVID-19",  
    dag=dag
)

stop_op = DummyOperator(task_id='stop', dag=dag)


check_github_op >> [trigger_etl_op, stop_op ]
trigger_etl_op.set_upstream(stop_op)