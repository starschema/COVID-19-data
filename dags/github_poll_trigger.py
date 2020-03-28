import airflow
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator

import requests
from datetime import datetime, timedelta

args = {
    'owner': 'admin',
    'start_date': '2020-03-18T12:00:00Z',
    'catchup_by_default': False
}

dag = DAG(
    dag_id='github_poller_jhu',
    default_args=args,
    schedule_interval="@hourly"
)

# [START howto_operator_python]


def get_last_commit(ds, **kwargs):
    since = kwargs.get('execution_date', None).strftime('%Y-%m-%dT%H:%M:%SZ')

    url = 'https://api.github.com/repos/CSSEGISandData/COVID-19/commits?since={}&path=csse_covid_19_data/csse_covid_19_time_series'.format(
        since)

    print("Loading data from " + url)

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
    trigger_rule="all_done",
    dag=dag,
)

trigger_etl_op = TriggerDagRunOperator(
    task_id="trigger_etl",
    trigger_dag_id="etl_JHU_COVID-19",  
    dag=dag
)

stop_op = DummyOperator(task_id='stop', trigger_rule="all_done", dag=dag)


check_github_op >> trigger_etl_op
check_github_op >> stop_op
trigger_etl_op >> stop_op
