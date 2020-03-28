import airflow
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.configuration import conf
import json
import requests
from datetime import datetime, timedelta

DAGS_FOLDER = conf.get('core', 'dags_folder')

with open( DAGS_FOLDER + "/../refresh_schedules.json", 'r') as f:
    schedules = json.load(f)


args = {
    'owner': 'admin',
    'start_date': '2020-03-18T12:00:00Z',
    'catchup_by_default': False
}

def get_last_commit(ds, **kwargs):
    since = kwargs.get('execution_date', None).strftime('%Y-%m-%dT%H:%M:%SZ')
    name =kwargs.get('name')
    url_template =kwargs.get('url')

    url = url_template.format(since)

    print("Loading data from " + url)

    response = requests.get(url)
    commits = response.json()

    if len(commits) > 0:
        print("We should run now.")
        return f"trigger_{name.lower()}"
    else:
        return "stop"

dag = DAG(
    dag_id='github_poll_trigger',
    default_args=args,
    schedule_interval="@hourly"
)


with dag:
    
    stop_op = DummyOperator(task_id='stop', trigger_rule="all_done", dag=dag)
    
    start_op = DummyOperator(task_id='start', dag=dag)

    for name,url in schedules["github"].items():

        check_github_op = BranchPythonOperator (
            task_id=f'check_commits_{name.lower()}',
            python_callable=get_last_commit,
            provide_context=True,
            op_kwargs={"name": name, "url": url},
            trigger_rule="all_done",
            dag=dag,
        )

        trigger_etl_op = TriggerDagRunOperator(
            task_id=f"trigger_{name.lower()}",
            trigger_dag_id=f'etl_{name}',  
            dag=dag
        )

        check_github_op >> trigger_etl_op
        check_github_op >> stop_op
        trigger_etl_op >> stop_op
