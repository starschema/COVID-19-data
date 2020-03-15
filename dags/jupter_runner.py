from datetime import timedelta
import papermill as pm

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.configuration import conf



default_args = {
	'owner': 'admin',
	'start_date': days_ago(2)
	}

def execute_notebook(notebook, **kwargs):
    pm.execute_notebook(
	    input_path=notebook,
	    output_path='/tmp/output.ipynb',
	    parameters=dict(),
	    log_output=True,
	    report_mode=True
	    )
    return 

dag = DAG(
	    dag_id='jupyter_runner',
	    default_args=default_args,
	    schedule_interval=None,
	    dagrun_timeout=timedelta(minutes=60)
	    ) 
with dag:
    notebook_path = conf.get('core','dags_folder') + '/../notebooks/JHU_COVID-19.ipynb'
    run_jhu_notebook = PythonOperator(
        task_id='execute_jupter_nb',
        provide_context=True,
        python_callable=execute_notebook,
        op_args = [notebook_path],
        dag=dag)

