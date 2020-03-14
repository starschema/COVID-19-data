from datetime import timedelta
import papermill as pm

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators import PythonOperator



default_args = {
	'owner': 'admin',
	'start_date': days_ago(2)
	}

def execute_notebook(notebook, **kwargs):
    pm.execute_notebook(
	    input_path=notebook,
	    output_path='/home/ec2-user/COVID-19-data/output.ipynb',
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

run_jhu_notebook = PythonOperator(
    task_id='execute_jupter_nb',
    provide_context=True,
    python_callable=execute_notebook,
    op_args = ['/home/ec2-user/COVID-19-data/JH_COVID-19.ipynb'],
    dag=dag)

