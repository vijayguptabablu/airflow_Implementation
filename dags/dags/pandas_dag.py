from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
import pandas_script as ps

default_args = {
    'owner': 'dinesh',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='pandas_script',
    default_args=default_args,
    description='This is Pandas dag',
    start_date=datetime(2024, 11, 1),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='panda_script',
        python_callable=ps.get_name
    )

    task1