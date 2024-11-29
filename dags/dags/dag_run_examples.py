import boto3
from airflow.models.dag import DAG, DagRun
from datetime import datetime
from airflow.operators.python import PythonOperator

s3 = boto3.client('s3')

args = {
    "owner": "dinesh",
    "start_date": datetime(2024, 11, 1)
}

dag = DAG(
    dag_id="dag_run",
    default_args=args,
)

def dag_run_examp():
    dag_runs = DagRun.find(dag_id='dag_run')
    print(dag_runs)

def kwargs_example(**kwargs):
    a = kwargs['dag_run']
    for i in a.conf:
        print(i)

dagr = PythonOperator(
    task_id="dagrun",
    python_callable=dag_run_examp,
    dag=dag
)

kwargs = PythonOperator(
    task_id="kwargs",
    python_callable=kwargs_example,
    dag=dag
)

dagr >> kwargs
