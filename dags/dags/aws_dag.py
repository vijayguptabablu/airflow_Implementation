import boto3
from airflow.models.dag import DAG, DagRun
from datetime import datetime
from airflow.operators.python import PythonOperator
import airflow.providers as p

s3 = boto3.client('s3')

args = {
    "owner": "dinesh",
    "start_date": datetime(2024, 11, 1)
}

dag = DAG(
    dag_id="aws_test_dag",
    default_args=args,
    schedule_interval="45 18 * * *",  # Cron expression for daily at 18:45
)

def get_list_of_s3_buckets():
    res = s3.list_buckets()
    for bucket in res['Buckets']:
        print(bucket['Name'])

def test2():
    print(dir(p))

s3_bucket = PythonOperator(
    task_id="test1",
    python_callable=get_list_of_s3_buckets,
    dag=dag
)

t2 = PythonOperator(
    task_id="test2",
    python_callable=test2,
    dag=dag
)

s3_bucket >> t2
