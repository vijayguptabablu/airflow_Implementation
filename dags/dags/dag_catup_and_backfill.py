from airflow.models.dag import DAG, DagRun
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

args = {
    "owner": "BD3",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def task1():
    print("Task1")
    
def task2():
    print("Task2")
    
def task3():
    print("Task3")

with DAG(
    dag_id="dag_catup_backfill",
    description="Task with Python Operator",
    default_args=args,
    start_date=datetime(2024, 11, 1),
    catchup=False
) as dag:
    t1 = PythonOperator(
    task_id="task1", python_callable=task1)
    
    t2 = PythonOperator(
    task_id="task2", python_callable=task2)
    
    t3 = PythonOperator(
    task_id="task3", python_callable=task3)
    
    t1 >> t2 >> t3