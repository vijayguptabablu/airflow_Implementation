from airflow.models.dag import DAG, DagRun
from datetime import datetime
from airflow.operators.python import PythonOperator
import pylogicalprogram.logical as lp
from pylogicalprogram.patterns.Alphabetspatterns import print_name

args = {
    "owner": "dinesh",
    "start_date": datetime(2024, 11, 3)
}

dag = DAG(
    dag_id="my-test-dag",
    default_args=args,
    schedule_interval="45 18 * * *",  # Cron expression for daily at 18:45
)

def test1(**kwargs):
    print("Keys:", list(kwargs.keys()))
    print("Values:", list(kwargs.values()))
    print("OWNER is : ", kwargs.get('owner'))
    print("Test 1")
    dag_runs = DagRun.find(dag_id="my-test-dag")
    print(dag_runs)

def test2():
    print("Test 2")
    name = print_name("dinesh")
    print(f"Name is : \n{name}")
    print(dir(lp))

t1 = PythonOperator(task_id="task1", python_callable=test1, dag=dag)
t2 = PythonOperator(task_id="task2", python_callable=test2, dag=dag)

t1 >> t2
