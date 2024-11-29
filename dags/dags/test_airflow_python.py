from airflow.models.dag import DAG, DagRun
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

args = {
    "owner": "dinesh",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def name(ti):
    ti.xcom_push(key="name", value="Dinesh")
    ti.xcom_push(key="location", value="Tiruchirappalli")

def age(ti):
    ti.xcom_push(key="age", value=30)

def greet(ti):
    name = ti.xcom_pull(task_ids="name", key="name")
    age = ti.xcom_pull(task_ids="age", key="age")
    location = ti.xcom_pull(task_ids="name", key="location")
    print(f"My name is {name}!, and I am {age} years old, I am from {location}.")

with DAG(
    dag_id="af_dag_python",
    description="Task with Python Operator",
    default_args=args,
    start_date=datetime(2024, 11, 3)
) as dag:
    t1 = PythonOperator(
    task_id="name", python_callable=name)
    
    t2 = PythonOperator(
    task_id="age", python_callable=age)
    
    t3 = PythonOperator(
    task_id="greet", python_callable=greet)
    
    [t1 >> t2] >> t3