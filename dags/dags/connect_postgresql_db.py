import json
import pandas as pd
from airflow.models.dag import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

args = {
    "owner": "BD4",
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

query = "SELECT * FROM sample_data;"

def get_data():
    hook = PostgresHook(postgres_conn_id='postgresql_conn')
    conn = hook.get_conn()
    df = pd.read_sql(query, conn)
    df_dict = df.to_dict(orient='records')
    return df_dict

def filter_data(ti):
    df_dict = ti.xcom_pull(task_ids='process_data')
    df = pd.DataFrame(df_dict)
    df = df[df['name'].isin(["Dinesh", "Shruthy"])]
    print(df)

with DAG(
    dag_id="postgre_db_connect",
    description="PostgreSQL DB Connection",
    default_args=args,
    start_date=datetime(2024, 11, 1),
    catchup=False
) as dag:

    get_results = PythonOperator(
        task_id="process_data",
        python_callable=get_data,
    )

    d_s = PythonOperator(
        task_id="filter_data",
        python_callable=filter_data,
    )

    get_results >> d_s
