from airflow.models.dag import DAG, DagRun
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
from airflow.models import Variable

args = {
    "owner": "dinesh",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "email": "rdinesh808@gmail.com",
    "email_on_failure": True,
    "email_on_retry": True
}

def notify_email(context):
    subject = f"Airflow Alert: Task Failed - {context['task_instance'].task_id}"

    body = f"""
    <p>Task {context['task_instance'].task_id} in DAG {context['dag'].dag_id} has failed.</p>
    <p>Task Instance Details:</p>
    <ul>
        <li>Execution Time: {context['execution_date']}</li>
        <li>Log URL: <a href="{context['task_instance'].log_url}">{context['task_instance'].log_url}</a></li>
    </ul>
    """

    # Fetch email addresses from Airflow Variables
    # Here "alert_email_recipients" is the variable name you should use
    recipient_list = Variable.get("alert_email_recipients", default_var="rdinesh808@gmail.com,awsdinesh8@gmail.com").split(',')

    # Send the email
    send_email(to=recipient_list, subject=subject, html_content=body)


def task1():
    a = 10
    b = 0
    c = a // b
    print(f"Multiplication is : {c}")
    

with DAG(
    dag_id="dag_with_email_notification",
    description="Task with Python Operator",
    default_args=args,
    start_date=datetime(2024, 11, 1),
    catchup=False,
    schedule_interval="0,30 * * * 1-5",
) as dag:
    t1 = PythonOperator(
    task_id="task1", python_callable=task1, on_failure_callback=notify_email)
    
    t1