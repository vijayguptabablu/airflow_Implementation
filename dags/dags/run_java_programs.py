from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.github.operators.github import GithubOperator
from airflow.utils.dates import days_ago
import os

def checkout_code_from_github():
    repo_url = "https://github.com/rdinesh808/pylogicalprograms.git"
    clone_dir = "/home/ubuntu/java_airflow"
    os.system(f"git clone {repo_url} {clone_dir}")

with DAG(
    dag_id="execute_java_program",
    default_args={"owner": "dinesh", "retries": 1},
    description="GitHub Checkout Example",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
) as dag:

    clone_repo = GithubOperator(
        task_id="clone_repo_task",
        github_method="clone",  # Method for cloning a repo
        #repo="https://github.com/rdinesh808/pylogicalprograms.git",  # Replace with your GitHub username and repository
        github_conn_id="github_default",  # Connection ID for GitHub
        #branch="main",  # Optional: Specify a branch (default is the main branch)
        #destination="/home/ubuntu/java_airflow",  # Optional: Specify the directory to clone the repo into
    )

    # Alternatively, use a Python function to checkout code
    checkout_code = PythonOperator(
        task_id="checkout_code",
        python_callable=checkout_code_from_github,
    )

    # Set task dependencies
    clone_repo
