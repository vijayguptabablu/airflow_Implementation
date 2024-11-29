from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.subdag import SubDagOperator

# Define the subDAG function
def create_subdag(parent_dag_name, child_dag_name, args):
    with DAG(
        dag_id=f"{parent_dag_name}.{child_dag_name}",
        default_args=args,
        schedule_interval="@daily",
    ) as subdag:
        start_task = DummyOperator(task_id="start_subdag_task")
        subdag_task_1 = DummyOperator(task_id="subdag_task_1")
        subdag_task_2 = DummyOperator(task_id="subdag_task_2")
        end_task = DummyOperator(task_id="end_subdag_task")

        # Setting dependencies within the subDAG
        start_task >> [subdag_task_1, subdag_task_2] >> end_task

    return subdag

# Define default arguments
default_args = {
    'start_date': datetime(2024, 11, 1),
}

# Create the main parent DAG
with DAG(
    "parent_dag_with_subdag",
    default_args=default_args,
    schedule_interval="@daily",
) as dag:

    # Define tasks in the parent DAG
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    # Create a SubDagOperator for the subDAG
    subdag = SubDagOperator(
        task_id="child_subdag",
        subdag=create_subdag("parent_dag_with_subdag", "child_subdag", default_args),
    )

    # Set up dependencies in the parent DAG
    start >> subdag >> end
