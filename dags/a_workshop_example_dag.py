from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

dag = DAG(
    dag_id='a_workshop_example_dag',
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    max_active_runs=1
)

task_1 = BashOperator(
    task_id='task_1',
    bash_command='echo "ini output task 1"',
    dag=dag
)

task_2 = BashOperator(
    task_id='task_2',
    bash_command='echo "ini output task 2"',
    dag=dag
)

task_1 >> task_2


if __name__ == "__main__":
    dag.cli()
