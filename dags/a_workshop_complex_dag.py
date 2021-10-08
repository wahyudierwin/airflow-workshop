from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

from file_sensor import FileSensor

dag = DAG(
    dag_id='a_workshop_complex_dag',
    schedule_interval='0 0 * * *',
    start_date=days_ago(3),
    dagrun_timeout=timedelta(minutes=60),
    max_active_runs=1
)

project_dir = '{{var.json.workshop.project_dir}}'
data_dir = '{{var.json.workshop.data_dir}}/{{ds}}'

wait_users_data = FileSensor(
    task_id='wait_users_data',
    path=f'{data_dir}/users.dat',
    dag=dag
)

wait_movies_data = FileSensor(
    task_id='wait_movies_data',
    path=f'{data_dir}/movies.dat',
    dag=dag
)

wait_ratings_data = FileSensor(
    task_id='wait_ratings_data',
    path=f'{data_dir}/ratings.dat',
    dag=dag
)

analyze = BashOperator(
    task_id='analyze',
    bash_command=f'python' \
        f' {project_dir}/codes/analyze.py'
        f' --users-path {data_dir}/users.dat'
        f' --movies-path {data_dir}/movies.dat'
        f' --ratings-path {data_dir}/ratings.dat'
        f' --output-path {data_dir}/grouped.csv',
    dag=dag
)

[wait_users_data, wait_movies_data, wait_ratings_data] >> analyze


if __name__ == "__main__":
    dag.cli()
