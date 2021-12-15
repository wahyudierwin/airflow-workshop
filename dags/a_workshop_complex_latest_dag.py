from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

from file_sensor import FileSensor

dag = DAG(
    dag_id='a_workshop_complex_latest_dag',
    schedule_interval='0 0 * * *',
    start_date=days_ago(3),
    dagrun_timeout=timedelta(minutes=60),
    max_active_runs=1
)

project_dir = '{{var.json.workshop.project_dir}}'
data_dir = '{{var.json.workshop.data_dir}}'

wait_users_data = FileSensor(
    task_id='wait_users_data',
    path=f'{data_dir}/{{{{ds}}}}/users.dat',
    execution_timeout=timedelta(minutes=5),
    dag=dag
)

wait_movies_data = FileSensor(
    task_id='wait_movies_data',
    path=f'{data_dir}/{{{{ds}}}}/movies.dat',
    execution_timeout=timedelta(minutes=5),
    dag=dag
)

wait_ratings_data = FileSensor(
    task_id='wait_ratings_data',
    path=f'{data_dir}/{{{{ds}}}}/ratings.dat',
    execution_timeout=timedelta(minutes=5),
    dag=dag
)

analyze = BashOperator(
    task_id='analyze',
    bash_command=f'python' \
        f' {project_dir}/codes/analyze_latest.py'
        f' --users-latest-path {data_dir}/latest/users.json'
        f' --movies-latest-path {data_dir}/latest/movies.json'
        f' --ratings-latest-path {data_dir}/latest/ratings.json'
        f' --output-path {data_dir}/grouped.csv',
    trigger_rule='all_done',
    dag=dag
)

[wait_users_data, wait_movies_data, wait_ratings_data] >> analyze


if __name__ == "__main__":
    dag.cli()
