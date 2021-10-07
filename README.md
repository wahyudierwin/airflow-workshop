# airflow-workshop

## Initialize conda
```bash
conda create --name airflow python=3.8
conda activate airflow
```

## Installing airflow
```bash
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.1.4/constraints-3.8.txt"
pip install "apache-airflow==2.1.4" --constraint "${CONSTRAINT_URL}"
```

## Initialization to run airflow
```bash
export AIRFLOW_HOME = "~/airflow"

airflow db init

airflow users create \
    --username admin \
    --firstname Erwin \
    --lastname Wahyudi \
    --role Admin \
    --email erwin.eko.w@ugm.ac.id
```
Masukkan password sesuai keinginan.


## Run scheduler dan webserver
```
airflow scheduler
airflow webserver -p 8080
```
Default port adalah 8080.

Lalu, buka localhost:8080 di web browser.


## Data

https://files.grouplens.org/datasets/movielens/ml-1m.zip
