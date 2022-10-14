import os

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.hooks.S3_hook import S3Hook

import pendulum


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

data_load = DAG(
    "data_load_dag",
    schedule= None,
    start_date= pendulum.datetime(2022, 1, 1)
)

  
URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet"
OUTPUT =  AIRFLOW_HOME + '/trip_2022-01.parquet'


with data_load:
    extract_data = BashOperator(
        task_id='extract',
        bash_command=f'curl -o {OUTPUT} {URL}'
    ) 

    @task()
    def load_data (**kwargs):
        S3_VAR = Variable.get("s3_var",deserialize_json=True)
        s3_hook = S3Hook("s3_upload")
        s3_hook.get_conn()
        s3_hook.load_file(filename=OUTPUT, key=S3_VAR["s3_key"], bucket_name=S3_VAR["s3_bucket"])

    extract_data >> load_data()