import os

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.hooks.S3_hook import S3Hook

import pendulum


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

s3_to_local = DAG(
    "download-from-S3",
    schedule= None,
    start_date= pendulum.datetime(2022, 1, 1)
)


with s3_to_local:

    @task()
    def download_data (**kwargs):
        S3_VAR = Variable.get("s3_var",deserialize_json=True)
        s3_hook = S3Hook("s3_upload")
        s3_hook.get_conn()
        output_file = s3_hook.download_file(key=S3_VAR["s3_key"], bucket_name=S3_VAR["s3_bucket"], local_path=AIRFLOW_HOME)
        return output_file
     
     # Rename the file 
    rename_file = BashOperator(
        task_id='rename',
        
        bash_command='mv -f {{ ti.xcom_pull("download_data") }} /opt/airflow/trip_2022-01.parquet'
    ) 


    
    download_data () >> rename_file




