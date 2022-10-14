import os

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.hooks.S3_hook import S3Hook

import pendulum


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

data_transform = DAG(
    "data_transform_dag",
    schedule= None,
    start_date= pendulum.datetime(2022, 1, 1)
)

  
URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet"
OUTPUT =  AIRFLOW_HOME + '/trip_2022-01.parquet'


with data_transform:

    @task()
    def to_postgres ():
       
        import pandas as pd
        import pyarrow.parquet as pq
        from sqlalchemy import create_engine
        import io


        trip_db_user = os.getenv('TRIP_DB_USER')
        trip_db_password = os.getenv('TRIP_DB_PASSWORD')
        trip_db_host = os.getenv('TRIP_DB_HOST')
        trip_db_database = os.getenv('TRIP_DB_DATABASE')
        
        
        S3_VAR = Variable.get("s3_var",deserialize_json=True)
        s3_hook = S3Hook("s3_upload")
        object = s3_hook.get_key(key=S3_VAR["s3_key"], bucket_name=S3_VAR["s3_bucket"])
        io_byte= io.BytesIO()
        object.download_fileobj(io_byte)




        engine = create_engine(f'postgresql://{trip_db_user}:{trip_db_password}@{trip_db_host}:5432/{trip_db_database}')
        print(engine.connect())

        trip = pq.read_table(io_byte)

        trip = trip.to_pandas()

        trip.to_sql(name='trip_2022_01', con=engine, if_exists='replace', index=False)

        print (f'Congrats! Data loaded to the table.')
        
        
    
    to_postgres()



