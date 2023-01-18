from airflow.sensors.filesystem import FileSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG
from datetime import datetime

with DAG("etl_movement", schedule_interval= "* * * * 5", start_date=datetime(2022,10,1)) as dag:
    start_task = DummyOperator(task_id="dag_started")
    sensor_task = FileSensor(task_id="file_sensor_task",poke_interval=60,timeout=60,filepath= "/opt/airflow/raw_data",fs_conn_id="raw_data")
    arrived = DummyOperator(task_id="file_arrived")


    start_task >> sensor_task >> arrived