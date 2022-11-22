#!python3 

from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator 
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago 

default_args = {
	'owner': 'Admin',
}

with DAG(
    "bigdata_processing_test",
    start_date = days_ago(1),
    schedule_interval = None,  ## for checking, we set to none
    # schedule_interval = '*/10 * * * *', ## dag run every 10 mins,
	default_args = default_args
    ) as dag:

	# start operator
    start = DummyOperator(
        task_id = "start"
    )

    dump = BashOperator(
    	task_id = 'dump_data',
    	bash_command='python3 /home/agnes/airflow/dags/airflow_project/dump_data.py')


    with TaskGroup("data_transform") as data_transform:
        mrjob = BashOperator(
            task_id = 'mapreduce_etl',
            bash_command='python3 /home/agnes/airflow/dags/airflow_project/mapreduce_etl.py /home/agnes/airflow/dags/airflow_project/dataset/bigdata_transaction.csv'
        ),

        spark = BashOperator(
            task_id = 'spark_etl',
            bash_command='python3 /home/agnes/airflow/dags/airflow_project/spark_etl.py'
        )
        
    end = DummyOperator(
        task_id = "end"
    )

    # Orchestration
    (
        start
        >> dump
        >> data_transform
        >> end
    )

