#!python3

""" NOTE TO MENTOR
Saya tidak menggunakan import os -> getcwd() karena saat dijalankan di airflow, path tidak terdeteksi
"""

import pandas as pd
import numpy as np

import psycopg2
from sqlalchemy import create_engine


if __name__ == "__main__":

    listFile = ['bigdata_customer', 'bigdata_product', 'bigdata_transaction']
	
    for file in listFile :

        # read data

        df = pd.read_csv('/home/agnes/airflow/dags/airflow_project/dataset/' + file + '.csv')
        # print(df.head(3))
        
        # connection
        url = 'postgresql://postgres:1234@localhost:5432/postgres'
        engine = create_engine(url)

        # dump data
        try:
            df.to_sql(file, index=False, con=engine, schema='project5', if_exists='replace')
            print(f"Data {file} Success Dump to Database")
        except:
            print(f"Data {file} Failed")
