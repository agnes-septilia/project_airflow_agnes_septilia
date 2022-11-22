#!python3

""" TASK : Count total order per month using Spark """

# import libraries
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import psycopg2


# define connection
pg_conn = psycopg2.connect(database = "postgres", user = "postgres", password = "1234", host = "localhost", port = "5432")


# create table in postgres
cur = pg_conn.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS project5.spark_trx_per_month (order_month varchar, order_count integer);")


# initiate Spark session
spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("project5") \
    .getOrCreate()

spark_session = spark


# read data
data = spark.read. \
    format("csv"). \
    option("inferSchema", "true"). \
    option("header", "true"). \
    load("/home/agnes/airflow/dags/airflow_project/dataset/bigdata_transaction.csv")

# print(data.show())
# print(data.printSchema())


# group and sort
data = data.groupBy(substring('date_transaction', 6,2).alias('order_month')) \
    .agg(count('id_transaction').alias('order_count')) \
    .sort('order_month') 

# data.show()


# upload result to postgres
data_df = data.toPandas()
for index, row in data_df.iterrows():
    cur.execute("INSERT INTO project5.spark_trx_per_month (order_month, order_count) VALUES(%s, %s);", (row[0], row[1]))

# # close session
spark_session.stop()

# # close connection
pg_conn.commit() 
pg_conn.close()
cur.close()
