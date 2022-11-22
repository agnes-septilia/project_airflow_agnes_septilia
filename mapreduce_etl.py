#!python3

""" NOTE TO MENTOR 
Karena program yang saya jalani tidak berhasil ketika pengambilan data dilakukan langsung dari koneksi postgres,
maka saya membuat script yg mengambil data dari csv

perintah yang saya gunakan untuk melihat hasil mapreduct adalah sebagai berikut:
python3 mapreduce_etl.py dataset/bigdata_transaction.csv
"""


""" TASK : Count total order per month using MapReduce """


# import libraries
import psycopg2
from mrjob.job import MRJob
from mrjob.step import MRStep


# define important variables
pg_conn = psycopg2.connect(database = "postgres", user = "postgres", password = "1234", host = "localhost", port = "5432")
cols = 'id_transaction,id_customer,date_transaction,product_transaction,amount_transaction'.split(',')


# create table in postgres
cur = pg_conn.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS project5.mrjob_trx_per_month (order_month varchar, order_count integer);")
pg_conn.commit() 


# Map Reduce JOb
class OrderMonthCount(MRJob):
    # define the connection to upload the result in postgres
    def reducer_init(self):
        self.conn = pg_conn
    
    # define the step of MRjob
    def steps(self):
        return [
			MRStep(mapper=self.mapper, reducer=self.reducer_aggregate),
			MRStep(reducer_init=self.reducer_init, reducer=self.sort, reducer_final=self.reducer_final)
		]

    # get key for the month from date_transaction, and value for counting the transaction
    def mapper(self, _, line):
        if line.split(',')[0] == 'id_transaction':
            pass
        else:
            row = dict(zip(cols, (line.split(','))))
            yield row['date_transaction'][5:7], 1
	
    # get key for the transaction month and value for sum of transaction
    def reducer_aggregate(self, key, values):
		#for 'order_date' compute
        yield None, (key, sum(values))

    # sort the result and upload to postgres table
    def sort(self, key, values):
        data = []
        for order_month, order_count in values:
            data.append((order_month, order_count))
            data.sort()

        for order_month, order_count in data:
            # yield order_month, order_count
            self.cur = self.conn.cursor()
            self.cur.execute("INSERT INTO project5.mrjob_trx_per_month (order_month, order_count) VALUES(%s, %s);", (order_month, order_count))
   
    
    # commit the connection
    def reducer_final(self):
        self.conn.commit()
        self.conn.close()

if __name__ == "__main__":
	OrderMonthCount.run()

pg_conn.close()
cur.close()