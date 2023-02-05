from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import os, sys, time
from datetime import datetime

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.master("spark://192.168.0.2:7077").appName("Queries Final").getOrCreate()

print("spark session created")


#### ------1--------####


parDF=spark.read.option("header", "true").option("inferSchema", "true").parquet("hdfs://master:9000/par")
schema1 = "LocationID LONG, Borough STRING, Zone STRING, service_zone STRING"
csvDF=spark.read.option("header", "true").csv("hdfs://master:9000/csv",schema=schema1)

parDF=parDF.filter((month(parDF['tpep_pickup_datetime']) < 7) & (year(parDF['tpep_pickup_datetime']) == 2022))
parDF=parDF.filter((parDF['fare_amount'] > 0) & (parDF['tip_amount'] >= 0) & (parDF['passenger_count'] > 0) & (parDF['trip_distance'] >= 0) & (parDF['extra'] >= 0) & (parDF['mta_tax'] >= 0) & (parDF['tolls_amount'] >= 0) & (parDF['total_amount'] >= 0) & (parDF['airport_fee'] >= 0) & (parDF['tpep_dropoff_datetime'] > parDF['tpep_pickup_datetime']) & (parDF['improvement_surcharge'] >= 0) & (parDF['congestion_surcharge'] >= 0))

parRDD = parDF.rdd
csvRDD = csvDF.rdd

parDF.count()
csvDF.count()
parDF.printSchema()
csvDF.printSchema()
#parDF.show(10)
#csvDF.show(10)

#### ------2--------####

start = time.time()


q=parDF.withColumn("date_type",to_date("tpep_pickup_datetime","YYYY/MM/DD")).withColumn('month',month("date_type"))

q1=q.join(csvDF, csvDF.LocationID ==  q.DOLocationID, "inner")
q1=q1.filter(q1['month'] == 3).filter(q1['Zone'] == 'Battery Park')
q1=q1.filter(q1['tip_amount'] == q1.select(max("tip_amount")).head()[0]).collect()

end = time.time()

print()
print()
print(f'Time taken for the first query: {end-start} seconds.')


start = time.time()

q2=q.filter(q['tolls_amount'] > 0).groupBy('month').max('tolls_amount').orderBy('month')
q2=q.join(q2, (q2['month'] ==  q['month']) & (q2['max(tolls_amount)'] == q['tolls_amount']), "inner").collect()

end = time.time()

print()
print()
print(f'Time taken for the second query: {end-start} seconds.')



#### ------3--------####

start = time.time()

q3DF=q.filter(q['PULocationID'] != q['DOLocationID'])
q3DF=q3DF.withColumn('date_segment', dayofyear("tpep_pickup_datetime"))
q3DF=q3DF.withColumn('date_segment', col('date_segment')/15)
q3DF=q3DF.withColumn('date_segment', q3DF['date_segment'].cast('int'))
q3DF=q3DF.groupBy('date_segment').avg('trip_distance', 'total_amount').orderBy('date_segment').collect()

end = time.time()

print()
print()
print(f'Time taken for the third query (DataFrame): {end-start} seconds.')


def day_of_year(datetime_str):
    return datetime.strptime(datetime_str, '%Y-%m-%d').timetuple().tm_yday 

start = time.time()

q3RDD=parRDD.filter(lambda x: x.PULocationID != x.DOLocationID)
q3RDD=q3RDD.map(lambda x: (day_of_year(str(x.tpep_pickup_datetime)[:10])//15, (float(x.trip_distance),float(x.total_amount),1)))\
    .reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1],a[2]+b[2]))\
    .mapValues(lambda x: (x[0]/x[2],x[1]/x[2]))\
    .map(lambda x: (x[0], x[1][0], x[1][1]))\
    .sortBy(lambda x: x[0])

print()
print()


for y in q3RDD.collect():
    print(y)

end = time.time()

print()
print()
print(f'Time taken for the third query (RDD): {end-start} seconds.')


#### ------4--------####

start = time.time()

q=parDF.withColumn("date_type",to_date("tpep_pickup_datetime","YYYY/MM/DD")).withColumn('month',month("date_type"))
q4=q.withColumn('day_int', dayofweek("tpep_pickup_datetime")).withColumn('day', date_format('tpep_pickup_datetime', 'EEEE'))
q4=q4.withColumn('hour', hour("tpep_pickup_datetime")) 
q4=q4.groupBy('day_int', 'day', 'hour').sum('passenger_count').orderBy('day_int',desc('sum(passenger_count)'))
q4=q4.withColumn('sum(passenger_count)', q4['sum(passenger_count)'].cast('int')).withColumnRenamed('sum(passenger_count)', 'sum_passengers')
window = Window.partitionBy("day_int").orderBy(col("sum_passengers").desc())
q4=q4.withColumn("rank",row_number().over(window))
q4=q4.filter(col("rank") <= 3).drop('day_int').collect()

end = time.time()

print()
print()
print(f'Time taken for the fourth query: {end-start} seconds.')

start = time.time()

q=q.withColumn('day', dayofyear("tpep_pickup_datetime"))

q=parDF.withColumn("date_type",to_date("tpep_pickup_datetime","YYYY/MM/DD")).withColumn('month',date_format("date_type", "MMMM")).withColumn('month_int', month('date_type')).withColumn('day', date_format("tpep_pickup_datetime", "d")).withColumn('tip_percentage', col("tip_amount")/col("fare_amount")*100)
q5=q.groupBy('month', 'day').avg('tip_percentage').orderBy(desc('avg(tip_percentage)'))
q=q.select(q.day, q.month, q.month_int).distinct().orderBy('month', 'day')
q5=q.join(q5, (q5.day == q.day) & (q5.month == q.month)).drop(q5.day).drop(q5.month)
window = Window.partitionBy("month").orderBy(col("avg(tip_percentage)").desc())
q5=q5.withColumn("rank",row_number().over(window))
q5=q5.filter(col("rank") <= 5).orderBy('month_int', 'rank').drop('month_int').collect()

end = time.time()

print()
print()
print(f'Time taken for the fifth query: {end-start} seconds.')