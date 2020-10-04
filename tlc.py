from pyspark import SparkContext
from pyspark.sql import SQLContext
import os
import sys
os.environ['PYSPARK_PYTHON']='/usr/local/bin/python3'
sys.setrecursionlimit(9000000)

sc = SparkContext( 'local', 'tlc')
sqlContext = SQLContext(sc) 
zoom = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('taxi+_zone_lookup.csv')
data0 = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('fhv_tripdata_2020-06.csv')
data1 = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('fhvhv_tripdata_2020-06.csv')
data2 = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('green_tripdata_2020-06.csv')
data3 = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('yellow_tripdata_2020-06.csv')
data0 = data0.select('vendorid','pickup_datetime','dropoff_datetime','pulocationid','dolocationid')
data1 = data1.select('vendorid','pickup_datetime','dropoff_datetime','pulocationid','dolocationid')
data2 = data2.select('vendorid','pickup_datetime','dropoff_datetime','pulocationid','dolocationid')
data3 = data3.select('vendorid','pickup_datetime','dropoff_datetime','pulocationid','dolocationid')
data = data0.unionByName(data1).unionByName(data2).unionByName(data3)
data = data.rdd

zoomSize = zoom.count()
for month in range(6, 7):
    for part in range(24 * 12):
        hour1 = int(part / 12)
        min1 = part % 12
        hour2 = int((part + 1) / 12)
        min2 = (part + 1) % 12
        starttime = '%02d:%02d:00'%(hour1, min1 * 5)
        endtime = '%02d:%02d:00'%(hour2, min2 * 5)
        data = data.filter(lambda x : '-%02d-'%(month) in x['pickup_datetime'].split(' ')[0] and x['pickup_datetime'].split(' ')[1] > starttime and x['pickup_datetime'].split(' ')[1] < endtime)
        # data = data.map(lambda x : (x['pulocationid'], x))
        print('%s-%s'%(starttime, endtime))
        data.foreach(print)
