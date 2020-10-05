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

# 获取车辆当天工作总时间

# 获取统计该月的总天数
def getDaysByMonth(dataMonth):
    dataByDay = dataMonth.map(lambda x : (x['pickup_datetime'].split(' ')[0], 1)).groupByKey()
    return dataByDay.count()

# 获取pickup订单数
def getZoomPUNumByTime(dataMonth, starttime, endtime):
    dataBytime = dataMonth.filter(lambda x : x['pickup_datetime'].split(' ')[1] > starttime and x['pickup_datetime'].split(' ')[1] < endtime)
    countByzoom = dataBytime.map(lambda x : ('264', x) if x['pulocationid'] is None else (x['pulocationid'], x)).countByKey()
    return countByzoom
# 获取dropoff订单数
def getZoomDONumByTime(dataMonth, starttime, endtime):
    dataBytime = dataMonth.filter(lambda x : x['dropoff_datetime'].split(' ')[1] > starttime and x['dropoff_datetime'].split(' ')[1] < endtime)
    countByzoom = dataBytime.map(lambda x : ('264', x) if x['dolocationid'] is None else (x['dolocationid'], x)).countByKey()
    return countByzoom
# 获取正在进行的车辆及其订单
def getOrderCarNumByTime(dataMonth, starttime, endtime):
    dataBytime = dataMonth.filter(lambda x : x['dropoff_datetime'].split(' ')[1] > starttime and x['pickup_datetime'].split(' ')[1] < endtime)
    dataBycar = dataBytime.map(lambda x : (x['vendorid'], x)).groupByKey()
    return dataBycar.count(), dataBytime.count()
# 获取车辆非空载比例
def getRateByTime(dataMonth, starttime, endtime):
    dataCarByDay = dataMonth.map(lambda x : (x['vendorid'], x)).groupByKey().map(lambda x : (x[0], getAllTime(x[1])))
    return 0

zoomSize = zoom.count()
for month in range(6, 7):
    # 过滤月
    dataMonth = data.filter(lambda x : '-%02d-'%(month) in x['pickup_datetime'].split(' ')[0])
    # 获取总天数
    days = getDaysByMonth(dataMonth)
    for part in range(24 * 12):
        hour1 = int(part / 12)
        min1 = part % 12
        hour2 = int((part + 1) / 12)
        min2 = (part + 1) % 12
        starttime = '%02d:%02d:00'%(hour1, min1 * 5)
        endtime = '%02d:%02d:00'%(hour2, min2 * 5)
        print('%s-%s'%(starttime, endtime))
        # 获取统一时刻订单数量
        print(getOrderCarNumByTime(dataMonth, starttime, endtime))
