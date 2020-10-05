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

# 获取车辆时长
def countAllCarTimeByDay(dataCarByDay):
    et = '0000-00-00 00:00:00'
    st = '9999-99-99 99:99:00'
    for one in dataCarByDay:
        if one['pickup_datetime'] < st :
            st = one['pickup_datetime']
        if one['dropoff_datetime'] > et :
            et = one['dropoff_datetime']
    return {'starttime' : st, 'endtime' : et}

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

# 获取车辆时刻表
def getAllCarTimeByDay(dataMonth):
    dataAllCarByDay = dataMonth.map(lambda x : (x['vendorid'] + ';' + x['pickup_datetime'].split(' ')[0], x)).groupByKey()
    dataAllCarByDay = dataAllCarByDay.map(lambda x : (x[0].split(';')[0], countAllCarTimeByDay(x[1])))
    return dataAllCarByDay

# 获取当前所有车辆
def getAllCarNumByTime(dataCarTime, starttime, endtime):
    dataAllCarByTime = dataCarTime.filter(lambda x : x[1]['endtime'].split(' ')[1] > starttime and x[1]['starttime'].split(' ')[1] < endtime)
    return dataAllCarByTime.count()

zoomSize = zoom.count()
for month in range(6, 7):
    # 过滤月
    dataMonth = data.filter(lambda x : '-%02d-'%(month) in x['pickup_datetime'].split(' ')[0] and x['pickup_datetime'].split(' ')[0] == x['dropoff_datetime'].split(' ')[0])
    # 获取总天数
    days = getDaysByMonth(dataMonth)
    # 获取车辆总时间
    dataCarTime = getAllCarTimeByDay(dataMonth)
    for part in range(24 * 12):
        hour1 = int(part / 12)
        min1 = part % 12
        hour2 = int((part + 1) / 12)
        min2 = (part + 1) % 12
        starttime = '%02d:%02d:00'%(hour1, min1 * 5)
        endtime = '%02d:%02d:00'%(hour2, min2 * 5)
        print('%s-%s'%(starttime, endtime))

        print(getOrderCarNumByTime(dataMonth, starttime, endtime), getAllCarNumByTime(dataCarTime, starttime, endtime))
