#!/usr/bin/env python
# coding: utf-8

# In[20]:


# 依赖库
from pyspark import SparkContext
from pyspark.sql import SQLContext
import os
import sys
import pandas
from datetime import datetime
import time
# os.environ['PYSPARK_PYTHON']='/usr/local/bin/python3'
# os.environ['JAVA_HOME'] = 'C:\Program Files (x86)\Java\jdk1.8.0_112'
sys.setrecursionlimit(9000000)


# In[2]:


# 初始化数据
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


# In[3]:


# 检验统计量
def statistics(list):
    data = pandas.Series(list)
    return {'mean': data.mean(), 'std': data.std()}


# In[4]:


# 获取统计该月的总天数
def getDaysByMonth(dataMonth):
    dataByDay = dataMonth.map(lambda x : (x[0].split(';')[1], 1)).groupByKey()
    return dataByDay.count()


# In[5]:


# 获取pickup订单数
def getZoomPUNumByTime(dataMonth, starttime, endtime):
    dataBytime = dataMonth.filter(lambda x : x[1]['pickup_datetime'].split(' ')[1] > starttime and x[1]['pickup_datetime'].split(' ')[1] < endtime)
    countByzoom = dataBytime.map(lambda x : ('264' + ';' + x[0].split(';')[1], 1) if x[1]['pulocationid'] is None else (x[1]['pulocationid'] + ';' + x[0].split(';')[1], 1))
    countByzoom = countByzoom.reduceByKey(lambda x, y : x + y)
    countByzoom = countByzoom.map(lambda x : (x[0].split(';')[0], x[1])).groupByKey()
    countByzoom = countByzoom.map(lambda x : {x[0] : statistics(list(x[1]))})
    return countByzoom.collect()


# In[6]:


# 获取dropoff订单数
def getZoomDONumByTime(dataMonth, starttime, endtime):
    dataBytime = dataMonth.filter(lambda x : x[1]['dropoff_datetime'].split(' ')[1] > starttime and x[1]['dropoff_datetime'].split(' ')[1] < endtime)
    countByzoom = dataBytime.map(lambda x : ('264' + ';' + x[0].split(';')[1], 1) if x[1]['dolocationid'] is None else (x[1]['dolocationid'] + ';' + x[0].split(';')[1], 1))
    countByzoom = countByzoom.reduceByKey(lambda x, y : x + y)
    countByzoom = countByzoom.map(lambda x : (x[0].split(';')[0], x[1])).groupByKey()
    countByzoom = countByzoom.map(lambda x : {x[0] : statistics(list(x[1]))})
    return countByzoom.collect()


# In[36]:


# 获取车辆时刻表
def getAllCarTimeByDay(dataMonth):
    dataAllCarByDay = dataMonth.groupByKey().map(lambda x : (x[0], countAllCarTimeByDay(x[1])))
    return dataAllCarByDay
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


# In[56]:


# 判断车辆是否在工作时间
def judgeCarTime(dataCarByDay, starttime, endtime):
    starttime = int(time.mktime(time.strptime(starttime, '%H:%M:%S')))
    endtime = int(time.mktime(time.strptime(endtime, '%H:%M:%S')))   
    for one in dataCarByDay:
        putime = int(time.mktime(time.strptime(one['pickup_datetime'].split(' ')[1], '%H:%M:%S')))
        dotime = int(time.mktime(time.strptime(one['dropoff_datetime'].split(' ')[1], '%H:%M:%S')))
        if (starttime - dotime < 3600 and putime - endtime < 3600):
            return True
    return False


# In[57]:


# 获取车辆工作比
def getCarRateByTime(dataMonth, starttime, endtime):
#     dataCarByTime = dataCarMonth.filter(lambda x : x[1]['endtime'].split(' ')[1] > starttime and x[1]['starttime'].split(' ')[1] < endtime)
#     dataCarByTime = dataCarByTime.map(lambda x : (x[0].split(';')[1], 1))
#     dataCarByTime = dataCarByTime.reduceByKey(lambda x, y : x + y)
    dataBytime = dataMonth.filter(lambda x : x[1]['dropoff_datetime'].split(' ')[1] > starttime and x[1]['pickup_datetime'].split(' ')[1] < endtime)
    dataBytime = dataBytime.map(lambda x : (x[0], 1)).distinct()
    dataBytime = dataBytime.map(lambda x : (x[0].split(';')[1], 1))
    dataBytime = dataBytime.reduceByKey(lambda x, y : x + y)
    dataCarByTime = dataMonth.groupByKey()
    dataCarByTime = dataCarByTime.filter(lambda x : judgeCarTime(x[1], starttime, endtime))
    dataCarByTime = dataCarByTime.map(lambda x : (x[0].split(';')[1], 1))
    dataCarByTime = dataCarByTime.reduceByKey(lambda x, y : x + y)
    dataRate = dataCarByTime.fullOuterJoin(dataBytime)
    print(dataRate.collect())
    dataRate = dataRate.map(lambda x : 0.0 if x[1][1] is None or x[1][0] is None else x[1][1] / x[1][0])
    return statistics(dataRate.collect())


# In[58]:


zoomSize = zoom.count()
for month in range(6, 7):
    # 清洗空数据
    dataMonth = data.filter(lambda x : x['vendorid'] is not None and  x['pickup_datetime'] is not None and x['dropoff_datetime'] is not None and x['pulocationid'] is not None and x['dolocationid'] is not None)
    # 过滤月
    dataMonth = dataMonth.filter(lambda x : '-%02d-'%(month) in x['pickup_datetime'].split(' ')[0] and x['pickup_datetime'].split(' ')[0] == x['dropoff_datetime'].split(' ')[0])
    dataMonth = dataMonth.map(lambda x : (x['vendorid'] + ';' + x['pickup_datetime'].split(' ')[0], x))
    # 获取车辆总时间
#     dataCarMonth = getAllCarTimeByDay(dataMonth)
    for part in range(24 * 12):
        hour1 = int(part / 12)
        min1 = part % 12
        hour2 = int((part + 1) / 12)
        min2 = (part + 1) % 12
        starttime = '%02d:%02d:00'%(hour1, min1 * 5)
        endtime = '%02d:%02d:00'%(hour2, min2 * 5)
        print('%s-%s'%(starttime, endtime))
        cr = getCarRateByTime(dataMonth, starttime, endtime)
        print('---cr---')
        print(cr)
        pu = getZoomPUNumByTime(dataMonth, starttime, endtime)
        print('---pu---')
        print(pu)
        do = getZoomDONumByTime(dataMonth, starttime, endtime)
        print('---do---')
        print(do)


# In[ ]:




