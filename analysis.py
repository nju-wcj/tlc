# %%
# 依赖
import os
import sys
import pandas
import math
import json
from boto3.session import Session
import matplotlib.pyplot as plt

# %%
# 获取数据结果
def getResult():
    session = Session(aws_access_key_id='AKIAS7W4C45L2MV2WPVE', aws_secret_access_key='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', region_name='ap-northeast-1')
    s3 = session.client('s3')
    for i in range(1, 13):
        s3.download_file('tlc-data', '%02d/pus.cvs'%(i), 'res/%02d/pus.csv'%(i))
        s3.download_file('tlc-data', '%02d/dos.cvs'%(i), 'res/%02d/dos.csv'%(i))

# getResult()
# %%
# 加载数据
def initData():
    pickup_data = pandas.read_csv('pus.csv')
    dropoff_data = pandas.read_csv('dos.csv')
    rate_data = pandas.read_csv('crs.csv')
    return pickup_data, dropoff_data, rate_data

# %%
# 将字符串解析为对象
def dataToObject(data):
    if isinstance(data, str):
        return json.loads(data.replace('\'', '"').replace('nan', '0')) 
    else:
        return {'mean':0, 'std':0, 'count':0}

# %%
# 数据最多区域
def printMaxZoom(pickup_data, dropoff_data):
    # 找到数据量最多的区域
    pickup_data_all = pickup_data.applymap(lambda x : x['mean'] * x['count'])
    dropoff_data_all = dropoff_data.applymap(lambda x : x['mean'] * x['count'])
    # 找到每个小时最多的区域
    for i in range(24):
        pickup_data_hour = pickup_data_all[i : i * 12].sum()
        dropoff_data_hour = dropoff_data_all[i : i * 12].sum()
        # print(pickup_data_hour[pickup_data_hour == pickup_data_hour.max()].index[0])
        print(dropoff_data_hour[dropoff_data_hour == dropoff_data_hour.max()].index[0])
    # pickup_data_all['max'] = pickup_data_all.apply(lambda x : x[x == x.max()].index[0], axis = 1)

# %%
# 订单数量变化图
def printPUGraph(pickup_data, zoom):
    pickup_data = pickup_data['%d'%(zoom)]
    pickup_data = pickup_data.map(lambda x : x['mean'])
    plt.figure()
    pickup_data.plot()
    plt.title('avgpickup')
    plt.show()

# %%
# 需求数量变化图
def printDOGraph(dropoff_data, zoom):
    dropoff_data = dropoff_data['%d'%(zoom)]
    dropoff_data = dropoff_data.map(lambda x : x['mean'])
    plt.figure()
    dropoff_data.plot()
    plt.title('avgdropoff')
    plt.show()

# %%
# 空载率变化图
def printRateGraph(rate_data):
    plt.figure()
    rate_data['mean'].plot()
    plt.title('rate')
    plt.show()

# %%
# 分析pickup数据
def analysisData():
    pickup_data, dropoff_data, rate_data = initData()
    pickup_data = pickup_data.applymap(lambda x : dataToObject(x))
    dropoff_data = dropoff_data.applymap(lambda x : dataToObject(x))
    # printMaxZoom(pickup_data, dropoff_data)
    printPUGraph(pickup_data, 74)
    printDOGraph(dropoff_data, 74)
    printRateGraph(rate_data)

    
# %%
# 数据分析
analysisData()
# %%
