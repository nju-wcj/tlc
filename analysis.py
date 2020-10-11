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
# 获取时间
def getTime(part):
    hour = int(part / 12)
    min = part % 12
    return '%02d:%02d'%(hour, min * 5)

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
        print(pickup_data_hour[pickup_data_hour == pickup_data_hour.max()].index[0], dropoff_data_hour[dropoff_data_hour == dropoff_data_hour.max()].index[0])
    # pickup_data_all['max'] = pickup_data_all.apply(lambda x : x[x == x.max()].index[0], axis = 1)
    return pickup_data_hour, dropoff_data_hour

# %%
# 订单数量变化图
def printPUGraph(pickup_data, zoom):
    pickup_data = pickup_data['%d'%(zoom)]
    pickup_data = pickup_data.map(lambda x : x['mean'])
    pickup_data.rename(index=lambda x : getTime(int(x)), inplace=True)
    plt.figure()
    pickup_data.plot()
    plt.title('avgpickup')
    plt.show()
    return pickup_data

# %%
# 需求数量变化图
def printDOGraph(dropoff_data, zoom):
    dropoff_data = dropoff_data['%d'%(zoom)]
    dropoff_data = dropoff_data.map(lambda x : x['mean'])
    dropoff_data.rename(index=lambda x : getTime(int(x)), inplace=True)
    plt.figure()
    dropoff_data.plot()
    plt.title('avgdropoff')
    plt.show()
    return dropoff_data

# %%
# 空载率变化图
def printRateGraph(rate_data):
    plt.figure()
    rate_data.rename(index=lambda x : getTime(int(x)), inplace=True)
    rate_data['mean'].plot()
    plt.title('rate')
    plt.show()
    return rate_data['mean']

# %%
# 需求供应变化图
def printPUDOGraph(pickup_data, dropoff_data, zoom):
    pickup_data = pickup_data['%d'%(zoom)]
    pickup_data = pickup_data.map(lambda x : x['mean'])
    pickup_data.rename('pu74', inplace=True)
    dropoff_data = dropoff_data['%d'%(zoom)]
    dropoff_data = dropoff_data.map(lambda x : x['mean'])
    dropoff_data.rename('do74', inplace=True)
    pudo_data = pandas.concat([pickup_data, dropoff_data], axis = 1)
    pudo_data['rotia'] = pudo_data.apply(lambda x : x['pu74'] / x['do74'], axis=1)
    pudo_data.rename(index=lambda x : getTime(int(x)), inplace=True)
    plt.figure()
    pudo_data['rotia'].plot()
    plt.title('rotia')
    plt.show()
    return pudo_data['rotia']

# %%
# 阶梯价格曲线
def printPriceGraph(pudo_data, rate_data):
    price_data = pandas.concat([pudo_data, rate_data], axis = 1)
    price_data['price'] = price_data.apply(lambda x : x['mean'] * 4 + x['rotia'] * 0.5, axis=1)
    price_data['price'] = price_data['price'].smooth()
    plt.figure()
    price_data['price'].plot()
    plt.title('price')
    plt.show()

# %%
# 分析pickup数据
def analysisData():
    pickup_data, dropoff_data, rate_data = initData()
    pickup_data = pickup_data.applymap(lambda x : dataToObject(x))
    dropoff_data = dropoff_data.applymap(lambda x : dataToObject(x))
    # printMaxZoom(pickup_data, dropoff_data)
    # printPUGraph(pickup_data, 74)
    # printDOGraph(dropoff_data, 74)
    pudo_data = printPUDOGraph(pickup_data, dropoff_data, 74)
    rate_data = printRateGraph(rate_data)
    printPriceGraph(pudo_data, rate_data)
    
# %%
# 数据分析
analysisData()
# %%

# %%
