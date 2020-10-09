# %%
import os
import sys
import pandas
import math
import json
from boto3.session import Session

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
# 分析pickup数据
def analysisData():
    pickup_data, dropoff_data, rate_data = initData()
    pickup_data = pickup_data.applymap(lambda x : dataToObject(x))
    dropoff_data = dropoff_data.applymap(lambda x : dataToObject(x))
    rate_data = rate_data.applymap(lambda x : dataToObject(x))
    # 找到数据量最多的区域
    pickup_data_all = pickup_data.applymap(lambda x : x['mean'] * x['count'])
    print(pickup_data_all)
# %%
analysisData()
# %%
