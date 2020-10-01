from bs4 import BeautifulSoup
from urllib import request
import pandas
import os
import requests
import time
from clint.textui import progress
from boto3.session import Session
from botocore.exceptions import ClientError

def findurl(s3_client, bucket_name, object_name, expiration=3600):
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_name)
    except ClientError as e:
        return None
    return response

def fixData(url, header):
    if header:
        return True
    if (url.find('yellow') >= 0 and url.find('2019') >= 0):
        return True
    elif (url.find('yellow') >= 0 and url.find('2018') >= 0):
        return True
    elif (url.find('yellow') >= 0 and url.find('2017') >= 0):
        return True
    elif (url.find('yellow') >= 0 and url.find('2020') >= 0):
        return True
    elif (url.find('yellow') >= 0 and url.find('2020') >= 0):
        return True
    elif (url.find('green') >= 0 and url.find('2017') >= 0):
        return True
    elif (url.find('green') >= 0 and url.find('2018') >= 0):
        return True
    elif (url.find('green') >= 0 and url.find('2019') >= 0):
        return True
    elif (url.find('fhv') >= 0 and url.find('2020') >= 0):
        return True
    elif (url.find('fhv') >= 0 and url.find('2017') >= 0):
        return True
    elif (url.find('fhv') >= 0 and url.find('2018') >= 0):
        return True
    elif (url.find('fhv') >= 0 and url.find('2019') >= 0):
        return True
    return False

def getUrls(header=False):
    urls = []
    # 要请求的网络地址
    url = 'https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
    # 请求网络地址得到html网页代码
    html = request.urlopen(url)
    # 整理代码
    soup = BeautifulSoup(html, 'html.parser')
    # 找出所有的 a 标签， 因为所有的链接都在 a 标签内
    data = soup.find_all('a')
    # 遍历所有的 a 标签， 获取它们的 href 属性的值和它们的 text
    for item in data:
        if item.string is not None and item['href'] != 'javascript:;' and item['href'] != '#':
            url = item.get('href')
            if len(url) > 4 and url[-4:] == '.csv' and fixData(url, header):
                urls.append(url)
    return urls

# 进度条模块
def download(url, path, once):
    if once:	
        res = requests.get(url, stream=True)
        if res.status_code != 200:
            return
        total_length = int(res.headers.get('content-length'))

        with open(path, "wb") as f:
            for chunk in progress.bar(res.iter_content(chunk_size=1024), expected_size=(total_length/1024) + 1, width=100):
                if chunk:
                    f.write(chunk)
                    f.flush()
                    return
    else:
        request.urlretrieve(url, path)

# 获取只有表头的数据
def getHeaderData():
    for url in getUrls(True):
        names = url.split('/')
        name = 'data/' + names[len(names) - 1]
        print(name)
        if not os.path.exists(name):
            download(url, name, True)

# 获取数据表头
def saveHeader():
    header_df = pandas.DataFrame(columns = ['name', 'col'])
    list = os.listdir('data') #列出文件夹下所有的目录与文件
    for i in range(0, len(list)):
        path = os.path.join('data',list[i])
        if os.path.isfile(path):
            # 读取表头
            header = pandas.read_csv(path, nrows=0)
            print(list[i], header.columns)
            for col in header.columns:       
                header_df = header_df.append({'name' : list[i], 'col' : col}, ignore_index=True)
    header_df.to_csv('header.csv')



# 下载区域文件
def getZoomData():
    if not os.path.exists('taxi+_zone_lookup.csv'):
        download('https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv', 'taxi+_zone_lookup.csv')
        session = Session(aws_access_key_id='AKIAS7W4C45L2MV2WPVE', aws_secret_access_key='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', region_name='ap-northeast-1')
        s3 = session.client('s3')
        s3.upload_file('taxi+_zone_lookup.csv', 'tlc-data', 'taxi-zone.csv')

# 分析标签文件
def analysisHeader():
    header = pandas.read_csv('header.csv')
    # 删选17，18，19，20的数据
    names = ['fhv', 'green', 'yellow']
    years = ['2017', '2018', '2019', '2020']
    for name in names:
        header_name = header[header['name'].str.contains(name)]
        header_years = []
        for year in years:
            header_year = header_name[header_name['name'].str.contains(year)]
            header_years.append(header_year)
        header_name_years = pandas.concat(header_years)
        cols = header_name_years['col'].drop_duplicates().sort_values()
        print(cols)
        print('')
        print('')

# 上传2017-2020数据到s3
def getHeaderData():
    session = Session(aws_access_key_id='AKIAS7W4C45L2MV2WPVE', aws_secret_access_key='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', region_name='ap-northeast-1')
    s3 = session.client('s3')
    for url in getUrls(False):
        names = url.split('/')
        name = names[len(names) - 1]
        month = name.split('-')
        if len(month[1]) > 6:
            name = name.replace('-0', '-')
            url = url.replace('-0', '-')
        names3 = ''
        if 'fhv' in name:
            names3 = 'fhv/' + name
        elif 'green' in name:
            names3 = 'green/' + name
        elif 'yellow' in name:
            names3 = 'yellow/' + name
        # 判断文件是否存在
        print(url)
        if findurl(s3, 'tlc-data', names3) is None:
            download(url, name, False)
            print('start s3')
            s3.upload_file(name, 'tlc-data', names3)
            print('finish s3')
            os.remove(name)

def CombineFhv():
    session = Session(aws_access_key_id='AKIAS7W4C45L2MV2WPVE', aws_secret_access_key='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', region_name='ap-northeast-1')
    s3 = session.client('s3')
    objs = s3.list_objects(Bucket='tlc-data')
    for obj in objs['Contents']:
        name = obj['Key']
        if 'csv' in name and 'fhv' in name:
            print('down ' + name)
            s3.download_file('tlc-data', name, name)
            print('start modify')
            # 修改列
            # csv = pandas.read_csv(name)
            chunks = pandas.read_csv(name, chunksize=2000000)
            num = 0
            for csv in chunks:
                if 'SR_Flag' in csv.columns:
                    csv = csv.drop('SR_Flag', 1)
                if 'Dispatching_base_number' in csv.columns:
                    csv = csv.drop('Dispatching_base_num', 1)
                csv.columns = csv.columns.str.lower()
                csv.rename(columns=lambda x:x.replace('number','num'), inplace=True)
                csv.to_csv('' + num + '-' + name, index=0)
                print('end modify')
                path = 'fhv/' + num + '-' + name.split('/')[1]
                print('up ' + path)
                s3.upload_file('' + num + '-' + name, 'tlc-data', path)
                os.remove('' + num + '-' + name)
                num += 1

# getZoomData()
# getHeaderData()
# saveHeader()
# analysisHeader()
# getHeaderData()

CombineFhv()