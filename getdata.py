from bs4 import BeautifulSoup
from urllib import request
import os
import requests
import time
from clint.textui import progress

def fixData(url, header):
    if header:
        return True
    if (url.find('yellow') >= 0 and url.find('2019') >= 0):
        return True
    # elif (url.find('yellow') >= 0 and url.find('2018') >= 0):
    #     return True
    # elif (url.find('yellow') >= 0 and url.find('2017') >= 0):
    #     return True
    # elif (url.find('green') >= 0 and url.find('2017') >= 0):
    #     return True
    # elif (url.find('green') >= 0 and url.find('2018') >= 0):
    #     return True
    elif (url.find('green') >= 0 and url.find('2019') >= 0):
        return True
    return False

def getUrls(header=True):
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
    res = requests.get(url, stream=True)
    if res.status_code != 200:
        return
    total_length = int(res.headers.get('content-length'))

    with open(path, "wb") as pypkg:
        for chunk in progress.bar(res.iter_content(chunk_size=1024), expected_size=(total_length/1024) + 1, width=100):
            if chunk:
                pypkg.write(chunk)
            if once:
                return

def getHeader():
    for url in getUrls(True):
        names = url.split('/')
        name = names[len(names) - 1]
        if not os.path.exists(name):
            download(url, name, True)

# 下载区域
if not os.path.exists('taxi+_zone_lookup.csv'):
    download('https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv', 'taxi+_zone_lookup.csv')


getHeader()

