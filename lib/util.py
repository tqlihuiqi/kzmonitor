# -*-coding:utf-8-*-

import time
from hashlib import md5

def md5Name(name):
    """ 传入字符串返回10位的md5值 """
    
    return md5(name.encode("utf8")).hexdigest()[:10]

def timeColumn(timestamp):
    """ 
    传入timestamp值
    返回 timestamp timestamp10m timestamp1h
    """
    return (timestamp, (timestamp / 300) * 300, (timestamp / 600) * 600, (timestamp / 3600) * 3600)

def selectColumn(interval):
    """ 
    根据传入的interval选择使用的timestamp列 
    interval = endTime - startTime
    """

    minute30 = 1800
    hour3 = 3600*3
    hour12 = 3600*12

    if interval <= minute30:
        column = 'timestamp'
    elif interval > minute30 and interval <= hour3:
        column = 'timestamp5m'
    elif interval > hour3 and interval <= hour12:
        column = 'timestamp10m'
    elif interval > hour12:
        column = 'timestamp1h'
    else:
        raise ValueError('Invalid interval: %s' % interval)

    return column

def toTimestamp(datetime, formatter='%Y-%m-%d %H:%M:%S'):
    """ 传入datetime, 返回timestamp """

    fmtTime = time.strptime(datetime, formatter)
    timeTs = time.mktime(fmtTime)
    return int(timeTs)