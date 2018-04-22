#-*-coding: utf-8-*-
#统计每只股票每天的大宗交易的频率
#-*-coding: utf-8-*-
import tushare as ts
import pandas as pd
import datetime
from config import *
from sql_utils import *
import time
import sys
import codecs
import csv
from config import *
import time_utils
import datetime
from time_utils import *
# -*- coding:utf-8 -*-
import pymysql
import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import TransportError
from elasticsearch.helpers import bulk

def datetime2ts(date):
    return int(time.mktime(time.strptime(date, '%Y-%m-%d')))
    #print line

#######时间与时间戳的转换
def ts2datetime(ts):
    return time.strftime('%Y-%m-%d', time.localtime(ts))
def tostr(year,month,day):
    date = str(year)+'-'+str(month)+'-'+str(day)
    return date
def datetime2ts(date):
    return int(time.mktime(time.strptime(date, '%Y-%m-%d')))
reload(sys)
sys.setdefaultencoding('utf-8')

######生成时间列表
def datelist(year1,month1,day1,year2,month2,day2):
    date_list = []
    begin_date = datetime.datetime.strptime(tostr(year1,month1,day1), "%Y-%m-%d")
    end_date = datetime.datetime.strptime(tostr(year2,month2,day2), "%Y-%m-%d")
    while begin_date <= end_date:
        date_str = begin_date.strftime("%Y-%m-%d")
        date_list.append(date_str)
        begin_date += datetime.timedelta(days=1)   #输出时间列表的函数
    return date_list

def test(code,nowdate,stock_name):
    conn = default_db()
    cur = conn.cursor()
    code = code
    dates = nowdate
    stock_name=stock_name
    cur.execute("SELECT * FROM large_trans WHERE stock_id = '%s'" % (code))
    results = cur.fetchall()
    frequency=0
    for result in results:
        if result['date'] == dates:
            frequency = frequency+1
            print result['date'],dates
    order = 'insert into ' + 'transaction_stat' + '( stock_id,stock_name,date,frequency)values("%s","%s","%s","%d")' % (code, stock_name, dates, frequency)

    try:
        cur.execute(order)
        conn.commit()
    except Exception, e:
        print e



if __name__ == "__main__":
    nowdate = time.strftime("%Y-%m-%d",time.localtime(int(time.time())))
    b = ts.get_today_all()
    for j in range(len(b.index)):
        stock = b.loc[b.index[j]]['code']
        name = b.loc[b.index[j]]['name']
        test(stock,nowdate,name)