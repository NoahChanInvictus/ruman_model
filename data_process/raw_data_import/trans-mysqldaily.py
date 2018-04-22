#!/usr/bin/env python
# encoding: utf-8
#将ES里的大宗交易数据转移到mysql
import requests
import json
import time
import elasticsearch
from elasticsearch import Elasticsearch
import tushare as ts
import pandas as pd
import tushare as ts
import pandas as pd
import datetime
from config import *
from sql_utils import *
import sys
import codecs
import csv 
from config import *
import time_utils
import datetime
from time_utils import *
import pymysql
import os
import sys

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



def es_trans(query,index_name,type_name,size,timenow):
    '''
    input：
        query:关键词;start_day,end_day:起止日期;
        index_name,type_name:ES配置参数;
        size,score:ES控制参数
    return:
        content中含有query的文本内容
    '''
    # print start_time
    # print end_time
    conn = default_db()
    cur = conn.cursor()
    es = Elasticsearch([{'host': '219.224.134.214', 'port': '9202'}])
    query_body = {"size":size,"query":{"bool": { "must":[{"match":{ "publish_time":timenow}}]}}}
    res = es.search(index='east_money', doc_type="type1",body=query_body, request_timeout=100)
    hits = res['hits']['hits']
    results = []
    for item in hits:
        res = item['_source']
        if 'stock_name' in res:
            stock_name=res['stock_name']
        else:
            stock_name="None"      	
        date=res['date']
        stock_id=res['stock_id']
        #stock_name=res['stock_name']
        transaction_price=float(res['transaction_price'])
        closing_price_today=float(res['closing_price_today'])
        transaction_number=float(res['transaction_number'])
        transaction_amount=float(res['transaction_amount'])
        discount_ratio=float(res['Discount_ratio'])
        buyer=res['Buyer']
        seller=res['Seller']
        order = 'insert into ' + 'large_trans' + '(stock_id,stock_name,date,\
        transaction_price\
        ,closing_price_today,transaction_number,transaction_amount,discount_ratio,buyer,seller)values("%s","%s","%s","%.4f","%.4f","%.4f","%.4f","%.4f","%s","%s")' % (stock_id, stock_name, date, transaction_price,closing_price_today,transaction_number, transaction_amount,discount_ratio,buyer,seller)
        try:
            cur.execute(order)
            conn.commit()
        except Exception, e:
            print e
        print 'good'
    return results


def test(shijian):
    query=1
    index_name='east_money'
    type_name= "type1"
    size=300
    score=1
    stock="000155"
    timenow=shijian
    a=es_trans(query,index_name,type_name,size,timenow)
 

if __name__=="__main__":
    today = time.strftime("%Y-%m-%d",time.localtime(int(time.time())))
    test(today)



