# -*- coding:utf-8 -*-
# step1:从东方财富网获取大宗交易数据存入es
# step2:从es中读数据进行统计，将结果存入es
from raw_data_import.eastMoneyDaily import eastMoney

def trans_daily(theday):
    eastMoney(theday)           #从东方财富网获取大宗交易数据存入es
