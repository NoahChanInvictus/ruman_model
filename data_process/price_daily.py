# -*- coding:utf-8 -*-
# 处理每日股价数据
# 
from raw_data_import.getprice import get_market_daily

def price_daily(theday):
    get_market_daily(theday)        #2018-03-02数据已更新

if __name__ == '__main__':
    price_daily('2018-03-03')