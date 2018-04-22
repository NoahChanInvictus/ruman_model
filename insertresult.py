#-*-coding: utf-8-*-
#将csv里的预测数据导入数据库，需更改为由模型直接导入
from time_utils import *
from sql_utils import *
import csv

conn = default_db()
cur = conn.cursor()
sql = "SELECT * FROM stock_list"
cur.execute(sql)
results = cur.fetchall()
stock_dict = {}
for result in results:
    stock_dict[result['stock_id']] = [result['stock_name'],result['industry_name'],result['industry_code']]
f = csv.reader(open('/home/lfz/python/yaoyan/modelcode/csv/result2016new.csv'))
a = 0
for data in f:
    print a
    if a:
        stock_id = data[2]
        date = data[1]
        stock_name = stock_dict[stock_id][0]
        manipulate_type = 1
        industry_name = stock_dict[stock_id][1]
        industry_code = stock_dict[stock_id][2]
        probability = float(data[3])
        if probability >= 0.5:
            result = 1
        else:
            result = 0
        order = 'insert into manipulate_result ( stock_id,date,stock_name,manipulate_type,industry_name,industry_code,probability,result)values("%s", "%s","%s","%d","%s","%s","%f","%d")' % (stock_id,date,stock_name,manipulate_type,industry_name,industry_code,probability,result)
        try:
            cur.execute(order)
            conn.commit()
        except Exception, e:
            print e
    a += 1