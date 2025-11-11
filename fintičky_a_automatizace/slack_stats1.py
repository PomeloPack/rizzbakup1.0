#!/usr/bin/python3

import pymysql
import logging
import os
from datetime import datetime, timedelta, time

logging.basicConfig(format='%(message)s', level=logging.DEBUG)

current_date = datetime.now().date()
back_in_time = current_date - timedelta(days=1)
business_day = back_in_time.strftime("%Y-%m-%d")
start = datetime.combine(back_in_time, time.min)
f_end = datetime.combine(back_in_time, time.max)
end = f_end.replace(microsecond=0)

def mysqlconnect():
        conn = pymysql.connect(host='pmydbtrans-vip01-spc',user='app_fare',password='fare@2016',db='fare')
        cur = conn.cursor()
        cur.execute(f"select count(*) from tap where server_dttm between '{start}' and '{end}';")
        output = cur.fetchone()
        result = output[0]
        logging.info(f"{business_day} provedeno celkove tapu: {result}")
        query = (f"select b.code, count(*) as counter from tap a left join operator b ON a.oper_id = b.oper_id where a.server_dttm between '{start}' and '{end}' group by b.code order by counter desc;")
        cur.execute(query)
        output2 = cur.fetchall()
        for taps in output2:
            operator_code = taps[0]
            operator_count = taps[1]
            logging.info(f"{operator_code}: {operator_count}")
        conn.close()

if __name__ == "__main__" :
    mysqlconnect()