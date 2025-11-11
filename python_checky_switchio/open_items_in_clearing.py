#!/usr/bin/python3.9
import pymysql
import sys
import logging
from datetime import datetime, timedelta

critical_count = 1

logfile = '/tmp/clearing_open_check.log'
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.INFO,
                    handlers=[
                        logging.FileHandler(logfile),
                        logging.StreamHandler()
                    ]
)

def check_clearing_stage():
    connection = pymysql.connect(
        host='db',
        user='user',
        password='heslo',
        db='fare',
        port=3306
    )
    cursor = connection.cursor(pymysql.cursors.DictCursor)

    cursor.execute("SELECT oper_id, code FROM operator WHERE entity_type = 'operator'")
    operators = cursor.fetchall()

    business_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    exit_status = 0

    for operator in operators:
        oper_id = operator['oper_id']
        code = operator['code']

        cursor.execute("""
            SELECT cld_id FROM clearing_day
            WHERE oper_id = %s AND business_date = %s AND clearing_stage = 'Closed' and cld_id not like '%%PAYMENT%%'
        """, (oper_id, business_date))
        clearing_days = cursor.fetchall()

        for clearing_day in clearing_days:
            cld_id = clearing_day['cld_id']

            cursor.execute("""
                SELECT COUNT(*) as open_count
                FROM clearing_item
                WHERE clearing_day_id = %s AND clearing_stage = 'Open'
            """, (cld_id,))
            result = cursor.fetchone()
            open_count = result['open_count']

            if open_count >= critical_count:
                logging.critical(f"Operator {code} ma Open: [{open_count}] items v uzavrenem clearing_day {cld_id}! pro BD: {business_date}")
                exit_status = max(exit_status, 2)
            else:
                logging.info(f"OK: Operator {code}, nema zadne clearing_items ve stavu open pro clearing_day: {cld_id}.")

    cursor.close()
    connection.close()
    sys.exit(exit_status)

if __name__ == "__main__":
    check_clearing_stage()