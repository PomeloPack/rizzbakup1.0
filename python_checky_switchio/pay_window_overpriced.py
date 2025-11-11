#!/usr/bin/python3.9

import pymysql
import sys
import logging
from datetime import datetime, timedelta

#Logging setup
LOG_FILE = "/tmp/check_pw_extracharged.log"
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)


#OPERATOR CUSTOM LIMIT
OPERATOR_LIMITS = {
    #182: (20, "EUR"), #operator 192, limit 50 EUR
    #218: (79, "EUR")
    # 999: (7500, "USD"),  #example: operator 999, limit 7 500 USD
}


#DEFAULT CURRENCY LIMIT
CURRENCY_LIMITS = {
    566: (90000,   "NGN"),
    840: (1000,    "USD"),
    985: (8000,    "PLN"),
    152: (3100,    "CLP"),
    188: (3000,    "CRC"),
    320: (3000,    "GTQ"),
    978: (2000,    "EUR"),
    980: (30000,   "UAH"),
    203: (30000,   "CZK"),
    946: (20000,   "RON"),
    348: (1500000, "HUF"),
    352: (3000,    "ISK")
}

def check_money_status() -> None:
    connection = pymysql.connect(
        host="db",
        user="user",
        password="heslo",
        db="fare",
        port=3306,
    )
    cursor = connection.cursor(pymysql.cursors.DictCursor)

    cursor.execute(
        "SELECT oper_id, code, currency FROM operator WHERE entity_type = 'operator'"
    )
    operators =  cursor.fetchall()

    exit_status = 0
    business_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    for operator in operators:
        oper_id = operator["oper_id"]
        oper_code = operator["code"]
        currency_no = int(operator["currency"])
        if oper_id in OPERATOR_LIMITS:
            limit_value, currency_alpha = OPERATOR_LIMITS[oper_id]
        elif currency_no in CURRENCY_LIMITS:
          limit_value, currency_alpha = CURRENCY_LIMITS[currency_no]
        else:
          logging.warning(f"SKIP: Operator {oper_code} has no configured limit (currency {currency_no})")
          continue

        cursor.execute(
            """
            SELECT cld_id
            FROM clearing_day
            WHERE oper_id = %s
              AND business_date = %s
              AND cld_id LIKE %s
            """,
            (oper_id, business_date, "%PAYMENT%"),
        )
        clr_day = cursor.fetchone()
        if not clr_day:
            logging.debug(f"OK: Operator {oper_code} has no payment for BusinessDate: {business_date}")
            continue

        cld_id = clr_day["cld_id"]
        cursor.execute(
            """
            SELECT paywindow_id, amount_clear
            FROM clearing_item
            WHERE clearing_day_id = %s
              AND business_date   = %s
              AND amount_clear    >= %s
            """,
            (cld_id, business_date, limit_value),
        )
        critical_items = cursor.fetchall()
        if critical_items:
            pwd_count = len(critical_items)

            if pwd_count > 5:
            #ids = ", ".join(str(item["paywindow_id"]) for item in critical_items)
               logging.critical(
                   f"Operator {oper_code} "
                   f"(currency: {currency_alpha}) has {pwd_count} windows | exceed limit {limit_value}"
               )
               exit_status = max(exit_status, 2)

            else:
               logging.warning(
                  f"Operator {oper_code} "
                  f"(currency: {currency_alpha}) has {pwd_count} windows | near/exceeding limit {limit_value}"
               )
               exit_status = max(exit_status, 1)

        else:
            logging.info(f"OK: Operator {oper_code} has no window above limit {limit_value} {currency_alpha}")

    cursor.close()
    connection.close()
    sys.exit(exit_status)

if __name__ == "__main__":
    check_money_status()