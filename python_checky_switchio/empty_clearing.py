#!/usr/bin/python3.9
import pymysql
import sys
import logging
from datetime import datetime, timedelta

# Nastavení loggingu
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',level=logging.INFO)

logfile = '/tmp/nagios_emptyclearing.log'

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.INFO, # for more info change INFO for DEBUG
                    handlers=[
                        logging.FileHandler(logfile),
                        logging.StreamHandler()
                    ]
)

def check_data_status():
    #Pripojeni do DB
    connection = pymysql.connect(
        host='host',
        user='user',
        password='heslo',
        db='fare',
        port=3306
    )
    cursor = connection.cursor(pymysql.cursors.DictCursor)

    #Ziskani operatoru
    cursor.execute("SELECT oper_id, code FROM operator where entity_type = 'operator'")
    operators = cursor.fetchall()

    exit_status = 0  # Defaultní stav OKpro Zabbix

    #Business date - aktualni date - 1 den
    business_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    for operator in operators:
        oper_id = operator['oper_id']

        #Hledani clearing day pro jednotlive opratory
        cursor.execute("""
            select cld_id from clearing_day
            where oper_id = %s  and business_date = %s and cld_id not like %s;
        """, (oper_id, business_date, '%PAYMENT%'))
        clr_day_result = cursor.fetchone()

        #Pokud clearing_day zaznam existuje
        if clr_day_result:
            cld_id = clr_day_result['cld_id']

            #Kontrola zda neni clearing day prazdny
            cursor.execute("""
                SELECT count(cldi_id) as clearing_count
                FROM clearing_item
                WHERE clearing_day_id = %s and business_date = %s
            """, (cld_id,business_date))
            clearing_count = cursor.fetchone()
            clr_c = clearing_count['clearing_count']
            #Pokud nejsou polozky v clearing_items
            if clr_c == 0:
                #Pocitani tapu pro operatora za predchozi den, dle term_dttm
                cursor.execute("""
                    SELECT COUNT(tapid) as data_count
                    FROM tap
                    WHERE oper_id = %s AND DATE(term_dttm) = CURDATE() - INTERVAL 1 DAY
                    AND term_spdh_code != '-32'
                """, (oper_id,))
                data_result = cursor.fetchone()
                data_count = data_result['data_count']

                if data_count > 0:
                    # Pokud jsou tapy za poslední den, ale clearing_items chybi
                    logging.critical(f"Critical: Operator {operator['code']} ma [{data_count}] tapu, ale zadne clearing_items za business_date {business_date}!")
                    exit_status = max(exit_status, 2)  # Exit status 2 pro critical
                else:
                    # Pokud operator nema ani tapy ani clearing_items za posledni den.
                    logging.warning(f"OK: Operator {operator['code']} nema tapy k prouctovani za business_date {business_date}")
            else:
                # Pokud existuji clearing_items pro clearing_day
                logging.info(f"OK: Operator {operator['code']} ma [{clr_c}] clearing_items za business_date {business_date}")
        else:
            # Pokud operator nema vygenerovany clearing_day
            logging.debug(f"OK: Operator {operator['code']} nema vygenerovany clearing_day za business_date {business_date}")

    cursor.close()
    connection.close()

    sys.exit(exit_status)

if __name__ == "__main__":
    check_data_status()