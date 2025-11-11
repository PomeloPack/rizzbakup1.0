#!/usr/bin/python3.9
import pymysql
import sys
import logging
from tabulate import tabulate

# Nastavení loggingu
logfile = '/tmp/check_temp_fail.log'
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.INFO,
                    handlers=[
                        logging.FileHandler(logfile),
                        logging.StreamHandler()
                    ]
)

def check_auth_tx_status():
    # Pripojeni k databazi
    connection = pymysql.connect(
        host="pmydbtrans-vip01-spc",
        user="mon_nagios",
        password="nagmon@2019",
        db="fare",
        port=3306
    )
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    
    logging.info(f"--------------------- SPOUSTIM CHECK ---------------------")

    # Nacteni operatoru
    cursor.execute("SELECT oper_id, code FROM operator where entity_type = 'operator'")
    operators = cursor.fetchall()

    exit_status = 0  # Defaultni stav OK

    for operator in operators:
        oper_id = operator['oper_id']
        query = """
            SELECT
                resp_code,
                GROUP_CONCAT(DISTINCT LEFT(resp_message, 100)) AS resp_messages,
                GROUP_CONCAT(DISTINCT resp_core) AS resp_cores,
                COUNT(*) AS count
            FROM auth_tx
            WHERE resp_type = 'TEMP_FAIL'
              AND timestmp >= NOW() - INTERVAL 2 HOUR
              AND oper_id = %s
            GROUP BY resp_code;
        """

        cursor.execute(query, (oper_id,))
        results = cursor.fetchall()

        if results:
            logging.info(f"Kontrola pro operatora {operator['code']}:")

            for row in results:
                resp_code = row['resp_code']
                resp_messages = row['resp_messages']
                resp_cores = row['resp_cores']
                result_count = row['count']

                # Vypis vysledku tabulky
                #logging.info(f"Resp_code: {resp_code}, Resp_messages: {resp_messages}, Resp_cores: {resp_cores}, Result_count: {result_count}")

                # Kontrola, zda je count > 10
                if result_count > 10:
                    logging.error(f"Error: Operator {operator['code']} ma [{result_count}] TEMP_FAILU pro resp_code {resp_code}.")
                    # Vypis vysledku jako tabulku
                    table = tabulate(results, headers="keys", tablefmt="grid")
                    logging.info(f"Tabulka vysledku:\n{table}")
                    exit_status = max(exit_status, 2)  # Exit status 2 pro error
        else:
            logging.info(f"OK: Operator {operator['code']} nema TEMP_FAIL zaznamy za posledni 2 hodiny.")

    cursor.close()
    connection.close()

    sys.exit(exit_status)

if __name__ == "__main__":
    check_auth_tx_status()