#!/usr/bin/python3.9

import pymysql
import logging
import subprocess
import sys

""" Konfigurace logovani """
logging.basicConfig(format='%(asctime)s - %(levelname)s -  %(message)s', level=logging.DEBUG)

op_new = input("Zadejte cislo operatora, ktereho chcete nakonfigurovat: ")

def mysqlconnect():
        # To connect MySQL database
        conn = pymysql.connect(host='pmydbtrans-vip01-spc',
                             user='app_fare',
                             password='fare@2016',
                             database='fare')
        cur = conn.cursor()
        # Select query
        get_code = f"SELECT code from operator WHERE oper_id = {op_new};"
        cur.execute(get_code)
        op_code = cur.fetchall()
        op_code = op_code[0]
        global test
        test = str(op_code[0])

        logging.info("NASTAVUJI OPERATORA " + str(op_code[0]))

        answer = input("CHCETE POKRACOVAT? (yes/no): ")
        if answer.lower() == "yes":
            print("Pokracuji v nastaveni operatora...")
        else:
            print("Ukoncuji script...")
            sys.exit()

        cur.execute(f"DELETE from operator_property WHERE operator_id = {op_new};")
        conn.commit()
        logging.info("VYMAZANY PROPERTIES OPERATORA: " +str(op_code[0]))
        print(cur.rowcount, "RECORD(S) AFFECTED")

        cur.execute(f"DELETE from operator_brand_params WHERE operator_id = {op_new};")
        logging.info("VYMAZANY PROPERTIES OPERATOR_BRAND_PARAMS: " +str(op_code[0]))
        print(cur.rowcount, "RECORD(S) AFFECTED")


        cur.execute(f"DELETE from operator_aid_bin WHERE operator_id = {op_new};")
        logging.info("VYMAZANY PROPERTIES OPERATOR_AID_BIN: " +str(op_code[0]))
        print(cur.rowcount, "RECORD(S) AFFECTED")

        op_old = input("ZADEJTE CISLO OPERATORA, OD KTEREHO CHCETE KONFIGURACI OKOPIROVAT: ")
        get_code_old = f"SELECT code from operator WHERE oper_id = {op_old};"
        cur.execute(get_code_old)
        op_code_old = cur.fetchall()
        op_code_old = op_code_old[0]

        logging.info("KOPIRUJI NASTAVENI OPERATORA " + str(op_code_old[0]))
        cur.execute(f"INSERT INTO operator_property (property_key, operator_id, value) SELECT property_key, '{op_new}', value FROM operator_property where operator_id = {op_old};")
        conn.commit()
        print(cur.rowcount, "RECORD(S) AFFECTED")

        logging.info('NASTAVUJI BRAND_PARAMS')
        cur.execute(f"INSERT INTO operator_brand_params (brand, operator_id, engine, open_amount, max_amount, max_days, limit1, limit2, debt_days) SELECT brand, '{op_new}', engine, open_amount, max_amount, max_days, limit1, limit2, debt_days FROM operator_brand_params where operator_id = {op_old};")
        conn.commit()
        print(cur.rowcount, "RECORD(S) AFFECTED")
        logging.info('BRAND_PARAMS NASTAVENO')


        logging.info('NASTAVUJI AID_BIN')
        cur.execute(f"INSERT INTO operator_aid_bin (aid_bin, operator_id, brand) SELECT aid_bin, '{op_new}', brand FROM operator_aid_bin where operator_id = {op_old};")
        conn.commit()
        print(cur.rowcount, "RECORD(S) AFFECTED")
        logging.info('AID_BIN NASTAVENO')

        logging.info('UPRAVUJI OPERATOR_TYPE')
        cur.execute(f"UPDATE operator SET type = (SELECT type FROM operator WHERE oper_id = '{op_old}') WHERE oper_id = '{op_new}';")
        conn.commit()
        logging.info('OPERATOR_TYPE NASTAVEN')

        logging.info('KOPIROVANI OPERATORA DOKONCENO')
        # To close the connection
        conn.close()

def clearcache():
        command = "curl -s -u mgmt:mgmt 'http://localhost:4151/api/management/mgmt/clearcache/operator'"
        result = subprocess.run(command, stdout=subprocess.PIPE, shell=True)
        logging.warning("CACHE APLIKACE PROMAZÁNA - " + result.stdout.decode("utf-8"))


def fcmoperator():
        fcmop = input("CHCETE VYTVORIT FCM OPERATORA? (yes/no): ")
        if fcmop.lower() == "yes":
            logging.info(f"VYTVARIM FCM OPERATORA {test}...")
        else:
            logging.info("Ukoncuji script...")
            sys.exit()
        creation = f"curl -s -k -u mgmt:mgmt -X 'POST' https://fcm.switchio.com:6155/api/management/mgmt/operator/create -H 'accept: text/plain' -H 'Content-Type: application/json' -d '{{\"code\": \"{test}\", \"type\": 0}}'"
        creation = subprocess.run(creation, stdout=subprocess.PIPE, shell=True)
        logging.info(f'FCM OPERATOR VYTVOREN\n{creation.stdout.decode("utf-8")}')
        logging.info('DOKONCENO, VYPINAM SCRIPT')

# Driver Code
if __name__ == "__main__" :
        mysqlconnect()
        clearcache()
        fcmoperator()
