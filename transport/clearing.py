#!/usr/bin/python3.9

import sqlalchemy as db
import logging
import sys
from datetime import date, timedelta, datetime, time
from configparser import ConfigParser
import os.path
from pathlib import Path
import urllib.parse
import subprocess


""" Konfigurace logovani """
logging.basicConfig(format='%(asctime)s - %(levelname)s -  %(message)s', level=logging.DEBUG)

logger = logging.getLogger('basic_log')
formatter_base = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
#logfile = "nagios_clearing.log"
logfile = "/tmp/nagios_clearing.log"
fileHandler = logging.FileHandler(logfile, mode='a')
fileHandler.setFormatter(formatter_base)

logger.setLevel(logging.INFO)
logger.addHandler(fileHandler)

""" Konfigurace logovani pro Nagios """
nglogger = logging.getLogger('nagios_logger')

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
nglogger.addHandler(handler)

""" Kontrola zda pustit skript """

virtual_ip= '10.5.0.197'
getip = "ip a s | grep '" + virtual_ip + "' | awk '{print $2}' | cut -f1  -d'/' | tr -d '\n'"

class Result:
    pass

def checkvip(command, vipaddr):
    cmd = subprocess.Popen(['/bin/bash', '-c', command],stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    result = cmd.communicate()[0]
    name_str = result.decode()
   # subprocess.getoutput
   # print("POZADOVANA  IP: ",vipaddr)
   # print("NALZENA IP: ",result)
   # print("VYSLEDNA IP: ",name_str)
    if name_str == vipaddr:
        pass
        logging.info('Virtualni adresa je IP adresa tohoto serveru, skript je mozne spustit')
    else:
        logging.warning('Tento server nema aktualne pozadvanou IP adresu, ukoncuji skript')
        sys.exit()

checkvip (getip, virtual_ip)

""" Load externi konfigurace """

def load_config(filename):
    dir_path = "/opt/fare/scripts"
    config_filepath = f'{dir_path}/{filename}'
    # check if the config file exists
    exists = os.path.exists(config_filepath)
    config = None
    if exists:
        #logger.info(f"--------{config_filepath} file found at ")
        config = ConfigParser()
        config.read(config_filepath)
    else:
        logger.info(f"--------{config_filepath} file not found at ")
    return config


db_cfg = load_config('db.ini')
operator_cfg = load_config('operator.ini')
db_config = db_cfg["DATABASE"]

""" Konfigurace """
# operator ID Monitorovanych operatoru
today = datetime.now()
notify_folder = "/var/log/nagios-alarm-error/"
# sqlengine = 'mysql+pymysql://mon_nagios:nagmon%402019@pmydbtrans-vip01-spc:3306/fare?charset=utf8'
#sqlengine = 'mysql+pymysql://admindb:monet+@localhost:37375/fare?charset=utf8'
sqlengine = f'mysql+pymysql://{db_config["username"]}:{urllib.parse.quote(db_config["password"])}@{db_config["host"]}:{db_config["port"]}/' \
            f'{db_config["database_name"]}?charset=utf8'

logging.info("Starting check for TRANSPORT clearings and payments")
engine = db.create_engine(sqlengine)
connection = engine.connect()
metadata = db.MetaData()
#global active_clearing_has_run
active_clearing_has_run = False
#global active
#active = active_clearing_has_run

class Connect:
    def __init__(self, connstring):
        self.engine = db.create_engine(connstring)
        self.connection = self.engine.connect()
        self.metadata = db.MetaData()


class DbOperation(Connect):
    def exec_query(self, table, query):
        tableinfo = db.Table(table, self.metadata, mysql_autoload=True, autoload_with=self.engine)
        o = self.engine
        c = self.connection
        a = tableinfo
        q = db.text(query)
        resultProxy = c.execute(q)
        return resultProxy


class Operator:
    def __init__(self, opercode):
        self.timeframe = None
        self.from_hour = datetime.now()
        self.to_hour = datetime.now()
        self.operid = None
        self.opertype = None
        self.opercode = opercode
        self.isenabled = False
        self.payment_ok = False
        self.clearing_ok = False
        self.business_day = date.today() - timedelta(days=1)

    def get_config(self):
        oper_config = operator_cfg[f"{self.opercode}"]
        self.isenabled = True if oper_config["enabled_checkclearing"] == 'True' else False
        self.from_hour = today.replace(hour=int(oper_config["checkclearing_from_hour"]), minute=0, second=0)
        self.to_hour = today.replace(hour=int(oper_config["checkclearing_to_hour"]), minute=0, second=0)
        if "operid" in oper_config:
            self.operid = int(oper_config["operid"])
        else:
            self.opertype = int(oper_config["opertype"])
            #logger.warn("LOADING AMCO CLIENTS...")

    def get_amcoclients(self):
        nagios1 = []
        nagios2 = []
        tosend1 = []
        tosend2 = []
#        inproc1 = []
#        inproc2 = []
        if (self.isenabled is True) and self.opertype is not None and (self.from_hour < datetime.now() < self.to_hour):
            query_type = f"SELECT oper_id,code FROM operator WHERE type = '{self.opertype}'"
            query_clients = DbOperation(sqlengine).exec_query('operator', query_type)
            clients = query_clients.fetchall()
            nagios1 = []
            nagios2 = []
            tosend1 = []
            tosend2 = []
#            inproc1 = []
#            inproc2 = []
            for row in clients:
                operator_id = row[0]
                operator_code = row[1]
                query_type2 = f"select clearing_stage from fare.clearing_day where oper_id = {operator_id} and business_date = '{self.business_day}' and cld_id not like '%PAYMENT%'"
                clients2 = DbOperation(sqlengine).exec_query('clearing_day', query_type2)
                owy = clients2.fetchall()
                try:
                   stage = owy[0][0]
                except IndexError:
                   stage = None
                if (stage == 'Open'):
                    #logger.warn("Opetrator {operator_code} ma clearing ve stavu {stage}!")
                    #inproc1.append(operator_code)
                    self.clearing_ok = True
                elif (stage == 'ToSend'):
                    tosend1.append(operator_code)
                    #logger.info(f"Operator {operator_code} ma clearing ve stavu {stage}, za BD {self.business_day}...")
                elif stage is None:
                    #logger.critical(f"Operator {operator_code} nema vubec zpracovany clearing za BD {self.business_day}...")
                    #Path(f'{notify_folder}{operator_code}_clearing_missing').touch(mode=0o644, exist_ok=True)
                    nagios1.append(operator_code)
                else:
                    self.clearing_ok = False
                    #logger.critical(f"Operator {operator_code} ma clearing v nevalidním stavu {stage}, za BD {self.business_day}...")
                    #Path(f'{notify_folder}{operator_code}_clearing_in_state_{stage}').touch(mode=0o644, exist_ok=True)
                query_pay = f"select clearing_stage from fare.clearing_day where oper_id = {operator_id} and business_date = '{self.business_day}' and cld_id like '%PAYMENT%'"
                query_res_pay = DbOperation(sqlengine).exec_query('clearing_day', query_pay)
                ResultSet = query_res_pay.fetchone()
                stage_pay = ResultSet[0] if ResultSet is not None else None
                if (stage_pay == 'InProcess'):
                    #inproc2.append(operator_code)
                    self.clearing_ok = True
                    #logger.info(f"Operator {operator_code} ma payment ve stavu {stage_pay}, za BD {self.business_day}...")
                elif (stage_pay == 'ToSend'):
                    tosend2.append(operator_code)
                    #logger.warn(f"Operator {operator_code} ma payment ve stavu {stage_pay}, za BD {self.business_day}...")
                elif stage_pay is None:
                    #logger.critical(f"Operator {operator_code} nema vubec zpracovany payment za BD {self.business_day}...")
                    #Path(f'{notify_folder}{operator_code}_payment_missing').touch(mode=0o644, exist_ok=True)
                    #if operator_code in nagios:
                    #   continue
                    nagios2.append(operator_code)
                else:
                    self.clearing_ok = False
                    #logger.critical(f"Operator {operator_code} ma payment v nevalidním stavu {stage_pay}, za BD {self.business_day}...")
                    #Path(f'{notify_folder}{operator_code}_payment_in_state_{stage_pay}').touch(mode=0o644, exist_ok=True)
            return nagios1, tosend1, nagios2, tosend2
        elif (self.opertype is not None) and (datetime.now() > self.to_hour or datetime.now() < self.from_hour):
            return nagios1, tosend1, nagios2, tosend2

    def get_data(self):
        clearing = []
        payment = []
        tosend1 = []
        tosend2 = []
        failed = []
#        inproc1 = []
#        inproc2 = []
        if (self.isenabled is True) and self.operid is not None and (self.from_hour < datetime.now() < self.to_hour):
            query = f"select clearing_stage from fare.clearing_day where oper_id = {self.operid} and business_date = '{self.business_day}' and cld_id not like '%PAYMENT%'"
            query_res = DbOperation(sqlengine).exec_query('clearing_day', query)
            ResultSet = query_res.fetchone()
            stage = ResultSet[0] if ResultSet is not None else None
            if  (stage == 'Open'):
                self.clearing_ok = True
                #inproc1.append(self.opercode)
                #logger.warn("Opetrator {operator_code} ma clearing ve stavu {stage}!")
            elif (stage == 'ToSend'):
                tosend1.append(self.opercode)
                #if self.opercode == 'PANAMA2':
                    #Path(f'{notify_folder}CREATE_JIRA_FOR_PANAMA2_MISSING_OK_RESPONSE_{self.business_day}').touch(mode=0o644, exist_ok=True)
                #    nglogger.critical(f"CREATE JIRA FOR {self.opercode}!!! MISSING OK RESPONSE!")
                #    print("")
                #else:
                #    pass
                    #logger.warn(f"Operator {self.opercode} ma clearing ve stavu {stage}, za BD {self.business_day}...")
            elif stage is None:
               # logger.critical(f"Operator {self.opercode} nema vubec zpracovany clearing za BD {self.business_day}...")
                #Path(f'{notify_folder}{self.opercode}_clearing_missing').touch(mode=0o644, exist_ok=True)
                clearing.append(self.opercode)
            else:
                self.clearing_ok = False
                #logger.critical(f"Operator {self.opercode} ma clearing v nevalidním stavu {stage}, za BD {self.business_day}...")
                #Path(f'{notify_folder}{self.opercode}_clearing_in_state_{stage}').touch(mode=0o644, exist_ok=True)
            query_pay = f"select clearing_stage from fare.clearing_day where oper_id = {self.operid} and business_date = '{self.business_day}' and cld_id like '%PAYMENT%'"
            query_res_pay = DbOperation(sqlengine).exec_query('clearing_day', query_pay)
            ResultSet = query_res_pay.fetchone()
            stage_pay = ResultSet[0] if ResultSet is not None else None
            if (stage_pay == 'InProcess'):
                #inproc2.append(self.opercode)
                self.clearing_ok = True
                #logger.info(f"Operator {self.opercode} ma payment ve stavu {stage_pay}, za BD {self.business_day}...")
            elif (stage_pay == 'ToSend'):
                tosend2.append(self.opercode)
                if self.opercode == 'BKK':
                    failed.append(self.opercode)
            elif (stage_pay == 'Failed'):
                failed.append(self.opercode)
                #logger.warn(f"Operator {self.opercode} ma payment ve stavu {stage_pay}, za BD {self.business_day}...")
            elif stage_pay is None:
                #logger.critical(f"Operator {self.opercode} nema vubec zpracovany payment za BD {self.business_day}...")
                #Path(f'{notify_folder}{self.opercode}_payment_missing').touch(mode=0o644, exist_ok=True)
                payment.append(self.opercode)
            else:
                self.clearing_ok = False
                #logger.critical(f"Operator {self.opercode} ma payment v nevalidním stavu {stage_pay}, za BD {self.business_day}...")
                #Path(f'{notify_folder}{self.opercode}_payment_in_state_{stage_pay}').touch(mode=0o644, exist_ok=True)
            return clearing, tosend1, payment, tosend2, failed
        elif (self.opertype is not None):
            pass
            #logger.warn(f"{self.opercode} CLIENTS SUCCESSFULLY CHECKED")
        else:
            pass
            #return clearing, tosend1, payment, tosend2, inproc1, inproc2
            #logger.info(f"Operator {self.opercode} není třeba monitorovat...")


    def active_clearing(self):
        global active_clearing_has_run
        if active_clearing_has_run is True:
            return
        active_clearing_has_run = True
        query_type = f"SELECT code FROM operator WHERE oper_id IN (SELECT oper_id from clearing_day where clearing_stage = 'InProcess' and business_date = '{self.business_day}' and cld_id not like '%PAYMENT%')"
        query_clients = DbOperation(sqlengine).exec_query('operator', query_type)
        clients = [row[0] for row in query_clients.fetchall()]
        query_type2 = f"SELECT code FROM operator WHERE oper_id IN (SELECT oper_id from clearing_day where clearing_stage = 'InProcess' and business_date = '{self.business_day}' and cld_id like '%PAYMENT%')"
        query_clients2 = DbOperation(sqlengine).exec_query('operator', query_type2)
        clients2 = [row[0] for row in query_clients2.fetchall()]
        return clients, clients2

operators = operator_cfg.sections()
clearing = []
tosend1 = []
payment = []
tosend2 = []
failed = []
#inproc1 = []
#inproc2 = []
for o in operators:
    operator_check = Operator(o)
    operator_check.get_config()
    amc = operator_check.get_amcoclients()
    if amc is not None:
        amc1, asnd1, amc2, asnd2 = amc
    res = operator_check.get_data()
    if res is not None:
        clr, tsnd1, pay, tsnd2, fail = res
        clearing.extend(clr)
        tosend1.extend(tsnd1)
        payment.extend(pay)
        tosend2.extend(tsnd2)
        failed.extend(fail)
    run = operator_check.active_clearing()
    if run is not None:
        clients, clients2 = run
#        inproc1.extend(inp1)
#        inproc2.extend(inp2)

merged_list = clearing + amc1
# ODSTRANENI OPERATORA Z LISTU
#merged_list = [item for item in merged_list if item not in ['RHODES']]
payment = payment + amc2
# ODSTRANENI OPERATORA Z LISTU
#payment = [item for item in payment if item not in ['RHODES']]
tosendclr = asnd1 + tosend1
tosendpay = asnd2 + tosend2
#clrrun = inproc1 + anproc1
#payrun = inproc2 + anproc2

nagios_result = []

#Nagios notify
if len(merged_list) > 0:
    nagios_result.append(1)
    nglogger.critical(f'Neni zpracovany CLEARING pro operatory s kodem {merged_list}')
    logger.critical(f'Neni zpracovany CLEARING pro operatory s kodem {merged_list}')

if len(payment) > 0:
    nagios_result.append(1)
    nglogger.critical(f'Neni zpracovany PAYMENT pro operatory s kodem {payment}')
    logger.critical(f'Neni zpracovany PAYMENT pro operatory s kodem {payment}')
    print("")

if len(failed) > 0:
    nagios_result.append(1)
    nglogger.critical(f'PAYMENT pro operatory s kodem {failed} je ve stavu FAILED!')
    logger.critical(f'PAYMENT pro operatory s kodem {failed} je ve stavu FAILED!')
    print("")

if (len(merged_list) == 0) or (len(merged_list) is None):
    nglogger.info('Vsechny clearingy zpracovany OK')
    logger.info('Vsechny clearingy zpracovany OK')


if (len(payment) == 0) or (len(payment) is None):
    print("")
    nglogger.info('PAYMENTY pro SWITCHIO TRANSPORT OK')
    logger.info('PAYMENTY pro SWITCHIO TRANSPORT OK')

if len(tosendclr) > 0:
    print("")
    nglogger.warning(f'CLEARING pro operatory s kodem {tosendclr} je ve stavu ToSend')
    fileHandler.close()
elif (len(tosendclr) == 0) or (len(tosendclr) is None):
    print("")
    nglogger.info('VSECHNY CLEARING RESULTY ODESLANY OK')
    logger.info('VSECHNY CLEARING RESULTY ODESLANY OK')

if len(tosendpay) > 0:
    print("")
    nglogger.warning(f'PAYMENT pro operatory s kodem {tosendpay} je ve stavu ToSend')

elif (len(tosendpay) == 0) or (len(tosendpay) is None):
    print("")
    nglogger.info('VSECHNY PAYMENT RESULTY ODESLANY OK')
    logger.info('VSECHNY PAYMENT RESULTY ODESLANY OK')

if len(clients) > 0:
    print("")
    nglogger.info(f'Prave probiha CLEARING pro operatory s kodem {clients}')

if len(clients2) > 0:
    print("")
    nglogger.info(f'Prave probiha PAYMENT pro operatory s kodem {clients2}')

if len(clients) == 0 and len(clients2) == 0:
    print("")
    nglogger.info('Prave neprobiha zadny clearing ani payment!')

#nagios result
if len(nagios_result) >= 1:
    fileHandler.close()
    sys.exit(2)
elif len(nagios_result) == 0 or len(nagios_result) is None:
    fileHandler.close()
    sys.exit(0)
else:
    nglogger.error('Neznama chyba checku pro clearing')
    logger.error('Neznama chyba checku pro clearing')
    fileHandler.close()
    sys.exit(3)