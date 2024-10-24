#!/usr/bin/python3

import sqlalchemy as db
import logging
import sys
from datetime import date, timedelta, datetime, time
from configparser import ConfigParser
import os.path
from pathlib import Path
import urllib
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
#logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

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
        logger.info(f"--------{config_filepath} file found at ")
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


class Connect:
    def __init__(self, connstring):
        self.engine = db.create_engine(connstring)
        self.connection = self.engine.connect()
        self.metadata = db.MetaData()


class DbOperation(Connect):
    def exec_query(self, table, query):
        tableinfo = db.Table(table, self.metadata, autoload=True, autoload_with=self.engine)
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
        self.operid = int(oper_config["operid"])

    def get_data(self):
        if (self.isenabled is True) and (self.from_hour < datetime.now() < self.to_hour):
            query = f"select clearing_stage from fare.clearing_day where oper_id = {self.operid} and business_date = '{self.business_day}' and sequence is not null"
            query_res = DbOperation(sqlengine).exec_query('clearing_day', query)
            # ResultProxy = connection.execute(query)
            ResultSet = query_res.fetchone()
            stage = ResultSet[0] if ResultSet is not None else None
            if (stage == 'Closed') or (stage == 'InProcess'):
                self.clearing_ok = True
                logger.info(f"Operator {self.opercode} ma clearing ve stavu {stage}, za BD {self.business_day}...")
            elif (stage == 'ToSend'):
                if self.opercode == 'PANAMA':
                    Path(f'{notify_folder}CREATE_JIRA_FOR_PANAMA2_MISSING_OK_RESPONSE_{self.business_day}').touch(mode=0o644, exist_ok=True)
                    logger.critical(f"Operator {self.opercode} ma clearing ve stavu  {stage}, za BD {self.business_day}...")
                else:
                    logger.warn(f"Operator {self.opercode} ma clearing ve stavu {stage}, za BD {self.business_day}...")
            elif stage is None:
                logger.critical(f"Operator {self.opercode} nema vubec zpracovany clearing za BD {self.business_day}...")
                Path(f'{notify_folder}{self.opercode}_clearing_missing').touch(mode=0o644, exist_ok=True)
            else:
                self.clearing_ok = False
                logger.critical(f"Operator {self.opercode} ma clearing v nevalidním stavu {stage}, za BD {self.business_day}...")
                Path(f'{notify_folder}{self.opercode}_clearing_in_state_{stage}').touch(mode=0o644, exist_ok=True)
            query_pay = f"select clearing_stage from fare.clearing_day where oper_id = {self.operid} and business_date = '{self.business_day}' and sequence is null"
            query_res_pay = DbOperation(sqlengine).exec_query('clearing_day', query_pay)
            # ResultProxy = connection.execute(query)
            ResultSet = query_res_pay.fetchone()
            stage_pay = ResultSet[0] if ResultSet is not None else None
            if (stage_pay == 'Closed') or (stage_pay == 'InProcess'):
                self.clearing_ok = True
                logger.info(f"Operator {self.opercode} ma payment ve stavu {stage_pay}, za BD {self.business_day}...")
            elif (stage_pay == 'ToSend'):
                logger.warn(f"Operator {self.opercode} ma payment ve stavu {stage_pay}, za BD {self.business_day}...")
            elif stage_pay is None:
                logger.critical(f"Operator {self.opercode} nema vubec zpracovany payment za BD {self.business_day}...")
                Path(f'{notify_folder}{self.opercode}_payment_missing').touch(mode=0o644, exist_ok=True)
            else:
                self.clearing_ok = False
                logger.critical(f"Operator {self.opercode} ma payment v nevalidním stavu {stage_pay}, za BD {self.business_day}...")
                Path(f'{notify_folder}{self.opercode}_payment_in_state_{stage_pay}').touch(mode=0o644, exist_ok=True)
        else:
            logger.info(f"Operator {self.opercode} není třeba monitorovat...")


operators = operator_cfg.sections()
for o in operators:
    operator_check = Operator(o)
    operator_check.get_config()
    operator_check.get_data(