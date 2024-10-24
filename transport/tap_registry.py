#!/usr/bin/python3.9

import sqlalchemy as db
import logging
import sys
from datetime import date, timedelta, datetime, time
from configparser import ConfigParser
import os.path
from pathlib import Path
import urllib.parse

""" Konfigurace logovani """
logging.basicConfig(format='%(asctime)s - %(levelname)s -  %(message)s', level=logging.DEBUG)

logger = logging.getLogger('basic_log')
formatter_base = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
#logfile = "nagios_clearing.log"
logfile = "/tmp/nagios_tapregistry.log"
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


""" Load externi konfigurace """

def load_config(filename):
    #dir_path = os.path.dirname(os.path.realpath(__file__))
    dir_path="/opt/fare/scripts"
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
notify_folder = "/var/log/nagios-alarm-error"
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
        tableinfo = db.Table(table, self.metadata, mysql_autoload=True, autoload_with=self.engine)
        o = self.engine
        c = self.connection
        a = tableinfo
        q = db.text(query)
        resultProxy = c.execute(q)
        return resultProxy


class Operator:
    def __init__(self, opercode):
        self.check_interval = None
        self.timeframe = None
        self.tapcount = None
        self.operid = None
        self.opertype = None
        self.opercode = opercode
        self.isenabled = False
        self.mintaps = None

    def get_config(self):
        oper_config = operator_cfg[f"{self.opercode}"]
        self.isenabled = True if oper_config["enabled_checktapregistry"] == 'True' else False
        self.check_interval = today - timedelta(minutes=int(oper_config["interval_checktapregistry"]))
        self.timeframe = int(oper_config["interval_checktapregistry"])
        self.mintaps = int(oper_config["mintaps_checktapregistry"])
        self.opercode = [self.opercode]
        if "operid" in oper_config:
            self.operid = []
            self.operid.append(int(oper_config["operid"]))
        else:
            self.opertype = int(oper_config["opertype"])

    def amco_clients(self):
        if self.opertype is not None:
            query_type = f"SELECT oper_id,code FROM operator WHERE type = '{self.opertype}'"
            query_clients = DbOperation(sqlengine).exec_query('operator', query_type)
            clients = query_clients.fetchall()
            operator_ids = []
            operator_codes = []
            for row in clients:
                operator_id = row[0]
                operator_code = row[1]
                operator_ids.append(operator_id)
                operator_codes.append(operator_code)
            self.operid = operator_ids
            self.opercode = operator_codes
            return operator_ids, operator_codes

    def get_data(self):
        if self.isenabled is True:
            tap_count = []
            for value in self.operid:
                query = f"select count(*) from fare.tap_temp where oper_id = {value}  and server_dttm <'{self.check_interval}'"
                query_res = DbOperation(sqlengine).exec_query('tap', query)
                ResultSet = query_res.fetchall()
                for row in ResultSet:
                    count_num = row[0]
                    tap_count.append(count_num)
            self.tapcount = tap_count
            return tap_count
        else:
            for code in self.opercode:
                logger.info(f"Operatory {code} není třeba monitorovat...")

    def evaluate(self):
        result_code = []
        if self.tapcount is None:
            #logger.info(f"Operator {self.opercode} není monitorován není třeba notifikovat...")
            pass
        else:
            for code, count in zip(self.opercode, self.tapcount):
                if count <= self.mintaps:
                    logger.info(f"Operator {code} ma aktuálně OK počet tapů k registraci do BO, {count} tapů...")
                else:
                    #for code, count in zip(self.opercode, self.tapcount):
                    if count >= self.mintaps:
                        result_code.append(code)
                        logger.error(f"Operator {code} má problém s registrací do BO, {count} tapů")
        return result_code

operators = operator_cfg.sections()
to_notify = []
for o in operators:
    operator_check = Operator(o)
    operator_check.get_config()
    operator_check.amco_clients()
    operator_check.get_data()
    res = operator_check.evaluate()
    if res:
        to_notify.append(res)

#Nagios notify
if len(to_notify) > 0:
    nglogger.critical(f'Nedari se registrovat tapy pro operatory s kodem {to_notify}')
    logger.critical(f'Nedari se registrovat tapy pro operatory s kodem {to_notify}')
    fileHandler.close()
    sys.exit(2)
elif (len(to_notify) == 0) or (len(to_notify) is None):
    nglogger.info('Tapy se registruji korektne')
    logger.info('Tapy se registruji korektne')
    fileHandler.close()
    sys.exit(0)
else:
    nglogger.error('Neznama chyba checku')
    logger.error('Neznama chyba checku')
    fileHandler.close()
    sys.exit(3)
