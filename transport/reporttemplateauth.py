#!/usr/bin/python3.9

import pymysql
import csv
import sys
import datetime
import smtplib
import subprocess
import pandas as pd
import logging
from datetime import timedelta, datetime
from os.path import basename
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from email.mime.image import MIMEImage


logging.basicConfig(format='%(asctime)s - %(levelname)s -  %(message)s', level=logging.DEBUG)


def handle_exception(exc_type, exc_value, exc_traceback):
    # Log the exception
    logging.error("Uncaught exception occurred:", exc_info=(exc_type, exc_value, exc_traceback))

sys.excepthook = handle_exception

now = datetime.today()
#prevfd = (datetime.today() - timedelta(weeks=4)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
#fd = (datetime.today().replace(day=1, hour=0, minute=0, second=0, microsecond=0))
#reportday = fd.strftime('%Y-%m-%d')
#reportmonth = prevfd.strftime('%m/%Y')
reportdate = now.strftime('%Y-%m-%d')

logging.info(f"Starting BKK daily auth operations for paywindows in debt report for day: " + reportdate)

output=f'/opt/fare/scripts/bkk_report/files/csv_authtx_bkk_report_{reportdate}.csv'

sql=f"""SELECT pw.pwid, pw.vs, pw.token, pw.brand_proc, t.masked_pan, at.operation, at.amount, at.resp_code, at.resp_message, at.timestmp AS timestamp, t.par_reader FROM pay_window pw LEFT JOIN auth_tx at ON pw.pwid = at.pay_window_id left join token t on t.id = pw.token_id WHERE pw.oper_id = 29 AND open_dttm > '2024-09-01 00:00:01' AND pw.stage IN ('DebtFinal', 'DebtManual', 'Debt', 'AuthDeclined') ORDER BY pw.open_dttm, at.timestmp;"""

conn = pymysql.connect(host='pmydbtrans-vip01-spc', port=3306, user='app_fare', passwd='fare@2016', db='fare')

def write_sql_to_file(file_name, sql, with_header=True, delimiter=',',quotechar='"',quoting=csv.QUOTE_NONNUMERIC, con_sscursor=False):
    cur = conn.cursor(pymysql.cursors.SSCursor) if con_sscursor else conn.cursor()
#    cur = conn.cursor(pymysql.cursors.SSCursor)
    cur.execute(sql)
    header = [field[0] for field in cur.description]
    ofile = open(file_name,'w')
    csv_writer = csv.writer(ofile, delimiter=delimiter, quotechar=quotechar,quoting=quoting)
    if with_header:
        csv_writer.writerow(header)
    if con_sscursor:
         while True:
            x = cur.fetchone()
            if x:
                csv_writer.writerow(x)
            else:
                break
    else:
        for x in cur.fetchall():
            csv_writer.writerow(x)
    cur.close()
    ofile.close()
    conn.close()

write_sql_to_file(output, sql, with_header=True, delimiter=',',quotechar='"',quoting=csv.QUOTE_NONNUMERIC, con_sscursor=False)


#email setup
#from_addr = 'notifikace@monetplus.cz'
from_addr = 'no-reply@switchio.com'
#to_addr = 'mholomek@monetplus.cz'
to_addr = 'zsuzsanna.timar@bkk.hu'
cc_addr = ['jhruby@monetplus.cz','monika.falusi@bkk.hu','natalia.szebenyi@bkk.hu','abigel.terkan@bkk.hu','Ildiko2.Kope@kh.hu','jbarton@monetplus.cz','gabor.farnadi@kh.hu','eva.kosane.pusztai@kh.hu','sandor2.santa@kh.hu','Noemi.Csetneky@kh.hu','POS_settlement@kh.hu','mholomek@monetplus.cz']
subject = f'BKK Production auth operations report for debt paywindows {reportdate}'
content = f'''<html>
<head></head>
<body>
<p>Hello,</p>
<p>Attached you can find the report with the list of auth operation for paywindows in debt state as of {reportdate}.</p>
<p>Best regards,</p> 
<p>Switchio Team</p>
<img src="cid:logo">
</body>
</html>'''

msg = MIMEMultipart()
msg['From'] = from_addr
msg['To'] = to_addr
msg['Cc'] = ', '.join(cc_addr)
msg['Subject'] = subject
body = MIMEText (content, 'html')
msg.attach(body)

filename = f'/opt/fare/scripts/bkk_report/files/csv_authtx_bkk_report_{reportdate}.csv'
with open (filename, 'r') as f:
    part = MIMEApplication(f.read(), Name=basename(filename))
    part['Content-Disposition'] = 'attachment; filename="{}"'.format(basename(filename))
msg.attach(part)

logopath = '/opt/fare/scripts/bkk_report/logo.png'
with open(logopath, 'rb') as f:
    logo_data = f.read()
logo = MIMEImage(logo_data)
logo.add_header('Content-ID', '<logo>')
logo.add_header('Content-Disposition', 'inline', filename='logo.png')
msg.attach(logo)

server = smtplib.SMTP('localhost', 25)
server.send_message(msg)

logging.info(f"BKK report for auth operation for debt paywindows successfully generated and sent for day: " + reportdate)
server.quit()
