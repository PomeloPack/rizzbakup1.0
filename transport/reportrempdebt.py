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

logging.info(f"Starting BKK daily debt report for day: " + reportdate)

output=f'/opt/fare/scripts/bkk_report/files/csv_export_bkk_debt_{reportdate}.csv'

sql=f"""WITH tst AS (SELECT a.oper_id, a.token, a.brand_proc, a.amount_to_settle, a.currency, a.stage, a.open_dttm, a.open_tap ,a.last_tap, a.vs FROM pay_window a WHERE oper_id = '29' AND open_dttm > '2023-06-20 01:56:18' AND stage IN ('authDeclined', 'debtFinal', 'debt', 'debtSettled', 'noAuthDone') AND amount_to_settle > 0) SELECT a.token, a.brand_proc AS card_brand, CAST(a.amount_to_settle / 100 AS INTEGER) AS amount, a.currency, a.stage, CAST(td.term_dttm AS DATE) AS 'SERV_DAT', a.vs AS 'PRN', t.masked_pan, t.par_reader AS 'PAR', CASE WHEN EXISTS (SELECT 1 FROM pay_window WHERE oper_id = a.oper_id AND token = a.token AND stage = 'closed' AND amount_settled > 0) THEN 'YES' ELSE 'NO' END AS SUCCESFULL_CLEARING_IN_PAST, CASE WHEN a.open_tap = a.last_tap THEN 'ONLY ONE TAP FOR PWID' ELSE 'MULTIPLE TAPS FOR PWID' END AS TAP_PWID FROM tst a LEFT JOIN token t ON a.token = t.token LEFT JOIN tap td ON a.open_tap = td.tapid ORDER BY a.open_dttm DESC;"""

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

#SUM PROCESS

df = pd.read_csv(f'/opt/fare/scripts/bkk_report/files/csv_export_bkk_debt_{reportdate}.csv', sep = ',')
Total = sum(df['amount'])
df.loc[0, 'Total amount in debt'] = str(Total)
df.to_csv(f'/opt/fare/scripts/bkk_report/files/csv_export_bkk_debt_{reportdate}.csv', sep = ',',index=False)

#email setup
from_addr = 'no-reply@switchio.com'
#to_addr = 'msmejkal@monetplus.cz'
to_addr = 'zsuzsanna.timar@bkk.hu'
cc_addr = ['jhruby@monetplus.cz','monika.falusi@bkk.hu','natalia.szebenyi@bkk.hu','abigel.terkan@bkk.hu','mholomek@monetplus.cz','Noemi.Csetneky@kh.hu','POS_settlement@kh.hu','ldarebnik@monetplus.cz']
subject = f'Production debt report for the BKK operator {reportdate}'
content = f'''<html>
<head></head>
<body>
<p>Hello,</p>
<p>Attached you can find the report with the list of tokens on the Denylist and the total amount of money in debt as of {reportdate}.</p>
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

filename = f'/opt/fare/scripts/bkk_report/files/csv_export_bkk_debt_{reportdate}.csv'
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
#print("BKK daily debt report successfully sent for day: " + reportdate)
logging.info(f"BKK daily debt report successfully sent for day: " + reportdate)
server.quit()
