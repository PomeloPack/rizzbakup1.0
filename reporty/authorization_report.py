#!/usr/bin/python3.9

import pymysql
import pandas as pd
import csv
import sys
import datetime
import smtplib
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

output=f'/opt/fare/scripts/bkk_report/files/xlsx_authtx_bkk_report_{reportdate}.xlsx'

sql=f"""SELECT pw.pwid, pw.vs, pw.token, pw.brand_proc, t.masked_pan, at.operation, at.amount, at.resp_code, at.resp_type, at.resp_message, at.timestmp AS timestamp, t.par_reader FROM pay_window pw LEFT JOIN auth_tx at ON pw.pwid = at.pay_window_id left join token t on t.id = pw.token_id WHERE pw.oper_id = 29 AND open_dttm > '2025-02-15 00:00:01' AND pw.stage IN ('DebtFinal', 'DebtManual', 'Debt', 'AuthDeclined') ORDER BY pw.open_dttm, at.timestmp;"""

conn = pymysql.connect(host='pmydbtrans-vip01-spc', port=3306, user='app_fare', passwd='fare@2016', db='fare')

def write_sql_to_file(file_name, sql, with_header=True, con_sscursor=False):
    cur = conn.cursor(pymysql.cursors.SSCursor) if con_sscursor else conn.cursor()
    cur.execute(sql)

    header = [field[0] for field in cur.description] if with_header else None

    if con_sscursor:
        rows = []
        while True:
            row = cur.fetchone()
            if row:
                rows.append(row)
            else:
                break
    else:
        rows = cur.fetchall()

    df = pd.DataFrame(rows, columns=header if with_header else None)
    df.to_excel(file_name, index=False)

    cur.close()
    conn.close()

write_sql_to_file(output, sql, with_header=True, con_sscursor=False)

# Email setup
from_addr = 'no-reply@switchio.com'
to_addr = 'zsuzsanna.timar@bkk.hu'
cc_addr = ['jhruby@monetplus.cz','monika.falusi@bkk.hu','natalia.szebenyi@bkk.hu','abigel.terkan@bkk.hu','Ildiko2.Kope@kh.hu','jbarton@monetplus.cz','gabor.farnadi@kh.hu','eva.kosane.pusztai@kh.hu','sandor2.santa@kh.hu','Noemi.Csetneky@kh.hu','POS_settlement@kh.hu','mholomek@monetplus.cz','ldarebnik@monetplus.cz','dorottya.fonyodi@bkk.hu']
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
body = MIMEText(content, 'html')
msg.attach(body)

filename = f'/opt/fare/scripts/bkk_report/files/xlsx_authtx_bkk_report_{reportdate}.xlsx'
with open(filename, 'rb') as f:
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