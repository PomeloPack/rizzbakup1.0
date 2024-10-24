#!/usr/bin/python3.9

import sys
import datetime
import smtplib
import subprocess
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

set_date = input("set date YYYY-MM-DD: ")
start = input("Input start-time 00:00:00:  ")
end = input("Input end-time 00:00:00:  ")
name_of_file = input(f"Set a name of the file: ")

original_file = subprocess.getoutput(f"zcat /opt/fare/fare-worker/log/worker-{set_date}* | awk '/^{set_date} {start}*/,/{set_date} {end}*/' >> {name_of_file}")
formated_file = subprocess.getoutput(f"tar -zcvf {name_of_file}.tar.gz {name_of_file}")


#email setup
from_addr = 'support@switchio.com'
to_addr = 'mholomek@monetplus.cz'
#cc_addr = ['jgajdosik@monetplus.cz']
subject = f'Production log from fare-worker'
content = f'''<html>
<head></head>
<body>
<p>Hello,</p>
<p>Attached you can find the log.</p>
<p>Best regards,</p>
<p>Switchio Team</p>
</body>
</html>'''

msg = MIMEMultipart()
msg['From'] = from_addr
msg['To'] = to_addr
#msg['Cc'] = ', '.join(cc_addr)
msg['Subject'] = subject
body = MIMEText (content, 'html')
msg.attach(body)

filename = f'/home/mholomek/{name_of_file}.tar.gz'
with open (filename, 'rb') as f:
    part = MIMEApplication(f.read(), Name=basename(filename))
    part['Content-Disposition'] = 'attachment; filename="{}"'.format(basename(filename))
msg.attach(part)

server = smtplib.SMTP('localhost', 25)
server.send_message(msg)

server.quit()
