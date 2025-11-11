#!/usr/bin/python3
  
import logging
import json
import sys, os
import subprocess

logging.basicConfig(format='%(asctime)s - %(levelname)s -  %(message)s', level=logging.DEBUG)

version = input("Zadejte verzi fare ke stazeni: ")
logging.info("Zastavuji aplikaci fare-worker")
os.system(f"crm resource stop fare-worker")

options = ['snapshot', 'release']

user_input = ''

input_message = "Pick an option / (RC = snapshot):\n"
for index, item in enumerate(options):
    input_message += f'{index+1}) {item}\n'

input_message += 'Your choice: '

while user_input not in map(str, range(1, len(options) + 1)):
    user_input = input(input_message)

print('You picked: ' + options[int(user_input) - 1])
result = options[int(user_input) - 1]

if result in ['snapshot']:
    core = f"https://nexus3.monetplus.cz/repository/tsg-snapshots/cz/monetplus/fare/fare-worker/{version}/fare-worker-{version}.rpm"

if result in ['release']:
    core = f"https://nexus3.monetplus.cz/repository/tsg-releases/cz/monetplus/fare/fare-worker/{version}/fare-worker-{version}.rpm"

logging.info("Stahuju instalacni balik")
os.system(f"wget -P /home/Packages {core} > /dev/null 2>&1")

multiple_nodes = input("Chces updatovat i 2 a 3 node? y/n: ")
if multiple_nodes in ['yes', 'y']:
    logging.info("Kopiruji instalacni baliky na 2 a 3 node")
    os.system(f"cp /home/Packages/fare-worker-{version}.rpm /home/Packages/fare-worker-updater.rpm")
    os.system("scp /home/Packages/fare-worker-updater.rpm  aptrans02-spc:/home/Packages/")
    os.system("scp /home/Packages/fare-worker-updater.rpm  aptrans03-spc:/home/Packages/")
    logging.info("Spoustim update na 2 a 3 nodu")
    node2 = subprocess.getoutput("ssh aptrans02-spc '/opt/fare/script/fare_update.py'")
    node3 = subprocess.getoutput("ssh aptrans03-spc '/opt/fare/script/fare_update.py'")
else:
    logging.info("Updatuji pouze node1")
    pass

logging.info("Updatuji fare-worker node 1")
os.system(f'yum -y update /home/Packages/fare-worker-{version}.rpm')

logging.info("Zapinam fare-worker")
os.system('crm resource cleanup fare-worker')
os.system('crm resource start fare-worker')

os.system("rm -f /home/Packages/fare-worker-updater.rpm")
delfile = input("Chces vymazat instalacni balik? y/n: ")

if delfile in ['yes', 'y']:
    os.system(f"rm -f /home/Packages/fare-worker-{version}.rpm")
    logging.info("Smazano")
    logging.info("Instalacni baliky na 2 a 3 node automaticky smazany")
else:
    logging.info("Instalacni balik ponechan na node 1")
    logging.info("Instalacni baliky na 2 a 3 node automaticky smazany")


logging.info("Kontrola nainstalovanych verzi")
node1 = subprocess.getoutput("rpm -qa | grep fare-worker")
logging.warning(f"NODE1: {node1}")
logging.warning(f"NODE2: {node2}")
logging.warning(f"NODE3: {node3}")