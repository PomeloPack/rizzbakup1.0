#!/usr/bin/python3.9
import subprocess
import pymysql
import sys

host = 'pmydbtrans-vip01-spc'
user = 'app_fare'
password = 'fare@2016'
database = 'fare'
connection = pymysql.connect(
    host=host,
    user=user,
    password=password,
    database=database
)

cursor = connection.cursor()

oper_id = input("Please enter oper_id: ")
place = f"SELECT code from operator where oper_id = {oper_id};"
cursor.execute(place)
code1 = cursor.fetchone()
code = code1[0]

cursor.close()
connection.close()

ques1 = input(f"Do you want to continue with operator: {code} ? (yes/no): ")

if ques1 in ['yes', 'y']:
    pass
else:
    print("Exiting...")
    sys.exit()


ques2 = input(f"Are data in tokenstoremove.txt correct? after this confirmation tokens will be removed!: (yes/no): ")

if ques2 in ['yes', 'y']:
    pass
else:
    print("Exiting...")
    sys.exit()

token_file = "/opt/fare/scripts/remove_token/tokenstoremove.txt"
try:
    with open(token_file, 'r') as file:
        data_list = [line.strip() for line in file]

    for line in data_list:
        curl_command = f"curl -u mgmt:mgmt -X GET http://localhost:4151/api/management/blacklist/remove/{code}/{line}"
        subprocess.run(curl_command, shell=True)
        print(f"  N.:  " + line)

except FileNotFoundError:
    print(f"File '{token_file}' not found.")