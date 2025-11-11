#!/usr/bin/python3
  
import logging
import json
import subprocess
import requests
import sys, os

""" Logovani """
logging.basicConfig(format='%(asctime)s - %(levelname)s -  %(message)s', level=logging.DEBUG)


user = subprocess.getoutput("whoami")

slack_webhook_url = 'https://hooks.slack.com/services/webhookzde'

payload = {
    "channel": "#transport_acc_check",
    "username": "Ultra Fare Updater",
    "text": f"Neplecha se nekoná! Tohle mohl zpackat jedině  uživatel: --> `{user}` <--",
    "icon_emoji": ":monkey:"
}


response = requests.post(slack_webhook_url, json=payload)
if response.status_code == 200:
    print("Message sent successfully to Slack!")
else:
    print("Failed to send the message to Slack.")
    print("Response status code:", response.status_code)
    print("Response content:", response.text)

os.system(f"sshpass -p monet+ ssh root@aptrans01-spc '/opt/fare/script/core_acc_updater.py'")