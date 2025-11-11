#!/usr/bin/python3.9
  
import logging
import re
import subprocess
import json
import sys, os
import requests

""" Logovani """
logging.basicConfig(format='%(asctime)s - %(levelname)s -  %(message)s', level=logging.DEBUG)


recent_file = subprocess.getoutput(f"/opt/fare/scripts/slack_stats.py")
#print(recent_file)
lines = recent_file.split('\n')
lines = lines[2:]
modified_log = '\n'.join(lines)


def format_line(line):
    parts = line.split(':', 1)
    if len(parts) == 2:
        return f"*{parts[0].strip()}:* `{parts[1].strip()}`"
    else:
        return line.strip()

formatted_log = '\n'.join(format_line(line) for line in modified_log.split('\n'))

proxy = {
    'https': 'http://10.5.20.110:3128'
}

slack_webhook_url = 'https://hooks.slack.com/services/webhoodzde'

payload = {
    "channel": "#transport_team",
    "username": "Daily statistic",
    "text": f"{formatted_log}",
    "icon_emoji": ":chart_with_upwards_trend:"
}


response = requests.post(slack_webhook_url, json=payload, proxies=proxy)
if response.status_code == 200:
    print("Message sent successfully to Slack!")
else:
    print("Failed to send the message to Slack.")
    print("Response status code:", response.status_code)
    print("Response content:", response.text)