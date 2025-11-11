#!/usr/bin/python3.9
import pymysql
import sys
import logging
from datetime import datetime, timedelta


connection = pymysql.connect(
    host='aa',
    user='aa',
    password='aa',
    db='fare',
    port=3306,
    cursorclass=pymysql.cursors.DictCursor
)

cursor = connection.cursor()

now = datetime.now()
six_months_ago = now - timedelta(days=6*30)
three_months_ago = now - timedelta(days=3*30)
oper_id = 4

query_active = """
WITH Atokens AS (
    SELECT token, MAX(dttm) AS newest_token
    FROM stoplist_inc
    WHERE oper_id = %s
    AND dttm < %s
    AND type = 1
    GROUP BY token
)
SELECT token, Atokens.newest_token
FROM Atokens
WHERE NOT EXISTS (
    SELECT 1
    FROM stoplist_inc
    WHERE oper_id = %s
    AND token = Atokens.token
    AND type = 2
    AND dttm > Atokens.newest_token
);
"""

cursor.execute(query_active, (oper_id, six_months_ago, oper_id))
active_tokens = cursor.fetchall()

active_with_activity = 0
for token_data in active_tokens:
    token_value = token_data['token']
    newest_token_dttm = token_data['newest_token']
    #print(token_value)
    #print(newest_token_dttm)

    activity_query = """
        SELECT count(tapid)
        FROM tap
        WHERE token = %s
          AND server_dttm > %s
          AND oper_id = %s
          AND server_dttm >= %s
    """
    cursor.execute(activity_query, (token_value, newest_token_dttm, oper_id, three_months_ago))
    activities = cursor.fetchone()
    #print(activities['count(tapid)'])

    if activities['count(tapid)'] > 0:
        active_with_activity += 1

active_tokens_count = len(active_tokens)
percent_with_activity = (active_with_activity / active_tokens_count) * 100 if active_tokens_count > 0 else 0

print(f"Pocet aktivnich tokenu na DL: {active_tokens_count}")
print(f"Pocet aktivnich tokenu s aktivitou v tapu za posledni 3 mesice: {active_with_activity} ({percent_with_activity:.2f}%)")

connection.close()