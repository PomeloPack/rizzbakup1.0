#!/usr/bin/python3
import pymysql
import asyncio
import time
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
six_months_ago = now - timedelta(days=6 * 30)
three_months_ago = now - timedelta(days=3 * 30)
oper_id = 5

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

async def process_token(token_data):
    token_value = token_data['token']
    newest_token_dttm = token_data['newest_token']

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

    return activities['count(tapid)'] > 0

async def process_batch(batch):
    results = await asyncio.gather(*(process_token(token_data) for token_data in batch))
    return sum(results)

def main():
    start_time = time.time()

    active_with_activity = 0
    active_tokens_count = len(active_tokens)

    if active_tokens_count == 0:
        print("Zadne tokeny na DL")
        return

    progress_interval = active_tokens_count // 10
    completed_tasks = 0

    batch_size = 10
    batches = [active_tokens[i:i + batch_size] for i in range(0, active_tokens_count, batch_size)]

    loop = asyncio.get_event_loop()
    print("ZACINAM KONTROLU TAPU")
    for batch in batches:
        active_with_activity += loop.run_until_complete(process_batch(batch))

        completed_tasks += len(batch)

        if progress_interval > 0 and completed_tasks >= progress_interval * (completed_tasks // progress_interval):
            actual_time = time.time() - start_time
            minutes = actual_time / 60
            print(f"Zpracovano: {int((completed_tasks / active_tokens_count) * 100)} % -- cas: {minutes:.2f}")

    percent_with_activity = (active_with_activity / active_tokens_count) * 100

    print(f"Pocet aktivnich tokenu na DL: {active_tokens_count}")
    print(f"Pocet aktivnich tokenu s aktivitou v tapu za posledni 3 mesice: {active_with_activity} ({percent_with_activity:.2f}%)")

    elapsed_time = time.time() - start_time
    print(f"Program trval: {elapsed_time:.2f} sekund")

if __name__ == '__main__':
    main()

connection.close()