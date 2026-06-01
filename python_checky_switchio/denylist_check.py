#!/usr/bin/python3.9
import pymysql
import sys
import logging
from tabulate import tabulate
from datetime import datetime, timedelta

# --- Logging setup ---
LOG_FILE = "/var/log/checks_logy/check_denylist.log"
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)

ACTION_NAMES = {
    0: "KEEP",
    1: "ADD",
    2: "REMOVE",
    3: "NOTIFY"
}

# --- Thresholds (tune these) ---
ALERT_THRESHOLDS = {
    "KEEP":    {"warning": 50,  "critical": 200},
    "ADD": {"warning": 20,  "critical": 100},
    "REMOVE": {"warning": 20,  "critical": 100},
    "NOTIFY": {"warning": 20,  "critical": 100}
    # add more actions here
}

# --- DB connections ---
conn_fare = pymysql.connect(
    host="ans-vip01-spc",
    user="nagios",
    password="@2019",
    db="fare",
    port=3306,
)

cur_fare = conn_fare.cursor(pymysql.cursors.DictCursor)
cur_fare.execute("SELECT oper_id, code FROM operator WHERE entity_type = 'operator'")
operators = cur_fare.fetchall()

exit_status = 0

for operator in operators:
    oper_id   = operator["oper_id"]
    oper_code = operator["code"]

    for list_name, table in [("LEGACY", "stoplist_ver"), ("DAVE", "denylist_entries")]:
        cursor = conn_fare.cursor(pymysql.cursors.DictCursor)

        # Assuming 'action' is the column for the action type, 'oper_id' is directly in the table,
        # and 'created_at' is the timestamp column. Adjust if your schema differs.
        query = f"""
            SELECT
                action,
                COUNT(*) AS cnt
            FROM {table}
            WHERE oper_id = %s
              AND created_at >= NOW() - INTERVAL 2 HOUR
            GROUP BY action
        """

        cursor.execute(query, (oper_id,))
        rows = cursor.fetchall()

        if not rows:
            logging.info(f"OK: [{list_name}] Operator {oper_code} - no denylist activity")
            cursor.close()
            continue

        table_data = []
        for row in rows:
            action_code = row["action"]
            cnt         = row["cnt"]
            action_name = ACTION_NAMES.get(action_code, f"UNKNOWN({action_code})")

            row["action"] = action_name   # replace int with label for the tabulate summary
            table_data.append(row)

            thresholds = ALERT_THRESHOLDS.get(action_name)
            if not thresholds:
                logging.info(f"[{list_name}] Operator {oper_code} | {action_name}: {cnt} (no threshold)")
                continue
            
            if cnt >= thresholds["critical"]:
                logging.critical(f"CRITICAL: [{list_name}] Operator {oper_code} | {action_name}: {cnt} >= {thresholds['critical']}")
                exit_status = max(exit_status, 2)
            elif cnt >= thresholds["warning"]:
                logging.warning(f"WARNING: [{list_name}] Operator {oper_code} | {action_name}: {cnt} >= {thresholds['warning']}")
                exit_status = max(exit_status, 1)
            else:
                logging.info(f"OK: [{list_name}] Operator {oper_code} | {action_name}: {cnt}")

        if table_data:
            summary = tabulate(table_data, headers="keys", tablefmt="grid")
            logging.info(f"[{list_name}] Operator {oper_code} summary:\n{summary}")

        cursor.close()


conn_fare.close()
sys.exit(exit_status)
