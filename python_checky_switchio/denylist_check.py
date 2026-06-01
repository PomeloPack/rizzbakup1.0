#!/usr/bin/python3.9
import pymysql
import sys
import logging
from tabulate import tabulate

# --- Logging setup ---
LOG_FILE = "/var/log/checks_logy/check_denylist.log"
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)

# --- Action code mapping ---
ACTION_NAMES = {
    0: "KEEP",
    1: "ADD",
    2: "REMOVE",
    3: "NOTIFY",
}

# --- Thresholds per action ---
ALERT_THRESHOLDS = {
    "KEEP":   {"warning": 50,  "critical": 200},
    "ADD":    {"warning": 20,  "critical": 100},
    "REMOVE": {"warning": 20,  "critical": 100},
    "NOTIFY": {"warning": 20,  "critical": 100},
}

# --- Denylist source definitions ---
DENYLIST_SOURCES = [
    {
        "name":       "LEGACY",
        "table":      "stoplist_inc",
        "oper_col":   "oper_id",
        "action_col": "type",
        "time_col":   "dttm",
    },
    {
        "name":       "DAVELIST",
        "table":      "denylist_entries",
        "oper_col":   "operator_id",
        "action_col": "action",
        "time_col":   "created_time",
    },
]

def check_denylist_status() -> None:
    conn = pymysql.connect(
        host="-vip01-spc",
        user="",
        password="@2019",
        db="fare",
        port=3306
    )

    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute("SELECT oper_id, code FROM operator WHERE entity_type = 'operator'")
    operators = cur.fetchall()


    exit_status = 0

    for operator in operators:
        oper_id   = operator["oper_id"]
        oper_code = operator["code"]

        for source in DENYLIST_SOURCES:
            list_name   = source["name"]
            table       = source["table"]
            oper_col    = source["oper_col"]
            action_col  = source["action_col"]
            time_col    = source["time_col"]

            cursor = conn.cursor(pymysql.cursors.DictCursor)
            query = f"""
                SELECT {oper_col} AS oper_id, {action_col} AS action, COUNT(*) AS count
                FROM {table}
                WHERE {oper_col} = %s
                  AND {time_col} >= NOW() - INTERVAL 2 HOUR
                GROUP BY {oper_col}, {action_col}
            """
            cursor.execute(query, (oper_id,))
            rows = cursor.fetchall()

            if not rows:
                logging.info(f"OK: [{list_name}] Operator {oper_code} - no denylist activity in last 2 hours")
                cursor.close()
                continue

            table_data = []
            for row in rows:
                action_code = row["action"]
                count       = row["count"]
                action_name = ACTION_NAMES.get(action_code, f"UNKNOWN({action_code})")

                row["action"] = action_name  # replace int with label for tabulate
                table_data.append(row)

                thresholds = ALERT_THRESHOLDS.get(action_name)
                if not thresholds:
                    logging.info(f"[{list_name}] Operator {oper_code} | {action_name}: {count} (no threshold configured)")
                    continue

                if count >= thresholds["critical"]:
                    logging.critical(f"CRITICAL: [{list_name}] Operator {oper_code} | {action_name}: {count} >= {thresholds['critical']}")
                    exit_status = max(exit_status, 2)
                elif count >= thresholds["warning"]:
                    logging.warning(f"WARNING:  [{list_name}] Operator {oper_code} | {action_name}: {count} >= {thresholds['warning']}")
                    exit_status = max(exit_status, 1)
                else:
                    logging.info(f"OK:       [{list_name}] Operator {oper_code} | {action_name}: {count}")

            if table_data:
                summary = tabulate(table_data, headers="keys", tablefmt="grid")
                logging.info(f"[{list_name}] Operator {oper_code} summary:\n{summary}")

            cursor.close()

    conn.close()
    sys.exit(exit_status)

if __name__ == "__main__":
    check_denylist_status()