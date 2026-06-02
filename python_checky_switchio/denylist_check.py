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

# info z denylistu
ACTION_NAMES = {
    0: "KEEP",
    1: "ADD",
    2: "REMOVE",
    3: "NOTIFY",
}

# budeme hlidat jen podle procenta tokenu pridanych na DL
ALERT_THRESHOLDS = {
    #"KEEP":   {"warning": 50,  "critical": 200},
    #"ADD":    {"warning": 20,  "critical": 100},
    #"REMOVE": {"warning": 20,  "critical": 100},
    #"NOTIFY": {"warning": 20,  "critical": 100},
    "PERCENTAGE": {"warning": 25,  "critical": 50},
}

INACTIVE_OPERATORS = [
        "KLAGENFURT",
        "ARAD",
        "SANTDCHBUS",
        "EPURSELG",
        "ZTMTYCHY"
    ]

# dl definice
DENYLIST_SOURCES = [
    {
        "name":       "LEGACY",
        "table":      "stoplist_inc",
        "table_alias": "si",
        "oper_col":   "oper_id",
        "action_col": "type",
        "time_col":   "dttm",
        "stoplist_engine_value": "BINARY",
    },
    {
        "name":       "DAVELIST",
        "table":      "denylist_entries",
        "table_alias": "de",
        "oper_col":   "operator_id",
        "action_col": "action",
        "time_col":   "created_time",
        "stoplist_engine_value": "DAVE_LIST",
    },
]

def check_denylist_status() -> None:
    conn = pymysql.connect(
        host="",
        user="",
        password="",
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

        if oper_code in INACTIVE_OPERATORS:
            continue
        
        tap_cursor = conn.cursor(pymysql.cursors.DictCursor)
        tap_query = """
            SELECT COUNT(*) AS total_taps FROM tap
            WHERE oper_id = %s AND registered = 1 AND server_dttm >= NOW() - INTERVAL 2 HOUR
        """
        tap_cursor.execute(tap_query, (oper_id,))
        tap_count_result = tap_cursor.fetchone()
        tap_cursor.close()
        
        total_taps = tap_count_result['total_taps'] if tap_count_result else 0
        logging.info(f"Operator {oper_code} (ID: {oper_id}) - {total_taps} registered taps in last 2 hours.")


        prop_cursor = conn.cursor(pymysql.cursors.DictCursor)
        prop_query = """
            SELECT value FROM operator_property
            WHERE operator_id = %s AND property_key = 'STOPLIST_ENGINE'
        """
        prop_cursor.execute(prop_query, (oper_id,))
        operator_stoplist_engine_prop = prop_cursor.fetchone()
        prop_cursor.close()

        if not operator_stoplist_engine_prop:
            logging.warning(f"Operator {oper_code} (ID: {oper_id}) has no 'STOPLIST_ENGINE' property. Skipping all denylist checks for this operator.")
            continue

        operator_stoplist_engine = operator_stoplist_engine_prop['value']

        for source in DENYLIST_SOURCES:
            list_name               = source["name"]
            stoplist_engine_value   = source["stoplist_engine_value"]

            
            if stoplist_engine_value != operator_stoplist_engine:
                continue
            
            add_count_for_source = 0
            source_status = 0

            table                   = source["table"]
            table_alias             = source["table_alias"]
            oper_col                = source["oper_col"]
            action_col              = source["action_col"]
            time_col                = source["time_col"]

            cursor = conn.cursor(pymysql.cursors.DictCursor)
            query = f"""
                SELECT {table_alias}.{oper_col} AS oper_id, {table_alias}.{action_col} AS action, COUNT(*) AS count
                FROM {table} {table_alias}
                JOIN operator_property op ON {table_alias}.{oper_col} = op.operator_id
                WHERE {table_alias}.{oper_col} = %s
                  AND {table_alias}.{time_col} >= NOW() - INTERVAL 2 HOUR
                  AND op.property_key = 'STOPLIST_ENGINE'
                  AND op.value = %s
                GROUP BY {table_alias}.{oper_col}, {table_alias}.{action_col}
            """
            cursor.execute(query, (oper_id, stoplist_engine_value))
            rows = cursor.fetchall()

            if not  rows:
                logging.warning(f"[{list_name}] Operator {oper_code} - no denylist activity in last 2 hours")
                cursor.close()
                continue

            table_data = []
            for row in rows:
                action_code = row["action"]
                count       = row["count"]
                action_name = ACTION_NAMES.get(action_code, f"UNKNOWN({action_code})")
                
                if action_name == "ADD":
                    add_count_for_source += count

                row["action"] = action_name
                table_data.append(row)

                thresholds = ALERT_THRESHOLDS.get(action_name)
                if not thresholds:
                    continue

                if count >= thresholds["critical"]:                                                        
                    source_status = max(source_status, 2)
                elif count >= thresholds["warning"]:
                    source_status = max(source_status, 1)

            if total_taps > 0 and add_count_for_source > 0:
                add_percentage = (add_count_for_source / total_taps) * 100
                table_data.append({
                    "oper_id": oper_id,
                    "action": "TOKEN_ON_DL_FROM_COUNT",
                    "count": f"{add_percentage:.2f}%"
                })

                percentage_thresholds = ALERT_THRESHOLDS.get("PERCENTAGE")
                if percentage_thresholds:
                    if add_percentage >= percentage_thresholds["critical"]:
                        source_status = max(source_status, 2)
                    elif add_percentage >= percentage_thresholds["warning"]:
                        source_status = max(source_status, 1)
            
            exit_status = max(exit_status, source_status)

            if table_data:                                                                                                                                                           
                summary = tabulate(table_data, headers="keys", tablefmt="grid")                                                                                                                                                             
                                                                                                                                                                               
                if source_status == 2:                                                                                                                                                                                                        
                    logging.critical(f"[{list_name}] Operator {oper_code} summary:\n{summary}")                                                                                                                                   
                elif source_status == 1:                                                                                                                                                                                                      
                    logging.warning(f"[{list_name}] Operator {oper_code} summary:\n{summary}")                                                                                                                                    
                else:                                                                                                                                                                                                                       
                    logging.info(f"[{list_name}] Operator {oper_code} summary:\n{summary}")

            cursor.close()

    conn.close()
    sys.exit(exit_status)

if __name__ == "__main__":
    check_denylist_status()