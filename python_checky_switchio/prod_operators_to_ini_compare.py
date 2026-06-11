#!/usr/bin/env python3.9

 
import configparser
import logging
import os
import sys
from datetime import datetime
import mysql.connector
import requests
 
DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "pmydbtrans-vip01-spc"),
    "port":     int(os.getenv("DB_PORT", "3306")),
    "user":     os.getenv("DB_USER",     "mon_nagios"),
    "password": os.getenv("DB_PASSWORD", "nagmon@2019"),
    "database": os.getenv("DB_NAME",     "fare"),
}
 
# Full path to operator.ini
OPERATOR_INI_PATH = os.getenv("OPERATOR_INI", "/opt/fare/fare-worker/conf/operator.ini")
 
# Slack incoming webhook URL
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/XXX/YYY/ZZZ")
 
# Log file path
LOG_FILE = os.getenv("OPERATOR_CHECK_LOG", "/var/log/checks_logy/check_operators.log")
 
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

 
def fetch_db_codes() -> set[str]:
    """Return all operator codes from the database table."""
    conn = mysql.connector.connect(**DB_CONFIG)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT code FROM operator where entity_type = 'operator'")
        rows = cursor.fetchall()
        codes = {row[0].strip().upper() for row in rows if row[0]}
        log.info("DB: found %d operator(s): %s", len(codes), sorted(codes))
        return codes
    finally:
        conn.close()
 
 
def fetch_ini_codes() -> set[str]:
    """Return all section names (operator codes) defined in operator.ini."""
    if not os.path.isfile(OPERATOR_INI_PATH):
        raise FileNotFoundError(f"operator.ini not found at: {OPERATOR_INI_PATH}")
 
    parser = configparser.ConfigParser()
    parser.read(OPERATOR_INI_PATH, encoding="utf-8")
 
    codes = {section.strip().upper() for section in parser.sections()}
    log.info("INI: found %d section(s): %s", len(codes), sorted(codes))
    return codes
 
 
def post_to_slack(missing: set[str]) -> None:
    """Send a Slack notification listing the missing operators."""
    lines = "\n".join(f"  • `{code}`" for code in sorted(missing))
    text = (
        f":warning: *Operator config drift detected* — {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
        f"The following operator(s) exist in the *database* but are *missing from operator.ini*:\n"
        f"{lines}\n"
        f"Please add the missing section(s) to `{OPERATOR_INI_PATH}` and restart fare-worker."
    )
    payload = {"text": text}
    resp = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
    if resp.status_code != 200:
        log.error("Slack notification failed: HTTP %s — %s", resp.status_code, resp.text)
    else:
        log.info("Slack notification sent successfully.")
 

 
def main() -> int:
    log.info("=== Operator check started ===")
 
    try:
        db_codes  = fetch_db_codes()
        ini_codes = fetch_ini_codes()
    except mysql.connector.Error as exc:
        log.error("Database error: %s", exc)
        return 1
    except FileNotFoundError as exc:
        log.error("%s", exc)
        return 1
 
    missing = db_codes - ini_codes
 
    if not missing:
        log.info("OK — all %d operator(s) from the DB are present in operator.ini.", len(db_codes))
        log.info("=== Operator check finished — no issues ===")
        return 0
 
    log.warning(
        "MISSING from operator.ini (%d operator(s)): %s",
        len(missing),
        ", ".join(sorted(missing)),
    )
 
    try:
        post_to_slack(missing)
    except Exception as exc:          # noqa: BLE001
        log.error("Could not send Slack alert: %s", exc)
 
    log.info("=== Operator check finished — %d issue(s) found ===", len(missing))
    return 1                          # non-zero exit so cron can detect a problem
 
 
if __name__ == "__main__":
    sys.exit(main())