#!/usr/bin/env python3.9

import configparser
import logging
import os
import sys
from datetime import datetime

import pymysql
import pymysql.cursors
import requests

DB_HOST     = os.getenv("DB_HOST",     "01-spc")
DB_PORT     = int(os.getenv("DB_PORT", "3306"))
DB_USER     = os.getenv("DB_USER",     "")
DB_PASSWORD = os.getenv("DB_PASSWORD", "@2019")
DB_NAME     = os.getenv("DB_NAME",     "fare")

# Full path to operator.ini
OPERATOR_INI_PATH = os.getenv("OPERATOR_INI", "/opt/fare/scripts/operator.ini")

# Slack incoming webhook URL
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/XXX/YYY/ZZZ")

# Log file path
LOG_FILE = os.getenv("OPERATOR_CHECK_LOG", "/home/mholomek/operator_to_ini/check_operators.log")


logging.basicConfig(
    level=logging.WARN,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)



def fetch_db_codes():
    # type: () -> set
    """Return all operator codes from the database table using pymysql."""
    conn = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        db=DB_NAME,
        port=DB_PORT,
        connect_timeout=10,
        read_timeout=60,
        write_timeout=60,
        charset="utf8mb4",
    )
    codes = set()
    try:
        cur = conn.cursor(pymysql.cursors.DictCursor)
        try:
            cur.execute(
                "SELECT oper_id, code FROM operator WHERE entity_type = 'operator' and type != 11 and oper_id not in (1,235,156,3)"
            )
            operators = cur.fetchall()
        except pymysql.Error as exc:
            log.critical("Database error when fetching operators: %s", exc)
            raise
        finally:
            cur.close()

        for operator in operators:
            code = operator["code"]
            if code:
                codes.add(code.strip().upper())

        log.info("DB: found %d operator(s): %s", len(codes), sorted(codes))
        return codes
    finally:
        conn.close()


def fetch_ini_codes():
    # type: () -> set
    """Return all section names (operator codes) defined in operator.ini."""
    if not os.path.isfile(OPERATOR_INI_PATH):
        raise FileNotFoundError(
            "operator.ini not found at: {}".format(OPERATOR_INI_PATH)
        )

    parser = configparser.ConfigParser()
    parser.read(OPERATOR_INI_PATH, encoding="utf-8")

    # configparser always includes a [DEFAULT] pseudo-section — parser.sections() excludes it
    codes = {section.strip().upper() for section in parser.sections()}
    log.info("INI: found %d section(s): %s", len(codes), sorted(codes))
    return codes


def post_to_slack(missing):
    # type: (set) -> None
    """Send a Slack notification """
    lines = "\n".join("  \u2022 `{}`".format(code) for code in sorted(missing))
    text = (
        ":warning: *Operator config drift detected* \u2014 {ts}\n"
        "The following operator(s) exist in the *database* but are *missing from operator.ini*:\n"
        "{lines}\n"
        "Please add the missing section(s) to `{path}` and restart fare-worker."
    ).format(
        ts=datetime.now().strftime("%Y-%m-%d %H:%M"),
        lines=lines,
        path=OPERATOR_INI_PATH,
    )

    payload = {"text": text}
    resp = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
    if resp.status_code != 200:
        log.error("Slack notification failed: HTTP %s — %s", resp.status_code, resp.text)
    else:
        log.info("Slack notification sent successfully.")


def main():
    # type: () -> int
    log.info("=== Operator check started ===")

    try:
        db_codes  = fetch_db_codes()
        ini_codes = fetch_ini_codes()
    except pymysql.Error as exc:
        log.error("Database error: %s", exc)
        return 2
    except FileNotFoundError as exc:
        log.error("%s", exc)
        return 1

    missing = db_codes - ini_codes

    if not missing:
        log.info(
            "OK \u2014 all %d operator(s) from the DB are present in operator.ini.",
            len(db_codes),
        )
        log.info("=== Operator check finished \u2014 no issues ===")
        return 0

    log.warning(
        "MISSING from operator.ini (%d operator(s)): %s",
        len(missing),
        ", ".join(sorted(missing)),
    )

    try:
        post_to_slack(missing)
    except Exception as exc:
        log.error("Could not send Slack alert: %s", exc)

    log.info("=== Operator check finished \u2014 %d issue(s) found ===", len(missing))
    return 1


if __name__ == "__main__":
    sys.exit(main())