#!/bin/python3.9

import pymysql
import logging
import subprocess
import sys
#import sqlalchemy as db
from datetime import date, timedelta, datetime, time
from configparser import ConfigParser
import os.path
from pathlib import Path
import urllib.parse


""" Konfigurace logovani """
logging.basicConfig(format='%(asctime)s - %(levelname)s -  %(message)s', level=logging.DEBUG)


def handle_exception(exc_type, exc_value, exc_traceback):
    # Log the exception
    logging.error("Uncaught exception occurred:", exc_info=(exc_type, exc_value, exc_traceback))

sys.excepthook = handle_exception


def mysqlconnect():
    conn = None
    tap_id = None

    try:
        # Connect to MySQL database
        conn = pymysql.connect(
            host='pmydbtrans-vip01-spc',
            user='app_fare',
            password='fare@2016',
            database='fare'
        )

        # choose oper_id and token
        operator_id = int(input("Add operator id: "))
        token = input("Add token from authorization for chosen operator: ")

        # used tables from fare db
        tables = ["auth_tx", "pay_window", "token", "tap", "operator"]


        def kafka_send_status(conn, table, operator_id, token, tap_id=None):
            try:
                with conn.cursor() as cursor:
                    # operator doesn't have token
                    if table == 'operator':
                        query = f"""
                            SELECT kafka_send_status
                            FROM {table}
                            WHERE oper_id = %s AND kafka_send_status = 1
                        """
                        cursor.execute(query, (operator_id,))

                    else:
                        # other tables
                        query = f"""
                            SELECT kafka_send_status
                            FROM {table}
                            WHERE oper_id = %s AND token = %s AND kafka_send_status = 1
                        """
                        cursor.execute(query, (operator_id, token))

                    result = cursor.fetchone()

                    # Check if kafka_send_status is 1
                    if result and result[0] == 1:
                        print(f"Table {table}: Kafka send status is OK for token {token}.")
                        return True
                    else:
                        print(f"Table {table}: Kafka send status is NOT OK for token {token}.")
                        return False

            except Exception as e:
                print(f"Error querying table {table}: {e}")
                return False

        # Execute the query for each table
        status_checks = [kafka_send_status(conn, table, operator_id, token) for table in tables]

        # Final result
        if all(status_checks):
            print(f"Token {token} has kafka_send_status OK in all tables.")

            #Last table for kafka using tapid
            check_tap_id = input("Do you want to check tap_id from the ticket_result table too? (Yes/No): ").strip().lower()

            if check_tap_id == "yes":
                tap_id = get_tap_id(conn, token, operator_id)

                if tap_id:
                    # Check kafka_send_status in the ticket_result table using the tap_id
                    ticket_result_ok = kafka_send_status_ticket_result(conn, tap_id)
                    if ticket_result_ok:
                        print("Everything is OK, closing the script.")
                    #else:
                        #print("Tap ID check failed in ticket_result table.")
                else:
                    print("No valid tap_id found, skipping tap_id check.")
            else:
                print("No tap_id check. Exiting script.")
                print("Everything is OK, closing the script.")
        else:
            print(f"Token {token} does NOT have kafka_send_status OK in all tables.")



    except Exception as e:
        print(f"Error connecting to the database: {e}")

    finally:
        # Close the database connection
        if conn:
            conn.close()


# Function to get tap_id from tap table
def get_tap_id(conn, token, operator_id):
    try:
        with conn.cursor() as cursor:
            query = """
                SELECT tapid
                FROM tap
                WHERE token = %s AND oper_id = %s
            """
            cursor.execute(query, (token, operator_id))
            result = cursor.fetchone()
            if result:
                return result[0]
            else:
                print(f"Token {token} not found in tap table.")
                return None
    except Exception as e:
        print(f"Error querying tap_id for token {token}: {e}")
        return None


# Function to check kafka_send_status in the ticket_result table using tap_id
def kafka_send_status_ticket_result(conn, tap_id):
    try:
        with conn.cursor() as cursor:
            query = """
                SELECT kafka_send_status
                FROM ticket_result
                WHERE tap_id = %s AND kafka_send_status = 1
            """
            cursor.execute(query, (tap_id,))
            result = cursor.fetchone()

            # Check if kafka_send_status is 1
            if result and result[0] == 1:
                print(f"ticket_result: Kafka send status is OK for tap_id {tap_id}.")
            else:
                print(f"ticket_result: There isn't id in ticket_result table for current token for specific tap_id AKA status = EMPTY {tap_id}.")

    except Exception as e:
        print(f"Error querying ticket_result table: {e}")

# Run the connection and checks
mysqlconnect()