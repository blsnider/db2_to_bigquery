import socket
import json
import os
import logging
import time
from google.cloud import secretmanager
import pandas as pd
import numpy as np
from datetime import date, timedelta

logger = logging.getLogger(__name__)

def get_secret_payload():
    """Retrieve DB2 connection configuration from Google Secret Manager"""
    logger.info("Retrieving DB2 connection secret from Secret Manager")
    sm = secretmanager.SecretManagerServiceClient()
    project_id = os.environ.get("PROJECT_ID", "sis-sandbox-463113")
    secret_name = os.environ.get("DB2_SECRET_NAME", "IBM_connect")
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    logger.debug(f"Fetching secret: {name}")

    try:
        resp = sm.access_secret_version(name=name)
        config = json.loads(resp.payload.data.decode("utf-8"))
        logger.info(f"Secret retrieved successfully. Config keys: {list(config.keys())}")
        logger.debug(f"DB2 Host: {config.get('host', 'NOT SET')}")
        logger.debug(f"DB2 Port: {config.get('port', 446)}")
        logger.debug(f"DB2 Database: {config.get('database', 'MYDB')}")
        logger.debug(f"DB2 User: {config.get('user', 'NOT SET')}")
        logger.debug(f"SSL Enabled: {config.get('security') == 'SSL'}")
        return config
    except Exception as e:
        logger.error(f"Failed to retrieve secret: {str(e)}")
        raise

def connect_to_db2(config=None):
    """Establish connection to DB2 database"""
    import ibm_db

    if config is None:
        config = get_secret_payload()

    # Enable IBM DB2 driver debug mode if DEBUG is set
    if os.environ.get('DEBUG', '0') == '1':
        logger.info("Enabling IBM DB2 driver debug mode")
        try:
            ibm_db.debug(True)
        except Exception as e:
            logger.warning(f"Could not enable DB2 debug mode: {e}")

    # Build connection string
    conn_str = f"DATABASE={config.get('database', 'MYDB')};"
    conn_str += f"HOSTNAME={config['host']};"
    conn_str += f"PORT={config.get('port', 446)};"
    conn_str += f"PROTOCOL=TCPIP;"
    conn_str += f"UID={config['user']};"
    conn_str += f"PWD={config['password']};"
    if config.get('security') == 'SSL':
        conn_str += "SECURITY=SSL;"
        logger.info("SSL security enabled for DB2 connection")

    # Log connection details (mask password)
    masked_conn_str = conn_str.replace(config['password'], '***MASKED***')
    logger.info(f"Attempting DB2 connection with string: {masked_conn_str}")

    # Test network connectivity first
    logger.debug(f"Testing TCP connectivity to {config['host']}:{config.get('port', 446)}")
    try:
        test_sock = socket.socket()
        test_sock.settimeout(5)
        test_sock.connect((config['host'], config.get('port', 446)))
        test_sock.close()
        logger.info(f"TCP connection successful to {config['host']}:{config.get('port', 446)}")
    except Exception as e:
        logger.error(f"TCP connection failed to {config['host']}:{config.get('port', 446)}: {e}")
        logger.error("This indicates a network/firewall issue - DB2 server is not reachable")

    # Attempt DB2 connection
    logger.info("Initiating DB2 connection...")
    try:
        conn = ibm_db.connect(conn_str, "", "")
        logger.info("DB2 connection established successfully")

        # Log connection info if successful
        try:
            server_info = ibm_db.server_info(conn)
            if server_info:
                logger.info(f"Connected to DB2 server: {server_info.DBMS_NAME} {server_info.DBMS_VER}")
                logger.debug(f"Server details: {vars(server_info)}")
        except Exception as e:
            logger.debug(f"Could not retrieve server info: {e}")

        # Enable autocommit - don't hold transactions
        try:
            ibm_db.autocommit(conn, ibm_db.SQL_AUTOCOMMIT_ON)
            logger.info("Enabled autocommit mode - no transactions held")
        except Exception as e:
            logger.warning(f"Could not enable autocommit: {e}")

        # Set isolation to UR (Uncommitted Read) for DB2 for i
        try:
            ibm_db.exec_immediate(conn, "SET CURRENT ISOLATION = UR")
            logger.info("Set CURRENT ISOLATION to UR (Uncommitted Read)")
        except Exception as e:
            logger.warning(f"Could not set CURRENT ISOLATION UR: {e}")

        # Set lock timeout to 5 seconds (fail fast on locks)
        try:
            ibm_db.exec_immediate(conn, "SET CURRENT LOCK TIMEOUT 5")
            logger.info("Set CURRENT LOCK TIMEOUT to 5 seconds")
        except Exception as e:
            logger.warning(f"Could not set CURRENT LOCK TIMEOUT: {e}")

        return conn
    except Exception as e:
        # Get detailed error information
        logger.error(f"DB2 connection failed: {str(e)}")
        try:
            error_msg = ibm_db.conn_errormsg()
            error_code = ibm_db.conn_error()
            logger.error(f"DB2 Error Code: {error_code}")
            logger.error(f"DB2 Error Message: {error_msg}")
        except:
            logger.error("Could not retrieve detailed DB2 error information")
        raise

def exec_with_retry(conn, sql, max_retries=3):
    """Execute SQL with retry logic for deadlock/timeout errors"""
    import ibm_db

    attempt = 0
    while True:
        try:
            return ibm_db.exec_immediate(conn, sql)
        except Exception as e:
            # Look for SQLSTATE 57033 or SQLCODE -913 (deadlock/timeout)
            msg = str(e)
            try:
                state = ibm_db.stmt_errormsg()
            except:
                state = ""

            if ("-913" in msg or "SQL0913N" in msg or "57033" in msg or "57033" in state or "deadlock" in msg.lower()):
                attempt += 1
                if attempt > max_retries:
                    logger.error(f"Max retries ({max_retries}) exceeded for deadlock/timeout")
                    raise
                sleep_s = min(2 ** attempt, 8)  # 2, 4, 8 seconds
                logger.warning(f"Deadlock/timeout on attempt {attempt}/{max_retries}; retrying in {sleep_s}s...")
                time.sleep(sleep_s)
                continue
            raise

def execute_query(query, start_date=None, end_date=None, conn=None):
    """Execute a DB2 query and return results as DataFrame"""
    import ibm_db

    # Establish connection if not provided
    close_conn = False
    if conn is None:
        conn = connect_to_db2()
        close_conn = True

    # Replace date parameters in query
    if start_date and end_date:
        query = query.replace(':START_DATE', f"'{start_date}'")
        query = query.replace(':END_DATE', f"'{end_date}'")

    logger.info(f"Executing DB2 query for date range: {start_date} to {end_date}")

    try:
        stmt = exec_with_retry(conn, query)
        logger.info("Query executed successfully, fetching results...")
    except Exception as e:
        logger.error(f"Query execution failed: {str(e)}")
        try:
            error_msg = ibm_db.stmt_errormsg()
            error_code = ibm_db.stmt_error()
            logger.error(f"DB2 Statement Error Code: {error_code}")
            logger.error(f"DB2 Statement Error Message: {error_msg}")
        except:
            pass
        if close_conn:
            ibm_db.close(conn)
        raise

    # Fetch results
    data = []
    row_count = 0
    row = ibm_db.fetch_assoc(stmt)
    while row:
        data.append(row)
        row_count += 1
        if row_count % 100 == 0:
            logger.info(f"Fetched {row_count} rows from DB2...")
        row = ibm_db.fetch_assoc(stmt)

    logger.info(f"DB2 Query completed. Total rows fetched: {row_count}")

    # Commit to release any locks
    try:
        ibm_db.commit(conn)
        logger.info("Transaction committed, locks released")
    except Exception as e:
        logger.warning(f"Could not commit transaction: {e}")

    if close_conn:
        ibm_db.close(conn)
        logger.info("DB2 connection closed")

    # Convert to DataFrame
    df = pd.DataFrame(data)
    if not df.empty:
        df.columns = df.columns.str.lower()

    logger.info(f"DataFrame created with {len(df)} rows")
    return df

def get_date_range(use_rolling_window=True, start_date=None, end_date=None):
    """Calculate date range for queries"""
    if use_rolling_window:
        # Use rolling window: 30 days back, 60 days forward from today
        today = date.today()
        start_date = today - timedelta(days=30)
        end_date = today + timedelta(days=60)
        logger.info(f"Using rolling window: {start_date} to {end_date} (30 days back, 60 days forward)")
    else:
        # Use provided dates
        if not start_date:
            start_date = date.today()
        if not end_date:
            end_date = start_date
        logger.info(f"Using specified date range: {start_date} to {end_date}")

    return start_date, end_date