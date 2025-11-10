import socket
import json
import os
import logging
from datetime import datetime, date, timedelta
from flask import Flask, jsonify, request
from google.cloud import secretmanager, bigquery
import pandas as pd
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.DEBUG if os.environ.get('DEBUG', '0') == '1' else logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

def get_secret_payload():
    logger.info("Retrieving DB2 connection secret from Secret Manager")
    sm = secretmanager.SecretManagerServiceClient()
    project_id = os.environ.get("PROJECT_ID", "sis-sandbox-463113")
    secret_name = os.environ.get("DB2_SECRET_NAME", "IBM_connect")
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    logger.debug(f"Fetching secret: {name}")
    
    try:
        resp = sm.access_secret_version(name=name)
        config = json.loads(resp.payload.data.decode("utf-8"))
        # Log config keys but not values for security
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

DEFAULT_PROJECT = os.environ.get("PROJECT_ID", "sis-sandbox-463113")
DEFAULT_DATASET = os.environ.get("BQ_DATASET", "db2_migration") 
DEFAULT_STAGING = os.environ.get("BQ_STAGING_TABLE", "po_inbound_daily")
DEFAULT_FINAL = os.environ.get("BQ_FINAL_TABLE", "po_inbound_final")
RECENT_WINDOW_DAYS = int(os.environ.get("RECENT_WINDOW_DAYS", "14"))


def connect_to_db2(config):
    import ibm_db
    
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

def fetch_po_data(start_date=None, end_date=None, mock_mode=False, use_rolling_window=True):
    """
    Fetch PO data from DB2.
    
    Args:
        start_date: Start date for the query (optional if use_rolling_window=True)
        end_date: End date for the query (optional if use_rolling_window=True)
        mock_mode: If True, return mock data instead of querying DB2
        use_rolling_window: If True, use a rolling window of -30 to +60 days from today
    """
    if mock_mode:
        logger.info(f"Fetching mock data for date range: {start_date} to {end_date or start_date}")
        today = date.today()
        data = pd.DataFrame({
            'store': ['110', '114', '614'],
            'vendor': ['VENDOR1', 'VENDOR2', 'VENDOR3'],
            'ss': ['SS1', 'SS2', 'SS3'],
            'type': ['P', 'R', 'P'],
            'po_code': ['O', 'D', 'O'],
            'po': ['PO001', 'PO002', 'PO003'],
            'po_man': ['PM001', 'PM002', 'PM003'],
            'sku': ['SKU001', 'SKU002', 'SKU003'],
            'style': ['STY001', 'STY002', 'STY003'],
            'skudesc1': ['Description 1', 'Description 2', 'Description 3'],
            'expected_date': [today, today + timedelta(days=1), today + timedelta(days=2)],
            'cancel_date': [today + timedelta(days=30)] * 3,
            'ooqty': [100, 200, 150],
            'rcvdqty': [50, 100, 75],
            'breakoutqty': [10, 20, 15],
            'allocated': [40, 80, 60],
            'unitinvc_per_sku': [25, 30, 35],
            'shipped_sku': ['SKU001', 'SKU002', 'SKU003'],
            'shipped_qty': [45, 95, 70],
            'latest_datercv': [today - timedelta(days=1)] * 3,
            'shipnotice_date': [today - timedelta(days=2)] * 3,
            'record_key': ['PO001_SKU001_' + today.strftime('%Y%m%d'), 
                          'PO002_SKU002_' + (today + timedelta(days=1)).strftime('%Y%m%d'),
                          'PO003_SKU003_' + (today + timedelta(days=2)).strftime('%Y%m%d')]
        })
        return data
    
    # Calculate date range
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
        logger.info(f"Starting data fetch for date range: {start_date} to {end_date}")
    
    config = get_secret_payload()
    conn = connect_to_db2(config)
    import ibm_db
    
    query = f"""
    WITH LatestDate AS (
        SELECT 
            PH.PO#,
            MAX(PH.DATERCV) AS DateRCV
        FROM "ITMDATADDL".PURHSTHDR PH
        WHERE PH.STR# IN ('110','114','614','116','616','118','618')
        GROUP BY PH.PO#
    ),

    FirstQuery AS (
        SELECT 
            PH.POSTR# AS STORE#,
            VM.VENDNAME AS VENDOR,
            SK.SS#,
            COALESCE(PT.TYPE, 'INVALID') AS TYPE,  
            PH.POCODE AS PO_CODE,
            PH.PO# AS PO#,
            PH."PO#MAN",  
            SK.SKU#,
            SK.STYLE,
            SK.SKUDESC1,
            MAX(LD.DateRCV) AS LATEST_DATERCV,
            PH.DATEEXP AS EXPECTED_DATE,
            PH.DATECANC AS CANCEL_DATE,
            SUM(PD.OOUNIT) AS OOQTY,
            SUM(PD.RCUNIT) AS RCVDQTY
        FROM "ITM.DATA".POHDR PH 
        LEFT JOIN ITMDATADDL.POTYPEF PT ON PT.PO = PH.PO#  
        JOIN ITMDATADDL.PODTL PD ON PD.PO# = PH.PO# 
        JOIN ITMDATADDL.SKUFILE SK ON SK.SKU# = PD.SKU# 
        JOIN ITMDATADDL.VENDMAIN VM ON SK.VEND#1 = VM.VEND# 
        LEFT JOIN LatestDate LD ON PD.PO# = LD.PO# 
        WHERE PH.POSTR# IN ('110','114','614','116','616','118','618')
          AND PH.DATEEXP BETWEEN '{start_date}' AND '{end_date}'
          AND PH.POCODE IN ('O','D')
          AND PT.TYPE IN ('P','R')
        GROUP BY PH.POSTR#, VM.VENDNAME, SK.SS#, PH.DATECANC, PH.PO#, PT.TYPE, PH."PO#MAN", 
                 PH.DATEEXP, PH.POCODE, PH.DATESHIP, SK.SKU#, SK.STYLE, SK.SKUDESC1
    ),

    InvoiceSum AS (
        SELECT 
            "PO#" AS PO_NUMBER,
            SKU#,
            SUM(unitinvc) AS UNITINVC_PER_SKU
        FROM "ITM.DATA".EDIINVDTL
        GROUP BY "PO#", SKU#
    ),

    ASNDetails AS (
        SELECT 
            H.PO,
            H.SHPNOTCDT AS SHIPNOTICE_DATE,
            D.SKU,
            SUM(D.QTY) AS ASN_QTY
        FROM "ITM.DATA".ASNHDR H
        JOIN "ITM.DATA".ASNDTL D ON H.ASN = D.ASN
        WHERE H.STR IN ('110','114','614','116','616','118','618')
        GROUP BY H.PO, H.SHPNOTCDT, D.SKU
    )

    SELECT 
        FQ.STORE# AS store,
        FQ.VENDOR AS vendor,
        FQ.SS# AS ss,
        FQ.TYPE AS type,
        FQ.PO_CODE AS po_code,
        FQ.PO# AS po,
        FQ."PO#MAN" AS po_man,
        FQ.SKU# AS sku,
        FQ.STYLE AS style,
        FQ.SKUDESC1 AS skudesc1,
        FQ.EXPECTED_DATE AS expected_date,
        FQ.CANCEL_DATE AS cancel_date,
        FQ.OOQTY AS ooqty,
        FQ.RCVDQTY AS rcvdqty,
        COALESCE(SQ.BREAKOUTQTY, 0) AS breakoutqty,
        COALESCE(SQ.Allocated, 0) AS allocated,
        COALESCE(InvoiceSum.UNITINVC_PER_SKU, 0) AS unitinvc_per_sku,
        A.SKU AS shipped_sku,
        COALESCE(A.ASN_QTY, 0) AS shipped_qty,
        FQ.LATEST_DATERCV AS latest_datercv,
        A.SHIPNOTICE_DATE AS shipnotice_date,
        CONCAT(CONCAT(CONCAT(CONCAT(FQ.PO#, '_'), FQ.SKU#), '_'), VARCHAR_FORMAT(FQ.EXPECTED_DATE, 'YYYYMMDD')) AS record_key

    FROM FirstQuery FQ
    LEFT JOIN InvoiceSum 
        ON FQ.PO# = InvoiceSum.PO_NUMBER 
       AND FQ.SKU# = InvoiceSum.SKU#

    LEFT JOIN ASNDetails A 
        ON FQ.PO# = A.PO 
       AND FQ.SKU# = A.SKU

    LEFT JOIN (
        SELECT 
            A."PO#MAN",
            A.SKU#,
            SUM(A.OOQTY) AS BREAKOUTQTY,
            COALESCE(SUM(AD.QTY), 0) AS Allocated
        FROM (
            SELECT 
                PH."PO#MAN", 
                PD.SKU#,
                SUM(CASE WHEN PT.TYPE = 'C' THEN PD.OOUNIT ELSE 0 END) AS OOQTY
            FROM "ITMDATADDL".POHDR PH
            LEFT JOIN ITMDATADDL.POTYPEF PT ON PT.PO = PH.PO#  
            JOIN ITMDATADDL.PODTL PD ON PD."PO#" = PH.PO# 
            JOIN ITMDATADDL.SKUFILE SK ON SK.SKU# = PD.SKU#
            WHERE PH."PO#MAN" IS NOT NULL AND TRIM(PH."PO#MAN") <> ''
            GROUP BY PH."PO#MAN", PD.SKU#
        ) A
        LEFT JOIN (
            SELECT 
                PH."PO#MAN",
                AD.SKU,
                SUM(AD.QTY) AS QTY
            FROM ITMDATADDL.WIMSKUHST SWIM
            JOIN "ITM.DATA".ASNDTL AD ON SWIM.GENASN = AD.ASN
            JOIN ITMDATADDL.SKUFILE SK ON AD.SKU = SK.SKU#
            JOIN ITMDATADDL.POHDR PH ON SWIM."PO#" = PH.PO#
            WHERE PH."PO#MAN" IS NOT NULL AND TRIM(PH."PO#MAN") <> ''
            GROUP BY PH."PO#MAN", AD.SKU
        ) AD ON A."PO#MAN" = AD."PO#MAN" AND A.SKU# = AD.SKU
        GROUP BY A."PO#MAN", A.SKU#
    ) AS SQ 
        ON FQ."PO#MAN" = SQ."PO#MAN" 
       AND FQ.SKU# = SQ.SKU#

    ORDER BY FQ.PO#, FQ.SKU#, A.SHIPNOTICE_DATE
    """
    
    logger.info("Executing DB2 query...")
    try:
        stmt = ibm_db.exec_immediate(conn, query)
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
        ibm_db.close(conn)
        raise
    
    data = []
    row_count = 0
    row = ibm_db.fetch_assoc(stmt)
    while row:
        data.append(row)
        row_count += 1
        if row_count % 100 == 0:
            logger.info(f"Fetched {row_count} rows from DB2...")
        row = ibm_db.fetch_assoc(stmt)
    
    logger.info(f"DB2 Query completed. Total rows fetched from DB2: {row_count}")
    ibm_db.close(conn)
    logger.info("DB2 connection closed")
    df = pd.DataFrame(data)
    logger.info(f"DataFrame created with {len(df)} rows")
    
    if not df.empty:
        df.columns = df.columns.str.lower()
        
        # If record_key doesn't exist (for backward compatibility), create it
        if 'record_key' not in df.columns:
            logger.info("Generating record_key for backward compatibility")
            df['record_key'] = df.apply(lambda row: f"{row['po']}_{row['sku']}_{row['expected_date'].strftime('%Y%m%d') if pd.notna(row['expected_date']) else 'NULL'}", axis=1)
    
    return df

def cleanup_old_records(client, table_ref):
    """
    Remove records outside the rolling window (30 days past to 60 days future).
    """
    today = date.today()
    cutoff_date_past = today - timedelta(days=30)
    cutoff_date_future = today + timedelta(days=60)
    
    cleanup_query = f"""
    DELETE FROM `{table_ref}`
    WHERE expected_date < DATE('{cutoff_date_past}')
       OR expected_date > DATE('{cutoff_date_future}')
    """
    
    logger.info(f"Cleaning up records outside rolling window: < {cutoff_date_past} or > {cutoff_date_future}")
    
    try:
        cleanup_job = client.query(cleanup_query)
        cleanup_job.result()
        
        deleted_rows = cleanup_job.num_dml_affected_rows
        logger.info(f"Cleanup completed. Deleted {deleted_rows} records outside the rolling window")
        
        return deleted_rows
    except Exception as e:
        logger.warning(f"Cleanup failed (non-critical): {str(e)}")
        return 0

def load_to_bigquery(df, start_date=None, end_date=None, use_merge=True):
    """
    Load data to BigQuery:
      - Append to STAGING table
      - MERGE latest per record_key from a recent window into FINAL table
      - Cleanup old rows from STAGING (optional housekeeping)
    """
    client = bigquery.Client(project=DEFAULT_PROJECT)
    dataset_id = DEFAULT_DATASET
    staging_table = DEFAULT_STAGING
    final_table = DEFAULT_FINAL

    staging_ref = f"{client.project}.{dataset_id}.{staging_table}"
    final_ref = f"{client.project}.{dataset_id}.{final_table}"

    # --- Type normalization (unchanged) ---
    logger.info("Converting data types for BigQuery compatibility")
    integer_columns = ['ooqty', 'rcvdqty', 'breakoutqty', 'allocated', 'unitinvc_per_sku', 'shipped_qty']
    for col in integer_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('Int64')

    date_columns = ['expected_date', 'cancel_date', 'latest_datercv', 'shipnotice_date']
    for col in date_columns:
        if col in df.columns and df[col].notna().any():
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

    string_columns = ['store', 'vendor', 'ss', 'type', 'po_code', 'po', 'po_man',
                      'sku', 'style', 'skudesc1', 'shipped_sku', 'record_key']
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).replace('None', '')

    # Ensure record_key exists (safety)
    if 'record_key' not in df.columns:
        logger.info("Generating record_key (po_sku_expectedDate) because it was missing")
        df['record_key'] = df.apply(
            lambda row: f"{row.get('po','')}_{row.get('sku','')}_"
                        f"{row['expected_date'].strftime('%Y%m%d') if pd.notna(row.get('expected_date')) else 'NULL'}",
            axis=1
        )

    # Timestamp for this batch
    df['load_timestamp'] = pd.Timestamp.now(tz="UTC")

    # --- 1) Append to STAGING ---
    logger.info(f"Appending {len(df)} rows to staging: {staging_ref}")
    load_cfg = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    job = client.load_table_from_dataframe(df, staging_ref, job_config=load_cfg)
    job.result()

    if not use_merge:
        logger.info("use_merge=False → Skipping MERGE to final; staging append only")
        return len(df)

    # --- 2) MERGE into FINAL using latest per record_key from recent window ---
    logger.info(f"Merging latest per record_key from last {RECENT_WINDOW_DAYS} days into final: {final_ref}")
    merge_sql = f"""
    -- Ensure FINAL exists (schema will be inferred from staging on first successful MERGE)
    CREATE TABLE IF NOT EXISTS `{final_ref}` AS
    SELECT * FROM `{staging_ref}` WHERE 1=0;

    MERGE `{final_ref}` T
    USING (
      WITH recent AS (
        SELECT *
        FROM `{staging_ref}`
        WHERE DATE(load_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL {RECENT_WINDOW_DAYS} DAY)
      ),
      ranked AS (
        SELECT
         r.*,
         ROW_NUMBER() OVER (
            PARTITION BY record_key
            ORDER BY
              load_timestamp DESC,     -- newest batch wins
              shipnotice_date DESC,    -- tie-breaker 1 (if present)
              latest_datercv DESC,     -- tie-breaker 2
              rcvdqty DESC,            -- tie-breaker 3
              ooqty DESC               -- tie-breaker 4
         ) AS rn
        FROM recent r
     )
    SELECT * EXCEPT(rn)
     FROM ranked
     WHERE rn = 1
    ) S
    ON T.record_key = S.record_key
    WHEN MATCHED THEN UPDATE SET
      store = S.store,
      vendor = S.vendor,
      ss = S.ss,
      type = S.type,
      po_code = S.po_code,
      po = S.po,
      po_man = S.po_man,
      sku = S.sku,
      style = S.style,
      skudesc1 = S.skudesc1,
      expected_date = S.expected_date,
      cancel_date = S.cancel_date,
      ooqty = S.ooqty,
      rcvdqty = S.rcvdqty,
      breakoutqty = S.breakoutqty,
      allocated = S.allocated,
      unitinvc_per_sku = S.unitinvc_per_sku,
      shipped_sku = S.shipped_sku,
      shipped_qty = S.shipped_qty,
      latest_datercv = S.latest_datercv,
      shipnotice_date = S.shipnotice_date,
      load_timestamp = S.load_timestamp
    WHEN NOT MATCHED THEN
      INSERT ROW;
    """
    merge_job = client.query(merge_sql)
    merge_job.result()
    logger.info(f"MERGE complete (affected rows reported by DML counters may be null for MERGE).")

    # --- 3) Housekeeping: remove out-of-window data from STAGING only ---
    try:
        deleted = cleanup_old_records(client, staging_ref)
        logger.info(f"Staging cleanup deleted {deleted} old rows.")
    except Exception as e:
        logger.warning(f"Staging cleanup skipped/failed: {e}")

    return len(df)


@app.get("/")
def root():
    return "db2-migration-service up"

@app.get("/health")
def health():
    return jsonify({"status": "healthy", "service": "db2-migration-service"})

@app.get("/bq-health")
def bq_health():
    try:
        client = bigquery.Client(project=os.environ.get("PROJECT_ID", "sis-sandbox-463113"))
        rows = list(client.query('SELECT "ok" AS status, CURRENT_TIMESTAMP() ts').result())
        return jsonify({"status": rows[0]["status"], "ts": str(rows[0]["ts"])})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500

@app.get("/tcpcheck")
def tcpcheck():
    try:
        cfg = get_secret_payload()
        host = request.args.get("host", cfg.get("host"))
        port = int(request.args.get("port", cfg.get("port", 446)))
    except Exception as e:
        host = request.args.get("host")
        port = int(request.args.get("port", "446"))
    
    if not host:
        return jsonify({"error": "Host required. Pass ?host=... or configure in secret"}), 400
    
    s = socket.socket()
    s.settimeout(3.0)
    try:
        s.connect((host, port))
        s.close()
        return jsonify({"reachable": True, "host": host, "port": port})
    except Exception as e:
        return jsonify({"reachable": False, "host": host, "port": port, "error": str(e)}), 500

@app.get("/db2-diagnose")
def db2_diagnose():
    """Comprehensive DB2 connection diagnostics endpoint"""
    results = {
        "timestamp": datetime.now().isoformat(),
        "environment": {},
        "network_test": {},
        "secret_test": {},
        "connection_test": {}
    }
    
    # Check environment variables
    results["environment"] = {
        "PROJECT_ID": os.environ.get("PROJECT_ID", "sis-sandbox-463113"),
        "DB2_SECRET_NAME": os.environ.get("DB2_SECRET_NAME", "IBM_connect"),
        "USE_MOCK_DB": os.environ.get("USE_MOCK_DB", "0"),
        "DEBUG": os.environ.get("DEBUG", "0"),
        "BQ_DATASET": os.environ.get("BQ_DATASET", "db2_migration"),
        "BQ_TABLE": os.environ.get("BQ_TABLE", "po_inbound_daily")
    }
    
    # Try to get secret configuration
    try:
        config = get_secret_payload()
        results["secret_test"]["status"] = "success"
        results["secret_test"]["config_keys"] = list(config.keys())
        results["secret_test"]["host"] = config.get("host", "NOT SET")
        results["secret_test"]["port"] = config.get("port", 446)
        results["secret_test"]["database"] = config.get("database", "MYDB")
        results["secret_test"]["user"] = config.get("user", "NOT SET")
        results["secret_test"]["ssl_enabled"] = config.get("security") == "SSL"
        
        # Test network connectivity
        host = config.get("host")
        port = config.get("port", 446)
        if host:
            logger.info(f"Testing network connectivity to {host}:{port}")
            s = socket.socket()
            s.settimeout(5.0)
            try:
                s.connect((host, port))
                s.close()
                results["network_test"]["status"] = "success"
                results["network_test"]["message"] = f"Successfully connected to {host}:{port}"
            except Exception as e:
                results["network_test"]["status"] = "failed"
                results["network_test"]["error"] = str(e)
                results["network_test"]["message"] = f"Cannot reach {host}:{port} - check firewall/network"
        
        # Test actual DB2 connection
        if results["network_test"].get("status") == "success":
            try:
                logger.info("Testing DB2 connection...")
                conn = connect_to_db2(config)
                
                # Try to get server info
                import ibm_db
                server_info = ibm_db.server_info(conn)
                
                results["connection_test"]["status"] = "success"
                results["connection_test"]["server_name"] = server_info.DBMS_NAME if server_info else "Unknown"
                results["connection_test"]["server_version"] = server_info.DBMS_VER if server_info else "Unknown"
                results["connection_test"]["message"] = "DB2 connection successful"
                
                # Try a simple query
                try:
                    stmt = ibm_db.exec_immediate(conn, "VALUES CURRENT TIMESTAMP")
                    row = ibm_db.fetch_assoc(stmt)
                    results["connection_test"]["query_test"] = "success"
                    results["connection_test"]["server_time"] = str(row) if row else "N/A"
                except Exception as e:
                    results["connection_test"]["query_test"] = "failed"
                    results["connection_test"]["query_error"] = str(e)
                
                ibm_db.close(conn)
                
            except Exception as e:
                results["connection_test"]["status"] = "failed"
                results["connection_test"]["error"] = str(e)
                
                # Try to get detailed error info
                try:
                    import ibm_db
                    error_code = ibm_db.conn_error()
                    error_msg = ibm_db.conn_errormsg()
                    if error_code:
                        results["connection_test"]["db2_error_code"] = error_code
                        results["connection_test"]["db2_error_msg"] = error_msg
                except:
                    pass
                    
                # Common error interpretations
                error_str = str(e).lower()
                if "password" in error_str or "authentication" in error_str:
                    results["connection_test"]["likely_cause"] = "Authentication failed - check username/password"
                elif "database" in error_str:
                    results["connection_test"]["likely_cause"] = "Database not found - check database name"
                elif "ssl" in error_str or "security" in error_str:
                    results["connection_test"]["likely_cause"] = "SSL/Security configuration issue"
                elif "timeout" in error_str:
                    results["connection_test"]["likely_cause"] = "Connection timeout - DB2 server may be down"
                else:
                    results["connection_test"]["likely_cause"] = "Unknown - check DB2 server logs"
        else:
            results["connection_test"]["status"] = "skipped"
            results["connection_test"]["message"] = "Network test failed, skipping DB2 connection test"
            
    except Exception as e:
        results["secret_test"]["status"] = "failed"
        results["secret_test"]["error"] = str(e)
        results["network_test"]["status"] = "skipped"
        results["connection_test"]["status"] = "skipped"
    
    # Determine overall status
    if results["connection_test"].get("status") == "success":
        results["overall_status"] = "healthy"
        results["summary"] = "All tests passed - DB2 connection is working"
        status_code = 200
    else:
        results["overall_status"] = "unhealthy"
        if results["secret_test"].get("status") == "failed":
            results["summary"] = "Cannot retrieve DB2 credentials from Secret Manager"
        elif results["network_test"].get("status") == "failed":
            results["summary"] = "Cannot reach DB2 server - network/firewall issue"
        elif results["connection_test"].get("status") == "failed":
            results["summary"] = "Can reach DB2 server but cannot connect - check credentials/permissions"
        else:
            results["summary"] = "Unknown issue - check logs"
        status_code = 500
    
    return jsonify(results), status_code

@app.route("/run", methods=["GET", "POST"])
def run_migration():
    try:
        # Determine if we should use rolling window or specific dates
        use_rolling_window = request.args.get("use_rolling_window", "true").lower() == "true"
        mock_mode = os.environ.get("USE_MOCK_DB", "0") == "1" or request.args.get("mock", "false").lower() == "true"
        use_merge = os.environ.get("USE_MERGE", "true").lower() == "true" if not request.args.get("use_merge") else request.args.get("use_merge", "true").lower() == "true"
        
        if use_rolling_window:
            # Use rolling window mode (default for nightly runs)
            logger.info(f"Starting migration with rolling window mode, mock_mode: {mock_mode}, use_merge: {use_merge}")
            df = fetch_po_data(mock_mode=mock_mode, use_rolling_window=True)
            start_date = (date.today() - timedelta(days=30)).strftime("%Y-%m-%d")
            end_date = (date.today() + timedelta(days=60)).strftime("%Y-%m-%d")
        else:
            # Use specific date range (for manual runs or backfills)
            yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
            start_date = request.args.get("start_date", request.args.get("date", yesterday))
            end_date = request.args.get("end_date", start_date)
            logger.info(f"Starting migration for specific date range: {start_date} to {end_date}, mock_mode: {mock_mode}")
            df = fetch_po_data(start_date=start_date, end_date=end_date, mock_mode=mock_mode, use_rolling_window=False)
        
        if df.empty:
            logger.warning(f"No data found for date range: {start_date} to {end_date}")
            return jsonify({
                "status": "no_data",
                "start_date": start_date,
                "end_date": end_date,
                "message": "No data found for the specified date range",
                "use_rolling_window": use_rolling_window
            })
        
        rows_loaded = load_to_bigquery(df, start_date, end_date, use_merge=use_merge)
        logger.info(f"Migration completed successfully. Rows loaded: {rows_loaded}")
        
        return jsonify({
            "status": "success",
            "start_date": start_date,
            "end_date": end_date,
            "rows_loaded": rows_loaded,
            "rows_fetched": len(df),
            "mock_mode": mock_mode,
            "use_rolling_window": use_rolling_window,
            "use_merge": use_merge,
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error in migration: {str(e)}")
        import traceback
        logger.error(f"Full traceback:\n{traceback.format_exc()}")
        
        # Try to get more specific DB2 error info if available
        error_details = {"error": str(e)}
        try:
            import ibm_db
            if ibm_db.conn_error():
                error_details["db2_error_code"] = ibm_db.conn_error()
                error_details["db2_error_msg"] = ibm_db.conn_errormsg()
        except:
            pass
        
        return jsonify({
            "status": "error",
            **error_details,
            "start_date": start_date if 'start_date' in locals() else None,
            "end_date": end_date if 'end_date' in locals() else None
        }), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))