import os
import logging
from datetime import datetime, date, timedelta
from flask import Flask, jsonify, request
from google.cloud import bigquery
import socket

# Import our modular components
from queries import po_query, breakout_query
from utils import db_utils, bq_utils

# Configure logging
logging.basicConfig(
    level=logging.DEBUG if os.environ.get('DEBUG', '0') == '1' else logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Get project configuration
DEFAULT_PROJECT = os.environ.get("PROJECT_ID", "sis-sandbox-463113")
DEFAULT_DATASET = os.environ.get("BQ_DATASET", "db2_migration")

@app.get("/")
def root():
    return "db2-migration-service up"

@app.get("/health")
def health():
    return jsonify({"status": "healthy", "service": "db2-migration-service"})

@app.get("/bq-health")
def bq_health():
    try:
        client = bigquery.Client(project=DEFAULT_PROJECT)
        rows = list(client.query('SELECT "ok" AS status, CURRENT_TIMESTAMP() ts').result())
        return jsonify({"status": rows[0]["status"], "ts": str(rows[0]["ts"])})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500

@app.get("/tcpcheck")
def tcpcheck():
    try:
        cfg = db_utils.get_secret_payload()
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
        "BQ_STAGING_TABLE": os.environ.get("BQ_STAGING_TABLE", "po_inbound_daily"),
        "BQ_BREAKOUT_STAGING_TABLE": os.environ.get("BQ_BREAKOUT_STAGING_TABLE", "po_breakout_staging")
    }

    # Try to get secret configuration
    try:
        config = db_utils.get_secret_payload()
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
                conn = db_utils.connect_to_db2(config)

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
    """
    Run migration for PO and/or Breakout data.

    Query Parameters:
        query_type: 'po', 'breakout', or 'both' (default: 'both')
        use_rolling_window: 'true' or 'false' (default: 'true')
        mock: 'true' or 'false' (default: 'false')
        use_merge: 'true' or 'false' (default: 'true')
        start_date: YYYY-MM-DD (optional if use_rolling_window=true)
        end_date: YYYY-MM-DD (optional if use_rolling_window=true)
    """
    try:
        # Parse parameters
        query_type = request.args.get("query_type", "both").lower()
        use_rolling_window = request.args.get("use_rolling_window", "true").lower() == "true"
        mock_mode = os.environ.get("USE_MOCK_DB", "0") == "1" or request.args.get("mock", "false").lower() == "true"
        use_merge = request.args.get("use_merge", "true").lower() == "true"

        # Get date range
        if use_rolling_window:
            start_date = (date.today() - timedelta(days=30)).strftime("%Y-%m-%d")
            end_date = (date.today() + timedelta(days=60)).strftime("%Y-%m-%d")
        else:
            yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
            start_date = request.args.get("start_date", request.args.get("date", yesterday))
            end_date = request.args.get("end_date", start_date)

        results = {
            "status": "success",
            "start_date": start_date,
            "end_date": end_date,
            "query_type": query_type,
            "mock_mode": mock_mode,
            "use_rolling_window": use_rolling_window,
            "use_merge": use_merge,
            "timestamp": datetime.now().isoformat(),
            "results": {}
        }

        # Run PO query if requested
        if query_type in ["po", "both"]:
            logger.info(f"Running PO query for date range: {start_date} to {end_date}")
            try:
                # Fetch PO data
                df_po = po_query.fetch_po_data(
                    start_date=start_date,
                    end_date=end_date,
                    mock_mode=mock_mode,
                    use_rolling_window=use_rolling_window
                )

                if df_po.empty:
                    results["results"]["po"] = {
                        "status": "no_data",
                        "message": "No PO data found for the specified date range"
                    }
                else:
                    # Load to BigQuery
                    config = po_query.get_po_config()
                    rows_loaded = bq_utils.load_to_bigquery(df_po, config, use_merge=use_merge)

                    results["results"]["po"] = {
                        "status": "success",
                        "rows_fetched": len(df_po),
                        "rows_loaded": rows_loaded
                    }
                    logger.info(f"PO migration completed. Rows loaded: {rows_loaded}")

            except Exception as e:
                logger.error(f"Error in PO migration: {str(e)}")
                results["results"]["po"] = {
                    "status": "error",
                    "error": str(e)
                }
                results["status"] = "partial_failure"

        # Run Breakout query if requested
        if query_type in ["breakout", "both"]:
            logger.info(f"Running Breakout query for date range: {start_date} to {end_date}")
            try:
                # Fetch Breakout data
                df_breakout = breakout_query.fetch_breakout_data(
                    start_date=start_date,
                    end_date=end_date,
                    mock_mode=mock_mode,
                    use_rolling_window=use_rolling_window
                )

                if df_breakout.empty:
                    results["results"]["breakout"] = {
                        "status": "no_data",
                        "message": "No breakout data found for the specified date range"
                    }
                else:
                    # Load to BigQuery
                    config = breakout_query.get_breakout_config()
                    rows_loaded = bq_utils.load_to_bigquery(df_breakout, config, use_merge=use_merge)

                    results["results"]["breakout"] = {
                        "status": "success",
                        "rows_fetched": len(df_breakout),
                        "rows_loaded": rows_loaded
                    }
                    logger.info(f"Breakout migration completed. Rows loaded: {rows_loaded}")

            except Exception as e:
                logger.error(f"Error in Breakout migration: {str(e)}")
                results["results"]["breakout"] = {
                    "status": "error",
                    "error": str(e)
                }
                results["status"] = "partial_failure" if results["status"] == "success" else "error"

        # Check if any errors occurred
        if all(r.get("status") == "error" for r in results["results"].values()):
            results["status"] = "error"

        return jsonify(results), 500 if results["status"] == "error" else 200

    except Exception as e:
        logger.error(f"Error in migration: {str(e)}")
        import traceback
        logger.error(f"Full traceback:\n{traceback.format_exc()}")

        return jsonify({
            "status": "error",
            "error": str(e),
            "start_date": start_date if 'start_date' in locals() else None,
            "end_date": end_date if 'end_date' in locals() else None
        }), 500

@app.route("/create-tables", methods=["POST"])
def create_tables():
    """Create BigQuery tables if they don't exist"""
    try:
        client = bigquery.Client(project=DEFAULT_PROJECT)
        dataset_id = DEFAULT_DATASET
        created_tables = []

        # Create PO tables
        po_config = po_query.get_po_config()
        po_staging_id = f"{DEFAULT_PROJECT}.{dataset_id}.{po_config['staging_table']}"
        po_final_id = f"{DEFAULT_PROJECT}.{dataset_id}.{po_config['final_table']}"

        for table_id in [po_staging_id, po_final_id]:
            try:
                schema = bq_utils.get_table_schema("po")
                bq_utils.create_table_if_not_exists(client, table_id, schema)
                created_tables.append(table_id)
            except Exception as e:
                logger.error(f"Error creating table {table_id}: {e}")

        # Create Breakout tables
        breakout_config = breakout_query.get_breakout_config()
        breakout_staging_id = f"{DEFAULT_PROJECT}.{dataset_id}.{breakout_config['staging_table']}"
        breakout_final_id = f"{DEFAULT_PROJECT}.{dataset_id}.{breakout_config['final_table']}"

        for table_id in [breakout_staging_id, breakout_final_id]:
            try:
                schema = bq_utils.get_table_schema("breakout")
                bq_utils.create_table_if_not_exists(client, table_id, schema)
                created_tables.append(table_id)
            except Exception as e:
                logger.error(f"Error creating table {table_id}: {e}")

        return jsonify({
            "status": "success",
            "created_tables": created_tables
        })

    except Exception as e:
        logger.error(f"Error creating tables: {str(e)}")
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))