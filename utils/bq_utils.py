import os
import logging
from datetime import date, timedelta
from google.cloud import bigquery
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

DEFAULT_PROJECT = os.environ.get("PROJECT_ID", "sis-sandbox-463113")
DEFAULT_DATASET = os.environ.get("BQ_DATASET", "db2_migration")
RECENT_WINDOW_DAYS = int(os.environ.get("RECENT_WINDOW_DAYS", "14"))

def convert_datatypes_for_bq(df, config):
    """Convert DataFrame data types for BigQuery compatibility"""
    logger.info("Converting data types for BigQuery compatibility")

    # Integer columns
    for col in config.get('integer_columns', []):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('Int64')

    # Date columns
    for col in config.get('date_columns', []):
        if col in df.columns and df[col].notna().any():
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

    # String columns
    for col in config.get('string_columns', []):
        if col in df.columns:
            df[col] = df[col].astype(str).replace('None', '')

    # Add timestamp for this batch
    df['load_timestamp'] = pd.Timestamp.now(tz="UTC")

    return df

def cleanup_old_records(client, table_ref, days_back=30, days_forward=60, date_column='expected_date'):
    """Remove records outside the rolling window"""
    today = date.today()
    cutoff_date_past = today - timedelta(days=days_back)
    cutoff_date_future = today + timedelta(days=days_forward)

    cleanup_query = f"""
    DELETE FROM `{table_ref}`
    WHERE {date_column} < DATE('{cutoff_date_past}')
       OR {date_column} > DATE('{cutoff_date_future}')
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

def load_to_bigquery(df, config, use_merge=True):
    """
    Load data to BigQuery with staging and final table strategy

    Args:
        df: DataFrame to load
        config: Configuration dictionary with table names and column mappings
        use_merge: Whether to use MERGE strategy for final table
    """
    client = bigquery.Client(project=DEFAULT_PROJECT)

    staging_ref = f"{client.project}.{DEFAULT_DATASET}.{config['staging_table']}"
    final_ref = f"{client.project}.{DEFAULT_DATASET}.{config['final_table']}"

    # Convert data types
    df = convert_datatypes_for_bq(df, config)

    # Ensure record_key exists
    if 'record_key' not in df.columns and 'generate_record_key' in config:
        logger.info("Generating record_key for deduplication")
        df['record_key'] = df.apply(config['generate_record_key'], axis=1)

    # 1) Append to STAGING
    logger.info(f"Appending {len(df)} rows to staging: {staging_ref}")
    load_cfg = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    job = client.load_table_from_dataframe(df, staging_ref, job_config=load_cfg)
    job.result()

    if not use_merge:
        logger.info("use_merge=False → Skipping MERGE to final; staging append only")
        return len(df)

    # 2) MERGE into FINAL using latest per record_key
    merge_to_final(client, staging_ref, final_ref, config)

    # 3) Housekeeping: cleanup old records from staging
    if config.get('cleanup_staging', True):
        try:
            deleted = cleanup_old_records(
                client, staging_ref,
                date_column=config.get('date_column', 'expected_date')
            )
            logger.info(f"Staging cleanup deleted {deleted} old rows.")
        except Exception as e:
            logger.warning(f"Staging cleanup skipped/failed: {e}")

    # 4) Cleanup old records from final table (remove phantom/ghost data)
    if config.get('cleanup_final', True):
        try:
            deleted_final = cleanup_old_records(
                client, final_ref,
                days_back=config.get('days_back', 30),
                days_forward=config.get('days_forward', 60),
                date_column=config.get('date_column', 'expected_date')
            )
            logger.info(f"Final table cleanup deleted {deleted_final} old rows outside rolling window.")
        except Exception as e:
            logger.warning(f"Final table cleanup skipped/failed: {e}")

    return len(df)

def merge_to_final(client, staging_ref, final_ref, config):
    """Execute MERGE from staging to final table"""
    logger.info(f"Merging latest per record_key from last {RECENT_WINDOW_DAYS} days into final: {final_ref}")

    # Get column list for MERGE statement
    columns = config.get('columns', [])

    # Build UPDATE SET clause
    update_clause = ',\n      '.join([f"{col} = S.{col}" for col in columns if col != 'record_key'])

    # Build INSERT columns
    insert_columns = ', '.join(columns)

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
            ORDER BY {config.get('merge_order_by', 'load_timestamp DESC')}
         ) AS rn
        FROM recent r
     )
    SELECT * EXCEPT(rn)
     FROM ranked
     WHERE rn = 1
    ) S
    ON T.record_key = S.record_key
    WHEN MATCHED THEN UPDATE SET
      {update_clause}
    WHEN NOT MATCHED THEN
      INSERT ROW;
    """

    merge_job = client.query(merge_sql)
    merge_job.result()
    logger.info(f"MERGE complete (affected rows reported by DML counters may be null for MERGE).")

def create_table_if_not_exists(client, table_id, schema):
    """Create a BigQuery table if it doesn't exist"""
    try:
        client.get_table(table_id)
        logger.info(f"Table {table_id} already exists")
    except Exception:
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)
        logger.info(f"Created table {table_id}")

def get_table_schema(table_type):
    """Get BigQuery table schema based on table type"""
    if table_type == "po":
        return [
            bigquery.SchemaField("store", "STRING"),
            bigquery.SchemaField("vendor", "STRING"),
            bigquery.SchemaField("ss", "STRING"),
            bigquery.SchemaField("type", "STRING"),
            bigquery.SchemaField("po_code", "STRING"),
            bigquery.SchemaField("po", "STRING"),
            bigquery.SchemaField("po_man", "STRING"),
            bigquery.SchemaField("sku", "STRING"),
            bigquery.SchemaField("style", "STRING"),
            bigquery.SchemaField("skudesc1", "STRING"),
            bigquery.SchemaField("expected_date", "DATE"),
            bigquery.SchemaField("cancel_date", "DATE"),
            bigquery.SchemaField("ooqty", "INT64"),
            bigquery.SchemaField("rcvdqty", "INT64"),
            bigquery.SchemaField("breakoutqty", "INT64"),
            bigquery.SchemaField("allocated", "INT64"),
            bigquery.SchemaField("unitinvc_per_sku", "INT64"),
            bigquery.SchemaField("shipped_sku", "STRING"),
            bigquery.SchemaField("shipped_qty", "INT64"),
            bigquery.SchemaField("latest_datercv", "DATE"),
            bigquery.SchemaField("shipnotice_date", "DATE"),
            bigquery.SchemaField("record_key", "STRING"),
            bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
        ]
    elif table_type == "breakout":
        return [
            bigquery.SchemaField("parent_po_man", "STRING"),
            bigquery.SchemaField("ss", "STRING"),
            bigquery.SchemaField("vendor", "STRING"),
            bigquery.SchemaField("sku", "STRING"),
            bigquery.SchemaField("style", "STRING"),
            bigquery.SchemaField("skudesc", "STRING"),
            bigquery.SchemaField("ship_date", "DATE"),
            bigquery.SchemaField("exp_date", "DATE"),
            bigquery.SchemaField("store", "STRING"),
            bigquery.SchemaField("breakout_qty", "INT64"),
            bigquery.SchemaField("record_key", "STRING"),
            bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
        ]
    else:
        raise ValueError(f"Unknown table type: {table_type}")