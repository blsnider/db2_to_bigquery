import os
import sys
import logging
import pandas as pd
from datetime import date, timedelta

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import db_utils

logger = logging.getLogger(__name__)

# Configuration for PO query
DEFAULT_STAGING = os.environ.get("BQ_STAGING_TABLE", "po_inbound_daily")
DEFAULT_FINAL = os.environ.get("BQ_FINAL_TABLE", "po_inbound_final")

PO_QUERY_SQL = """
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
      AND PH.DATEEXP BETWEEN :START_DATE AND :END_DATE
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
FOR READ ONLY WITH UR
"""

def fetch_po_data(start_date=None, end_date=None, mock_mode=False, use_rolling_window=True):
    """
    Fetch PO data from DB2.

    Args:
        start_date: Start date for the query
        end_date: End date for the query
        mock_mode: If True, return mock data instead of querying DB2
        use_rolling_window: If True, use a rolling window of -30 to +60 days from today
    """
    if mock_mode:
        return get_mock_data(start_date, end_date)

    # Calculate date range
    start_date, end_date = db_utils.get_date_range(use_rolling_window, start_date, end_date)

    # Execute query
    df = db_utils.execute_query(PO_QUERY_SQL, start_date, end_date)

    # Generate record_key if not present (backward compatibility)
    if not df.empty and 'record_key' not in df.columns:
        logger.info("Generating record_key for backward compatibility")
        df['record_key'] = df.apply(generate_po_record_key, axis=1)

    return df

def get_mock_data(start_date, end_date):
    """Generate mock data for testing"""
    logger.info(f"Generating mock PO data for date range: {start_date} to {end_date}")
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

def generate_po_record_key(row):
    """Generate unique record key for PO data"""
    po = row.get('po', '')
    sku = row.get('sku', '')
    exp_date = row.get('expected_date')
    if pd.notna(exp_date):
        exp_date_str = exp_date.strftime('%Y%m%d') if hasattr(exp_date, 'strftime') else str(exp_date).replace('-', '')
    else:
        exp_date_str = 'NULL'
    return f"{po}_{sku}_{exp_date_str}"

def get_po_config():
    """Return configuration for PO query"""
    return {
        'staging_table': DEFAULT_STAGING,
        'final_table': DEFAULT_FINAL,
        'integer_columns': ['ooqty', 'rcvdqty', 'breakoutqty', 'allocated', 'unitinvc_per_sku', 'shipped_qty'],
        'date_columns': ['expected_date', 'cancel_date', 'latest_datercv', 'shipnotice_date'],
        'string_columns': ['store', 'vendor', 'ss', 'type', 'po_code', 'po', 'po_man',
                          'sku', 'style', 'skudesc1', 'shipped_sku', 'record_key'],
        'columns': ['store', 'vendor', 'ss', 'type', 'po_code', 'po', 'po_man', 'sku', 'style',
                   'skudesc1', 'expected_date', 'cancel_date', 'ooqty', 'rcvdqty', 'breakoutqty',
                   'allocated', 'unitinvc_per_sku', 'shipped_sku', 'shipped_qty', 'latest_datercv',
                   'shipnotice_date', 'record_key', 'load_timestamp'],
        'generate_record_key': generate_po_record_key,
        'merge_order_by': 'load_timestamp DESC, shipnotice_date DESC, latest_datercv DESC, rcvdqty DESC, ooqty DESC',
        'date_column': 'expected_date',
        'cleanup_staging': True
    }