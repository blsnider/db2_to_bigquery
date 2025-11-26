import os
import sys
import logging
import pandas as pd
from datetime import date, timedelta

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import db_utils

logger = logging.getLogger(__name__)

# Configuration for Breakout query
DEFAULT_STAGING = os.environ.get("BQ_BREAKOUT_STAGING_TABLE", "po_breakout_staging")
DEFAULT_FINAL = os.environ.get("BQ_BREAKOUT_FINAL_TABLE", "po_breakout_final")

BREAKOUT_QUERY_SQL = """
WITH ParentPOs AS (
    SELECT DISTINCT
        PH.PO#,
        PH."PO#MAN",
        PH.POCODE,
        PT.TYPE,
        PH.DATEEXP,
        PH.POSTR#
    FROM "ITM.DATA".POHDR PH
    JOIN ITMDATADDL.POTYPEF PT  ON PT.PO = PH.PO#
    JOIN "ITMDATADDL".PODTL PD  ON PD.PO# = PT.PO
    WHERE PH.POSTR# IN ('118','618','114','614','116','616','110')
      AND PH.DATEEXP BETWEEN :START_DATE AND :END_DATE
      AND PH.POCODE IN ('O','D')
      AND PT.TYPE = 'P'
      AND PH."PO#MAN" IS NOT NULL AND TRIM(PH."PO#MAN") <> ''
),

-- Distinct parent stores per PO#MAN (avoid duplicates in listagg)
ParentStores AS (
    SELECT DISTINCT "PO#MAN", POSTR#
    FROM ParentPOs
),

-- Parent Exp Date + concatenated Parent Stores per PO#MAN
ParentByMan AS (
    SELECT
        E."PO#MAN",
        E.ParentExpDate,
        S.ParentStores
    FROM (
        SELECT "PO#MAN", MAX(DATEEXP) AS ParentExpDate
        FROM ParentPOs
        GROUP BY "PO#MAN"
    ) E
    LEFT JOIN (
        SELECT "PO#MAN",
               LISTAGG(CAST(POSTR# AS VARCHAR(10)), ', ') WITHIN GROUP (ORDER BY POSTR#) AS ParentStores
        FROM ParentStores
        GROUP BY "PO#MAN"
    ) S ON S."PO#MAN" = E."PO#MAN"
),

-- Children (breakouts): no DATEEXP filter; only those tied to Parent PO#MANs
ChildPOs AS (
    SELECT
        PH.PO#MAN,
        PH.POSTR#,
        SK.SS#,
        VM.VENDNAME AS VENDORNAME,
        SK.SKU#,
        SK.STYLE,
        SK.SKUDESC1,
        PH.DATESHIP,
        PH.DATEEXP,
        SUM(PD.OOUNIT) AS OOUNIT
    FROM "ITM.DATA".POHDR PH
    JOIN "ITM.DATA".PODTL PD       ON PD.PO# = PH.PO#
    JOIN "ITM.DATA".SKUFILE SK     ON SK.SKU# = PD.SKU#
    JOIN ITMDATADDL.VENDMAIN VM    ON SK.VEND#1 = VM.VEND#
    JOIN ITMDATADDL.POTYPEF PT     ON PT.PO = PH.PO#
    WHERE PT.TYPE = 'C'
      AND PH."PO#MAN" IS NOT NULL AND TRIM(PH."PO#MAN") <> ''
      AND EXISTS (SELECT 1 FROM ParentPOs P WHERE P."PO#MAN" = PH."PO#MAN")
    GROUP BY PH.PO#MAN, PH.PO#, PH.POSTR#, SK.SS#, VM.VENDNAME,
             SK.SKU#, SK.STYLE, SK.SKUDESC1, PH.DATESHIP, PH.DATEEXP
)

-- Pivot by store number + include parent exp date and parent stores
SELECT
    A.PO#MAN     AS "PO#MAN",
    A.SS#        AS "SS#",
    A.VENDORNAME AS "VENDOR",
    A.SKU#       AS "SKU#",
    A.STYLE      AS "STYLE",
    A.SKUDESC1   AS "SKUDESC",
    A.DATESHIP   AS "SHIP_DATE",
    A.DATEEXP    AS "EXP_DATE",
    PBM.ParentExpDate AS "Parent Exp Date",
    PBM.ParentStores  AS "Parent Stores",

    COALESCE(MAX(CASE WHEN A.POSTR# = 6   THEN A.OOUNIT END), 0) AS DS,
    COALESCE(MAX(CASE WHEN A.POSTR# = 16  THEN A.OOUNIT END), 0) AS SH,
    COALESCE(MAX(CASE WHEN A.POSTR# = 20  THEN A.OOUNIT END), 0) AS MO,
    COALESCE(MAX(CASE WHEN A.POSTR# = 22  THEN A.OOUNIT END), 0) AS CS,
    COALESCE(MAX(CASE WHEN A.POSTR# = 24  THEN A.OOUNIT END), 0) AS MI,
    COALESCE(MAX(CASE WHEN A.POSTR# = 26  THEN A.OOUNIT END), 0) AS CH,
    COALESCE(MAX(CASE WHEN A.POSTR# = 28  THEN A.OOUNIT END), 0) AS TU,
    COALESCE(MAX(CASE WHEN A.POSTR# = 30  THEN A.OOUNIT END), 0) AS MB,
    COALESCE(MAX(CASE WHEN A.POSTR# = 38  THEN A.OOUNIT END), 0) AS WI,
    COALESCE(MAX(CASE WHEN A.POSTR# = 40  THEN A.OOUNIT END), 0) AS EA,
    COALESCE(MAX(CASE WHEN A.POSTR# = 44  THEN A.OOUNIT END), 0) AS IC,
    COALESCE(MAX(CASE WHEN A.POSTR# = 48  THEN A.OOUNIT END), 0) AS SF,
    COALESCE(MAX(CASE WHEN A.POSTR# = 50  THEN A.OOUNIT END), 0) AS SM,
    COALESCE(MAX(CASE WHEN A.POSTR# = 54  THEN A.OOUNIT END), 0) AS AP,
    COALESCE(MAX(CASE WHEN A.POSTR# = 56  THEN A.OOUNIT END), 0) AS SC,
    COALESCE(MAX(CASE WHEN A.POSTR# = 58  THEN A.OOUNIT END), 0) AS OM,
    COALESCE(MAX(CASE WHEN A.POSTR# = 60  THEN A.OOUNIT END), 0) AS DM,
    COALESCE(MAX(CASE WHEN A.POSTR# = 62  THEN A.OOUNIT END), 0) AS RH,
    COALESCE(MAX(CASE WHEN A.POSTR# = 64  THEN A.OOUNIT END), 0) AS FA,
    COALESCE(MAX(CASE WHEN A.POSTR# = 70  THEN A.OOUNIT END), 0) AS GF,
    COALESCE(MAX(CASE WHEN A.POSTR# = 72  THEN A.OOUNIT END), 0) AS KI,
    COALESCE(MAX(CASE WHEN A.POSTR# = 74  THEN A.OOUNIT END), 0) AS RS,
    COALESCE(MAX(CASE WHEN A.POSTR# = 76  THEN A.OOUNIT END), 0) AS RC,
    COALESCE(MAX(CASE WHEN A.POSTR# = 78  THEN A.OOUNIT END), 0) AS SP,
    COALESCE(MAX(CASE WHEN A.POSTR# = 80  THEN A.OOUNIT END), 0) AS SS,
    COALESCE(MAX(CASE WHEN A.POSTR# = 82  THEN A.OOUNIT END), 0) AS CF,
    COALESCE(MAX(CASE WHEN A.POSTR# = 84  THEN A.OOUNIT END), 0) AS GK,
    COALESCE(MAX(CASE WHEN A.POSTR# = 86  THEN A.OOUNIT END), 0) AS BL,
    COALESCE(MAX(CASE WHEN A.POSTR# = 88  THEN A.OOUNIT END), 0) AS OP,
    COALESCE(MAX(CASE WHEN A.POSTR# = 90  THEN A.OOUNIT END), 0) AS RO,
    COALESCE(MAX(CASE WHEN A.POSTR# = 92  THEN A.OOUNIT END), 0) AS JO,
    COALESCE(MAX(CASE WHEN A.POSTR# = 94  THEN A.OOUNIT END), 0) AS LI,
    COALESCE(MAX(CASE WHEN A.POSTR# = 96  THEN A.OOUNIT END), 0) AS TC,
    COALESCE(MAX(CASE WHEN A.POSTR# = 98  THEN A.OOUNIT END), 0) AS EN,
    COALESCE(MAX(CASE WHEN A.POSTR# = 110 THEN A.OOUNIT END), 0) AS SD,
    COALESCE(MAX(CASE WHEN A.POSTR# = 114 THEN A.OOUNIT END), 0) AS FFC,
    COALESCE(MAX(CASE WHEN A.POSTR# = 116 THEN A.OOUNIT END), 0) AS IP,
    COALESCE(MAX(CASE WHEN A.POSTR# = 118 THEN A.OOUNIT END), 0) AS "Sidney",
    COALESCE(MAX(CASE WHEN A.POSTR# = 614 THEN A.OOUNIT END), 0) AS "FFC WEB",
    COALESCE(MAX(CASE WHEN A.POSTR# = 616 THEN A.OOUNIT END), 0) AS "IP WEB",
    COALESCE(MAX(CASE WHEN A.POSTR# = 618 THEN A.OOUNIT END), 0) AS "Sidney WEB"
FROM ChildPOs A
LEFT JOIN ParentByMan PBM
  ON PBM."PO#MAN" = A.PO#MAN
GROUP BY
    A.PO#MAN, A.SS#, A.VENDORNAME, A.SKU#, A.STYLE,
    A.SKUDESC1, A.DATEEXP, A.DATESHIP, PBM.ParentExpDate, PBM.ParentStores
ORDER BY A.PO#MAN ASC, A.SS# ASC, A.VENDORNAME ASC, A.SKU# ASC
FOR READ ONLY WITH UR
"""

def fetch_breakout_data(start_date=None, end_date=None, mock_mode=False, use_rolling_window=True):
    """
    Fetch breakout data from DB2.

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
    df = db_utils.execute_query(BREAKOUT_QUERY_SQL, start_date, end_date)

    # Rename columns to match BigQuery schema (remove special characters)
    if not df.empty:
        logger.info("Renaming columns for BigQuery compatibility")
        # First rename the special character columns
        df = df.rename(columns={
            'po#man': 'po_man',
            'ss#': 'ss_num',
            'vendor': 'vendor',
            'sku#': 'sku_num',
            'style': 'style',
            'skudesc': 'skudesc',
            'ship_date': 'ship_date',
            'exp_date': 'exp_date',
            'parent exp date': 'parent_exp_date',
            'parent stores': 'parent_stores',
            'ffc web': 'FFC_WEB',
            'ip web': 'IP_WEB',
            'sidney web': 'Sidney_WEB'
        })

        # Then rename store columns to uppercase to match BigQuery schema
        store_rename = {
            'ds': 'DS', 'sh': 'SH', 'mo': 'MO', 'cs': 'CS', 'mi': 'MI', 'ch': 'CH',
            'tu': 'TU', 'mb': 'MB', 'wi': 'WI', 'ea': 'EA', 'ic': 'IC', 'sf': 'SF',
            'sm': 'SM', 'ap': 'AP', 'sc': 'SC', 'om': 'OM', 'dm': 'DM', 'rh': 'RH',
            'fa': 'FA', 'gf': 'GF', 'ki': 'KI', 'rs': 'RS', 'rc': 'RC', 'sp': 'SP',
            'ss': 'SS', 'cf': 'CF', 'gk': 'GK', 'bl': 'BL', 'op': 'OP', 'ro': 'RO',
            'jo': 'JO', 'li': 'LI', 'tc': 'TC', 'en': 'EN', 'sd': 'SD', 'ffc': 'FFC',
            'ip': 'IP', 'sidney': 'Sidney'
        }
        df = df.rename(columns=store_rename)

        # Generate record_key
        logger.info("Generating record_key for breakout data")
        df['record_key'] = df.apply(generate_breakout_record_key, axis=1)

    return df

def get_mock_data(start_date, end_date):
    """Generate mock data for testing"""
    logger.info(f"Generating mock breakout data for date range: {start_date} to {end_date}")
    today = date.today()
    # Create pivoted format matching new query structure with BQ-compatible column names
    data = pd.DataFrame({
        'po_man': ['PM001', 'PM002', 'PM003'],
        'ss_num': ['SS1', 'SS2', 'SS3'],
        'vendor': ['VENDOR1', 'VENDOR2', 'VENDOR3'],
        'sku_num': ['SKU001', 'SKU002', 'SKU003'],
        'style': ['STY001', 'STY002', 'STY003'],
        'skudesc': ['Child Desc 1', 'Child Desc 2', 'Child Desc 3'],
        'ship_date': [today - timedelta(days=5)] * 3,
        'exp_date': [today, today + timedelta(days=1), today + timedelta(days=2)],
        'parent_exp_date': [today, today + timedelta(days=1), today + timedelta(days=2)],
        'parent_stores': ['110, 114', '110, 614', '116'],
        # Store columns (all defaulting to 0 except a few with values) - uppercase to match BQ schema
        'DS': [0, 0, 0], 'SH': [0, 0, 0], 'MO': [0, 0, 0], 'CS': [0, 0, 0],
        'MI': [0, 0, 0], 'CH': [0, 0, 0], 'TU': [0, 0, 0], 'MB': [0, 0, 0],
        'WI': [0, 0, 0], 'EA': [0, 0, 0], 'IC': [0, 0, 0], 'SF': [0, 0, 0],
        'SM': [0, 0, 0], 'AP': [0, 0, 0], 'SC': [0, 0, 0], 'OM': [0, 0, 0],
        'DM': [0, 0, 0], 'RH': [0, 0, 0], 'FA': [0, 0, 0], 'GF': [0, 0, 0],
        'KI': [0, 0, 0], 'RS': [0, 0, 0], 'RC': [0, 0, 0], 'SP': [0, 0, 0],
        'SS': [0, 0, 0], 'CF': [0, 0, 0], 'GK': [0, 0, 0], 'BL': [0, 0, 0],
        'OP': [0, 0, 0], 'RO': [0, 0, 0], 'JO': [0, 0, 0], 'LI': [0, 0, 0],
        'TC': [0, 0, 0], 'EN': [0, 0, 0],
        'SD': [50, 100, 0],  # Store 110
        'FFC': [75, 0, 0],   # Store 114
        'IP': [0, 0, 150],   # Store 116
        'Sidney': [0, 0, 0], # Store 118
        'FFC_WEB': [0, 125, 0], # Store 614
        'IP_WEB': [0, 0, 0],    # Store 616
        'Sidney_WEB': [0, 0, 0], # Store 618
        'record_key': [
            f"PM001_SKU001_{today.strftime('%Y%m%d')}",
            f"PM002_SKU002_{(today + timedelta(days=1)).strftime('%Y%m%d')}",
            f"PM003_SKU003_{(today + timedelta(days=2)).strftime('%Y%m%d')}"
        ]
    })
    return data

def generate_breakout_record_key(row):
    """Generate unique record key for breakout data"""
    po_man = row.get('po_man', '')
    sku = row.get('sku_num', '')
    exp_date = row.get('exp_date')
    if pd.notna(exp_date):
        exp_date_str = exp_date.strftime('%Y%m%d') if hasattr(exp_date, 'strftime') else str(exp_date).replace('-', '')
    else:
        exp_date_str = 'NULL'
    return f"{po_man}_{sku}_{exp_date_str}"

def get_breakout_config():
    """Return configuration for breakout query"""
    # Define all store columns
    store_columns = [
        'DS', 'SH', 'MO', 'CS', 'MI', 'CH', 'TU', 'MB', 'WI', 'EA', 'IC', 'SF',
        'SM', 'AP', 'SC', 'OM', 'DM', 'RH', 'FA', 'GF', 'KI', 'RS', 'RC', 'SP',
        'SS', 'CF', 'GK', 'BL', 'OP', 'RO', 'JO', 'LI', 'TC', 'EN', 'SD', 'FFC',
        'IP', 'Sidney', 'FFC WEB', 'IP WEB', 'Sidney WEB'
    ]

    # Update store columns to match BQ naming (uppercase to match BigQuery schema)
    store_columns = [
        'DS', 'SH', 'MO', 'CS', 'MI', 'CH', 'TU', 'MB', 'WI', 'EA', 'IC', 'SF',
        'SM', 'AP', 'SC', 'OM', 'DM', 'RH', 'FA', 'GF', 'KI', 'RS', 'RC', 'SP',
        'SS', 'CF', 'GK', 'BL', 'OP', 'RO', 'JO', 'LI', 'TC', 'EN', 'SD', 'FFC',
        'IP', 'Sidney', 'FFC_WEB', 'IP_WEB', 'Sidney_WEB'
    ]

    return {
        'staging_table': DEFAULT_STAGING,
        'final_table': DEFAULT_FINAL,
        'integer_columns': store_columns,  # All store columns are integers
        'date_columns': ['ship_date', 'exp_date', 'parent_exp_date'],
        'string_columns': ['po_man', 'ss_num', 'vendor', 'sku_num', 'style', 'skudesc', 'parent_stores', 'record_key'],
        'columns': ['po_man', 'ss_num', 'vendor', 'sku_num', 'style', 'skudesc', 'ship_date', 'exp_date',
                   'parent_exp_date', 'parent_stores'] + store_columns + ['record_key', 'load_timestamp'],
        'generate_record_key': generate_breakout_record_key,
        'merge_order_by': 'load_timestamp DESC, exp_date DESC',
        'date_column': 'exp_date',
        'days_back': 30,
        'days_forward': 60,
        'cleanup_staging': True,
        'cleanup_final': True,
        'cleanup_stale': True
    }