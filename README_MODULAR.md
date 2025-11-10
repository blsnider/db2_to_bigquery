# DB2 to BigQuery Migration - Modular Architecture

## Overview
This project has been refactored into a modular architecture to support multiple DB2 queries with shared utilities and clean separation of concerns.

## Directory Structure
```
db2_to_bigquery/
├── main_new.py              # Flask app orchestrator (rename to main.py when deploying)
├── queries/                 # Query-specific modules
│   ├── __init__.py
│   ├── po_query.py         # Existing PO inbound query
│   └── breakout_query.py   # New breakout quantities query
├── utils/                   # Shared utilities
│   ├── __init__.py
│   ├── db_utils.py         # DB2 connection and query execution
│   └── bq_utils.py         # BigQuery loading and MERGE operations
├── Dockerfile              # Updated with new directories
├── deploy.sh               # Updated with new environment variables
└── requirements.txt        # Dependencies
```

## Key Features

### 1. Two Query Types
- **PO Query**: Original purchase order tracking (Types P & R)
- **Breakout Query**: Child PO breakout quantities (Type C linked to parent POs)

### 2. Shared Utilities
- **db_utils.py**: DB2 connection management, query execution, date range calculation
- **bq_utils.py**: BigQuery data type conversion, staging/final table management, MERGE logic

### 3. API Endpoints

#### Run Migration
```bash
# Run both queries (default)
curl -H "Authorization: Bearer $TOKEN" "${SERVICE_URL}/run?query_type=both"

# Run only PO query
curl -H "Authorization: Bearer $TOKEN" "${SERVICE_URL}/run?query_type=po"

# Run only Breakout query
curl -H "Authorization: Bearer $TOKEN" "${SERVICE_URL}/run?query_type=breakout"

# Run with specific date range
curl -H "Authorization: Bearer $TOKEN" \
  "${SERVICE_URL}/run?query_type=both&use_rolling_window=false&start_date=2025-01-01&end_date=2025-01-31"

# Run in mock mode for testing
curl -H "Authorization: Bearer $TOKEN" "${SERVICE_URL}/run?query_type=both&mock=true"
```

#### Create Tables
```bash
# Create all BigQuery tables
curl -X POST -H "Authorization: Bearer $TOKEN" "${SERVICE_URL}/create-tables"
```

## BigQuery Tables

### PO Query Tables
- **Staging**: `db2_migration.po_inbound_daily` (append-only, recent window)
- **Final**: `db2_migration.po_inbound_final` (deduplicated via MERGE)

### Breakout Query Tables
- **Staging**: `db2_migration.po_breakout_staging` (append-only, recent window)
- **Final**: `db2_migration.po_breakout_final` (deduplicated via MERGE)

## Environment Variables
```bash
# Core settings
PROJECT_ID=sis-sandbox-463113
BQ_DATASET=db2_migration
DB2_SECRET_NAME=IBM_connect

# PO query tables
BQ_STAGING_TABLE=po_inbound_daily
BQ_FINAL_TABLE=po_inbound_final

# Breakout query tables
BQ_BREAKOUT_STAGING_TABLE=po_breakout_staging
BQ_BREAKOUT_FINAL_TABLE=po_breakout_final

# Other settings
USE_MOCK_DB=0  # Set to 1 for testing without DB2
DEBUG=1        # Set to 1 for verbose logging
RECENT_WINDOW_DAYS=14  # Days to consider for MERGE deduplication
```

## Deployment Steps

### 1. Initial Setup
```bash
# Create BigQuery dataset if not exists
bq mk --dataset --location=US sis-sandbox-463113:db2_migration

# Create tables via API
TOKEN=$(gcloud auth print-identity-token)
curl -X POST -H "Authorization: Bearer $TOKEN" \
  "https://db2-migration-service-41815171183.us-central1.run.app/create-tables"
```

### 2. Deploy Service
```bash
# Rename main_new.py to main.py for deployment
mv main_new.py main.py

# Deploy to Cloud Run
./deploy.sh
```

### 3. Test Deployment
```bash
# Test DB2 connection
TOKEN=$(gcloud auth print-identity-token)
curl -H "Authorization: Bearer $TOKEN" \
  "https://db2-migration-service-41815171183.us-central1.run.app/db2-diagnose"

# Test with mock data
curl -H "Authorization: Bearer $TOKEN" \
  "https://db2-migration-service-41815171183.us-central1.run.app/run?mock=true&query_type=both"

# Run actual migration
curl -H "Authorization: Bearer $TOKEN" \
  "https://db2-migration-service-41815171183.us-central1.run.app/run?query_type=both"
```

### 4. Update Scheduler (Optional)
If you want the scheduler to run both queries:
```bash
gcloud scheduler jobs update http db2-migration-nightly \
  --uri="https://db2-migration-service-41815171183.us-central1.run.app/run?query_type=both" \
  --location=us-central1
```

## Adding New Queries

To add a new query to the system:

1. **Create Query Module** (`queries/new_query.py`):
   - Define SQL query constant
   - Implement `fetch_new_data()` function
   - Implement `get_new_config()` function
   - Define `generate_new_record_key()` function

2. **Update main.py**:
   - Import new query module
   - Add handling in `/run` endpoint

3. **Update Environment Variables**:
   - Add table names to Dockerfile
   - Update deploy.sh with new env vars

4. **Create BigQuery Tables**:
   - Add schema to `bq_utils.get_table_schema()`
   - Run `/create-tables` endpoint

## Monitoring

### Check Logs
```bash
gcloud run services logs read db2-migration-service \
  --limit=50 --region=us-central1
```

### Query BigQuery
```bash
# Check PO data
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) as total FROM \`sis-sandbox-463113.db2_migration.po_inbound_final\`"

# Check Breakout data
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) as total FROM \`sis-sandbox-463113.db2_migration.po_breakout_final\`"

# Check recent loads
bq query --use_legacy_sql=false \
  "SELECT DATE(load_timestamp) as load_date, COUNT(*) as records
   FROM \`sis-sandbox-463113.db2_migration.po_breakout_staging\`
   GROUP BY load_date ORDER BY load_date DESC LIMIT 10"
```

## Troubleshooting

### DB2 Connection Issues
```bash
# Run diagnostics
curl -H "Authorization: Bearer $TOKEN" "${SERVICE_URL}/db2-diagnose"

# Check TCP connectivity
curl -H "Authorization: Bearer $TOKEN" "${SERVICE_URL}/tcpcheck?host=scheels.scheelssports.pvt&port=446"
```

### Data Issues
- Check staging tables for raw data
- Verify MERGE logic in final tables
- Review date ranges and rolling window settings
- Check record_key generation for duplicates

### Performance
- Monitor Cloud Run metrics
- Check BigQuery query performance
- Consider partitioning for large tables
- Adjust RECENT_WINDOW_DAYS for MERGE efficiency