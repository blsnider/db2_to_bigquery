# DB2 to BigQuery Migration Pipeline - Summary Overview

## Project Details
- **Project ID:** sis-sandbox-463113
- **Cloud Run Service:** db2-migration-service
- **Service URL:** https://db2-migration-service-41815171183.us-central1.run.app
- **BigQuery Dataset:** db2_migration
- **BigQuery Table:** po_inbound_daily
- **Scheduled Job:** db2-migration-nightly (runs daily at 2:15 AM CST)

## Database Connection
### DB2 Configuration
- **Host:** scheels.scheelssports.pvt
- **Port:** 446
- **Database:** SCHEELS
- **User:** QPGMR
- **Password:** Stored in Google Secret Manager as `IBM_connect`
- **Connection Status:** ✅ Working

## Data Loading Summary (as of August 27, 2025)
- **Total Records Loaded:** 141,934 records
- **Date Range:** January 1, 2024 to December 31, 2025
- **Unique Purchase Orders:** 12,445
- **Stores Included:** 110, 114, 614, 116, 616, 118, 618

### Data Distribution by Period
| Period | Records Loaded |
|--------|---------------|
| 2024 Full Year | 66,509 |
| 2025 Jan-Jun | 40,243 |
| 2025 July | 11,552 |
| 2025 August | 3,437 |
| 2025 September | 5,257 |
| 2025 October | 10,767 |
| 2025 November | 1,303 |
| 2025 December | 2,833 |
| **Total** | **141,934** |

## Query Details
The pipeline queries DB2 for purchase order data with the following filters:
- **Stores:** 110, 114, 614, 116, 616, 118, 618
- **PO Codes:** O (Open), D (Drop Ship)
- **PO Types:** P (Purchase), R (Return)
- **Date Filter:** Based on EXPECTED_DATE field

### Main Tables Queried
- ITMDATADDL.POHDR (PO Headers)
- ITMDATADDL.PODTL (PO Details)
- ITMDATADDL.SKUFILE (SKU Information)
- ITMDATADDL.VENDMAIN (Vendor Information)
- ITM.DATA.EDIINVDTL (Invoice Details)
- ITM.DATA.ASNHDR/ASNDTL (Advanced Shipping Notice)
- ITMDATADDL.PURHSTHDR (Purchase History)

## Issues Resolved

### 1. Authentication Issues
- **Problem:** Initial credentials (bsnidesc/bsnidesc) were invalid
- **Solution:** Updated to correct credentials (QPGMR/JOBRUNR) in Google Secret Manager

### 2. Data Type Mismatches
- **Problem:** DB2 numeric fields were coming as strings, causing BigQuery load failures
- **Solution:** Added data type conversion in Python code:
  - Integer columns: Converted to Int64 with proper null handling
  - Date columns: Parsed and converted to date format
  - String columns: Ensured proper string formatting

### 3. Limited Data Loading
- **Problem:** Only 33 records initially loaded vs. hundreds expected
- **Root Cause:** Scheduled job only queries yesterday's data by default
- **Solution:** Performed historical load for 2024-2025 data; daily job continues incremental loading

## Operational Commands

### Test DB2 Connection Health
```bash
TOKEN=$(gcloud auth print-identity-token)
curl -H "Authorization: Bearer $TOKEN" \
  https://db2-migration-service-41815171183.us-central1.run.app/db2-diagnose
```

### Manually Trigger Scheduled Job
```bash
gcloud scheduler jobs run db2-migration-nightly --location=us-central1
```

### Run Migration for Specific Date Range
```bash
TOKEN=$(gcloud auth print-identity-token)
curl -H "Authorization: Bearer $TOKEN" \
  "https://db2-migration-service-41815171183.us-central1.run.app/run?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD"
```

### Check Cloud Run Logs
```bash
gcloud run services logs read db2-migration-service --limit=20 --region=us-central1
```

### Query BigQuery Data
```bash
# Check total records
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) as total FROM \`sis-sandbox-463113.db2_migration.po_inbound_daily\`"

# Check records by date
bq query --use_legacy_sql=false \
  "SELECT DATE(expected_date) as date, COUNT(*) as records 
   FROM \`sis-sandbox-463113.db2_migration.po_inbound_daily\` 
   GROUP BY date ORDER BY date DESC LIMIT 10"
```

### Update DB2 Credentials
```bash
echo '{
  "host": "scheels.scheelssports.pvt",
  "port": 446,
  "database": "SCHEELS",
  "user": "NEW_USER",
  "password": "NEW_PASSWORD"
}' | gcloud secrets versions add IBM_connect --data-file=-
```

### Deploy Service Updates
```bash
gcloud run deploy db2-migration-service \
  --source . \
  --region us-central1 \
  --no-allow-unauthenticated \
  --set-env-vars "DEBUG=1,USE_MOCK_DB=0" \
  --quiet
```

## Architecture Overview

```
DB2 Database (IBM AS/400)
    ↓
Cloud Scheduler (Daily 2:15 AM CST)
    ↓
Cloud Run Service (db2-migration-service)
    ↓
BigQuery Table (db2_migration.po_inbound_daily)
```

## Service Features

1. **Daily Incremental Loading:** Automatically loads previous day's data
2. **Manual Date Range Loading:** Supports custom date ranges via API
3. **Health Monitoring:** `/db2-diagnose` endpoint for connection testing
4. **Data Type Handling:** Automatic conversion between DB2 and BigQuery types
5. **Error Logging:** Detailed logging with DEBUG mode enabled
6. **Authentication:** Secured with Google Cloud IAM

## Performance Metrics

- **Average Load Time:** ~3-5 seconds per 1,000 records
- **Largest Single Load:** 66,509 records (2024 full year) in ~6 minutes
- **Daily Incremental Load:** Typically < 1,000 records in < 30 seconds

## Maintenance Notes

1. **Scheduled Job:** Runs daily at 2:15 AM CST for previous day's data
2. **Data Retention:** All historical data preserved (append-only mode)
3. **Error Handling:** Service logs all DB2 connection and query errors
4. **Monitoring:** Check Cloud Run logs and BigQuery table for daily updates

## Future Considerations

1. **Data Archival:** Consider partitioning BigQuery table by date for better performance
2. **Alerting:** Set up monitoring alerts for failed jobs
3. **Deduplication:** May need logic to handle duplicate records if re-running date ranges
4. **Performance:** For very large date ranges, consider batch processing

## Contact & Support

- **Service Logs:** Available in Cloud Run console
- **Secret Management:** IBM_connect secret in Secret Manager
- **Scheduler Configuration:** Cloud Scheduler console (us-central1 region)

---

*Document Generated: August 27, 2025*
*Last Data Load: 141,934 records (2024-01-01 to 2025-12-31)*