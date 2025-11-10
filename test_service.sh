#!/bin/bash

# Test script for DB2 to BigQuery migration service

SERVICE_URL="https://db2-migration-service-41815171183.us-central1.run.app"

# Get authentication token
echo "Getting authentication token..."
TOKEN=$(gcloud auth print-identity-token)

echo "================================="
echo "Testing DB2 Migration Service"
echo "================================="
echo ""

# Test health endpoint
echo "1. Testing /health endpoint..."
echo "----------------------------"
curl -s -H "Authorization: Bearer $TOKEN" "${SERVICE_URL}/health" | python3 -m json.tool
echo ""

# Test BigQuery connectivity
echo "2. Testing /bq-health endpoint..."
echo "--------------------------------"
curl -s -H "Authorization: Bearer $TOKEN" "${SERVICE_URL}/bq-health" | python3 -m json.tool
echo ""

# Test TCP check (optional - only if you have DB2 connectivity)
echo "3. Testing /tcpcheck endpoint (example)..."
echo "---------------------------------------"
echo "Note: This will fail unless you have proper VPC connector and DB2 access configured"
curl -s -H "Authorization: Bearer $TOKEN" "${SERVICE_URL}/tcpcheck?host=your-db2-host&port=446" | python3 -m json.tool
echo ""

# Test migration with mock data
echo "4. Testing /run with mock data..."
echo "---------------------------------"
curl -s -H "Authorization: Bearer $TOKEN" "${SERVICE_URL}/run?mock=true&start_date=2025-07-01&end_date=2025-07-01" | python3 -m json.tool
echo ""

# Test migration for a specific date range (will use real DB2 if configured)
echo "5. Testing /run for date range (real data)..."
echo "-------------------------------------------"
echo "This will attempt to connect to DB2. It may fail if DB2 credentials/connectivity are not configured."
echo "Testing for July 1-3, 2025..."
curl -s -H "Authorization: Bearer $TOKEN" "${SERVICE_URL}/run?start_date=2025-07-01&end_date=2025-07-03" | python3 -m json.tool
echo ""

echo "================================="
echo "Test completed!"
echo "================================="
echo ""
echo "Notes:"
echo "- If step 5 fails with DB2 connection error, you need to:"
echo "  1. Ensure Secret Manager has correct DB2 credentials"
echo "  2. Configure VPC connector if DB2 is on-premise"
echo "  3. Verify firewall rules allow connection"
echo ""
echo "To manually trigger the nightly job:"
echo "gcloud scheduler jobs run db2-migration-nightly --location us-central1"