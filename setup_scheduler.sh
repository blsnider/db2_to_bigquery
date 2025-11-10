#!/bin/bash

# Setup Cloud Scheduler for nightly DB2 to BigQuery migration

PROJECT_ID="sis-sandbox-463113"
REGION="us-central1"
SERVICE_NAME="db2-bigquery-migration"
SCHEDULER_SA="db2-migration-sa@${PROJECT_ID}.iam.gserviceaccount.com"
JOB_NAME="db2-bigquery-nightly-migration"

# Get the service URL
SERVICE_URL=$(gcloud run services describe ${SERVICE_NAME} --region ${REGION} --format='value(status.url)')

if [ -z "$SERVICE_URL" ]; then
    echo "Error: Cloud Run service not found. Please deploy the service first using ./deploy.sh"
    exit 1
fi

echo "Setting up Cloud Scheduler for nightly migration..."

# Grant Cloud Run Invoker permission to the scheduler service account
echo "Granting Cloud Run Invoker permission..."
gcloud run services add-iam-policy-binding ${SERVICE_NAME} \
    --region ${REGION} \
    --member="serviceAccount:${SCHEDULER_SA}" \
    --role="roles/run.invoker"

# Create the scheduler job to run every night at 2:15 AM Chicago time
# It will fetch data for the previous day
echo "Creating Cloud Scheduler job..."
gcloud scheduler jobs create http ${JOB_NAME} \
    --schedule="15 2 * * *" \
    --time-zone="America/Chicago" \
    --uri="${SERVICE_URL}/run" \
    --http-method=GET \
    --oidc-service-account-email=${SCHEDULER_SA} \
    --location=${REGION} \
    --description="Nightly DB2 to BigQuery migration for PO data"

echo ""
echo "Cloud Scheduler setup complete!"
echo "Job Name: ${JOB_NAME}"
echo "Schedule: Daily at 2:15 AM Chicago time"
echo "Target URL: ${SERVICE_URL}/run"
echo ""
echo "The job will automatically fetch data for the previous day."
echo ""
echo "To test the scheduler job manually:"
echo "gcloud scheduler jobs run ${JOB_NAME} --location=${REGION}"
echo ""
echo "To check job status:"
echo "gcloud scheduler jobs describe ${JOB_NAME} --location=${REGION}"