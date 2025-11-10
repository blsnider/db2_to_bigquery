#!/bin/bash

# Setup service account and IAM permissions for DB2 to BigQuery migration

PROJECT_ID="sis-sandbox-463113"
SERVICE_ACCOUNT_NAME="db2-migration-sa"
SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

echo "Setting up service account for DB2 to BigQuery migration..."

# Set the project
gcloud config set project ${PROJECT_ID}

# Create the service account
echo "Creating service account ${SERVICE_ACCOUNT_NAME}..."
gcloud iam service-accounts create ${SERVICE_ACCOUNT_NAME} \
    --display-name="DB2 to BigQuery Migration Service Account" \
    --description="Service account for running DB2 to BigQuery migration on Cloud Run"

# Grant BigQuery permissions
echo "Granting BigQuery permissions..."
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
    --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
    --role="roles/bigquery.dataEditor"

# Grant Secret Manager access
echo "Granting Secret Manager permissions..."
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
    --role="roles/secretmanager.secretAccessor"

echo ""
echo "Service account setup complete!"
echo "Service Account: ${SERVICE_ACCOUNT_EMAIL}"
echo ""
echo "Next steps:"
echo "1. Run ./deploy.sh to deploy the service to Cloud Run"
echo "2. Set up Cloud Scheduler to trigger the service nightly"