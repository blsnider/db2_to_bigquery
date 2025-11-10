#!/bin/bash

# Deploy DB2 to BigQuery migration service to Cloud Run

# Configuration
PROJECT_ID="sis-sandbox-463113"
REGION="us-central1"
SERVICE_NAME="db2-migration-service"  # Updated to match existing service
IMAGE_NAME="db2-migration-service"
ARTIFACT_REGISTRY="${REGION}-docker.pkg.dev/${PROJECT_ID}/cloud-run-source-deploy"

# Service account for Cloud Run (using existing SA)
SERVICE_ACCOUNT="db2-migration-sa@${PROJECT_ID}.iam.gserviceaccount.com"

# Set the project
echo "Setting project to ${PROJECT_ID}..."
gcloud config set project ${PROJECT_ID}

# Build the Docker image
echo "Building Docker image..."
docker build -t ${IMAGE_NAME} .

# Tag the image for Artifact Registry
echo "Tagging image for Artifact Registry..."
docker tag ${IMAGE_NAME} ${ARTIFACT_REGISTRY}/${IMAGE_NAME}:latest

# Push the image to Artifact Registry
echo "Pushing image to Artifact Registry..."
docker push ${ARTIFACT_REGISTRY}/${IMAGE_NAME}:latest

# Deploy to Cloud Run
echo "Deploying to Cloud Run..."
gcloud run deploy ${SERVICE_NAME} \
    --image ${ARTIFACT_REGISTRY}/${IMAGE_NAME}:latest \
    --region ${REGION} \
    --platform managed \
    --port 8080 \
    --memory 1Gi \
    --timeout 600 \
    --max-instances 10 \
    --no-allow-unauthenticated \
    --service-account ${SERVICE_ACCOUNT} \
    --set-env-vars "PROJECT_ID=${PROJECT_ID},BQ_DATASET=db2_migration,BQ_TABLE=po_inbound_daily,BQ_STAGING_TABLE=po_inbound_daily,BQ_FINAL_TABLE=po_inbound_final,BQ_BREAKOUT_STAGING_TABLE=po_breakout_staging,BQ_BREAKOUT_FINAL_TABLE=po_breakout_final,USE_MOCK_DB=0,DB2_SECRET_NAME=IBM_connect,DEBUG=1"

# Get the service URL
SERVICE_URL=$(gcloud run services describe ${SERVICE_NAME} --region ${REGION} --format='value(status.url)')
echo ""
echo "Deployment complete!"
echo "Service URL: ${SERVICE_URL}"
echo ""
echo "To test the service:"
echo "TOKEN=\$(gcloud auth print-identity-token)"
echo "curl -H \"Authorization: Bearer \$TOKEN\" \"${SERVICE_URL}/health\""
echo ""
echo "To run a migration:"
echo "curl -H \"Authorization: Bearer \$TOKEN\" \"${SERVICE_URL}/run?start_date=2025-07-01&end_date=2025-07-31\""