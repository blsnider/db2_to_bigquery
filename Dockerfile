
FROM python:3.10-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code and modules
COPY main.py .
COPY check_bq.py .
COPY queries/ ./queries/
COPY utils/ ./utils/

# Set environment variables
ENV PORT=8080
ENV PROJECT_ID=sis-sandbox-463113
ENV BQ_DATASET=db2_migration
ENV BQ_TABLE=po_inbound_daily
ENV BQ_STAGING_TABLE=po_inbound_daily
ENV BQ_FINAL_TABLE=po_inbound_final
ENV BQ_BREAKOUT_STAGING_TABLE=po_breakout_staging
ENV BQ_BREAKOUT_FINAL_TABLE=po_breakout_final
ENV USE_MOCK_DB=0

# Run the application with gunicorn
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app