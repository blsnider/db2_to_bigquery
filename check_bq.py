from google.cloud import bigquery

# Initialize BigQuery client
client = bigquery.Client(project='sis-sandbox-463113')

# Run a simple test query
sql = 'SELECT "ok" AS status, CURRENT_TIMESTAMP() AS ts'
print("Testing BigQuery connection...")
results = list(client.query(sql).result())
print(f"BigQuery test result: {results}")

# Test access to the db2_migration dataset
dataset_id = 'sis-sandbox-463113.db2_migration'
dataset = client.get_dataset(dataset_id)
print(f"Dataset '{dataset.dataset_id}' exists with description: {dataset.description}")