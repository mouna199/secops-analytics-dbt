from google.cloud import bigquery

# Configuration
PROJECT_ID = "secops-analytics-platform"
BUCKET_NAME = "secops_storage_mouna"
DATASET_ID = "raw_production"

client = bigquery.Client(project=PROJECT_ID)

# ============================================
# FONCTION DE LOAD
# ============================================

def load_table(source_file, table_id, schema, partition_field=None, cluster_fields=None):
    """Load un fichier GCS vers BigQuery."""
    
    gcs_uri = f"gs://{BUCKET_NAME}/data/{source_file}"
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
    
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    
    if partition_field:
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field
        )
    
    if cluster_fields:
        job_config.clustering_fields = cluster_fields
    
    print(f"Loading {gcs_uri} → {table_ref}...")
    load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    load_job.result()
    
    table = client.get_table(table_ref)
    print(f"✓ Loaded {table.num_rows:,} rows\n")


# ============================================
# SCHEMAS (adaptés à TES colonnes)
# ============================================

# Schema 1 : Authentication logs
SCHEMA_AUTH = [
    bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("event_timestamp", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("user_id", "STRING"),
    bigquery.SchemaField("email", "STRING"),
    bigquery.SchemaField("event_type", "STRING"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("source_ip", "STRING"),
    bigquery.SchemaField("source_country", "STRING"),
    bigquery.SchemaField("device_type", "STRING"),
    bigquery.SchemaField("mfa_enabled", "BOOLEAN"),
    bigquery.SchemaField("failure_reason", "STRING"),
]

# Schema 2 : Network logs
SCHEMA_NETWORK = [
    bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("event_timestamp", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("source_ip", "STRING"),
    bigquery.SchemaField("destination_ip", "STRING"),
    bigquery.SchemaField("destination_port", "INTEGER"),
    bigquery.SchemaField("protocol", "STRING"),
    bigquery.SchemaField("action", "STRING"),
    bigquery.SchemaField("bytes_sent", "INTEGER"),
    bigquery.SchemaField("threat_detected", "BOOLEAN"),
    bigquery.SchemaField("threat_type", "STRING"),
    bigquery.SchemaField("threat_severity", "STRING"),
]

# Schema 3 : Incidents
SCHEMA_INCIDENTS = [
    bigquery.SchemaField("incident_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("acknowledged_at", "TIMESTAMP"),
    bigquery.SchemaField("resolved_at", "TIMESTAMP"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("priority", "STRING"),
    bigquery.SchemaField("category", "STRING"),
    bigquery.SchemaField("false_positive", "BOOLEAN"),
    bigquery.SchemaField("sla_breached", "BOOLEAN"),
]


# ============================================
# EXECUTION
# ============================================

if __name__ == "__main__":
    
    print("=" * 60)
    print("LOADING DATA TO BIGQUERY")
    print("=" * 60 + "\n")
    
    # Table 1 : Auth logs
    load_table(
        source_file="auth_logs.csv",
        table_id="authentication_logs",
        schema=SCHEMA_AUTH,
        partition_field="event_timestamp",
        cluster_fields=["user_id", "status"]
    )
    
    # Table 2 : Network logs
    load_table(
        source_file="network_logs.csv",
        table_id="network_traffic_logs",
        schema=SCHEMA_NETWORK,
        partition_field="event_timestamp",
        cluster_fields=["source_ip", "action"]
    )
    
    # Table 3 : Incidents
    load_table(
        source_file="incidents.csv",
        table_id="security_incidents",
        schema=SCHEMA_INCIDENTS,
        partition_field="created_at",
        cluster_fields=["priority", "status"]
    )
    
    print("=" * 60)
    print("✓ ALL TABLES LOADED SUCCESSFULLY")
    print("=" * 60)