CREATE TABLE IF NOT EXISTS `dataleaguenovaretail.nR_core_datasets.bronze_events` (
  event_id INT64 NOT NULL,
  date_id INT64 NOT NULL,
  customer_id INT64,
  product_id INT64,
  session_id STRING,
  event_type STRING,
  ingestion_time TIMESTAMP NOT NULL
)
PARTITION BY DATE(ingestion_time)
CLUSTER BY event_type, customer_id;
