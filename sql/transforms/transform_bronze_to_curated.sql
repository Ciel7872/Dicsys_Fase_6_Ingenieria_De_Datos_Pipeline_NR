-- Transformación desde bronze_events a FACT_EVENTS
-- Este script es un ejemplo base para la capa curada.

CREATE OR REPLACE TABLE `dataleaguenovaretail.nR_core_datasets.FACT_EVENTS` AS
SELECT
  event_id,
  date_id,
  customer_id,
  product_id,
  session_id,
  event_type,
  ingestion_time
FROM `dataleaguenovaretail.nR_core_datasets.bronze_events`
WHERE event_id IS NOT NULL;
