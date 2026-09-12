-- ============================================================
-- CAPSTONE REAL-TIME
-- 01 - Redshift Streaming Ingestion desde Kinesis PROCESSED
-- ============================================================

-- ------------------------------------------------------------
-- 1. External schema Kinesis
-- ------------------------------------------------------------

CREATE EXTERNAL SCHEMA IF NOT EXISTS kinesis_processed
FROM KINESIS
REGION 'us-east-1'
IAM_ROLE default;


-- ------------------------------------------------------------
-- 2. Materialized View base
-- La lógica compleja queda fuera de la MV para mantener
-- la Streaming Ingestion simple y robusta.
-- ------------------------------------------------------------

CREATE MATERIALIZED VIEW sensor_events_processed_stream
AUTO REFRESH YES
AS
SELECT
    approximate_arrival_timestamp,
    partition_key,
    shard_id,
    sequence_number,

    CASE
        WHEN CAN_JSON_PARSE(kinesis_data)
        THEN JSON_PARSE(kinesis_data)
        ELSE NULL
    END AS payload

FROM kinesis_processed."urban-sensors-processed-dev";


-- Primer refresh explícito para la validación inicial.
REFRESH MATERIALIZED VIEW sensor_events_processed_stream;


-- ------------------------------------------------------------
-- 3. Capa analítica tipada
-- ------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS analytics_consumption;

CREATE OR REPLACE VIEW analytics_consumption.sensor_events_live AS
SELECT
    approximate_arrival_timestamp,

    payload.aggregation_id::VARCHAR(200)
        AS aggregation_id,

    payload.sensor_id::VARCHAR(50)
        AS sensor_id,

    payload.window_start::VARCHAR(40)
        AS window_start,

    payload.window_end::VARCHAR(40)
        AS window_end,

    payload.avg_temperature::FLOAT8
        AS avg_temperature,

    payload.avg_air_quality_index::FLOAT8
        AS avg_air_quality_index,

    payload.event_count::BIGINT
        AS event_count,

    payload.event_date::VARCHAR(10)
        AS event_date

FROM sensor_events_processed_stream
WHERE payload IS NOT NULL;


-- ------------------------------------------------------------
-- 4. Validaciones
-- ------------------------------------------------------------

SELECT COUNT(*) AS total_processed_events
FROM analytics_consumption.sensor_events_live;


SELECT *
FROM analytics_consumption.sensor_events_live
ORDER BY approximate_arrival_timestamp DESC
LIMIT 10;
