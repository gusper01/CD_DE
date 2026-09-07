-- ============================================================
-- PRE-ENTREGA 6
-- 01 - External Schema hacia AWS Glue / Apache Iceberg
-- ============================================================

CREATE EXTERNAL SCHEMA IF NOT EXISTS lakehouse_iceberg
FROM DATA CATALOG
DATABASE 'lakehouse_db'
IAM_ROLE default
--CREATE EXTERNAL DATABASE IF NOT EXISTS; ya se crea con Terraform

-- ============================================================

-- ============================================================
-- PRE-ENTREGA 6
-- 02 - Streaming Ingestion desde Kinesis
-- ============================================================

CREATE EXTERNAL SCHEMA IF NOT EXISTS kinesis_stream
FROM KINESIS
REGION 'us-east-1'
IAM_ROLE default;

CREATE MATERIALIZED VIEW sensor_events_stream
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

FROM kinesis_stream."urban-sensors-dev";

-- ============================================================

-- ============================================================
-- PRE-ENTREGA 6
-- 03 - Capa analítica tipada sobre el stream
-- ============================================================
-- Se utiliza una VIEW normal sobre la Materialized View de streaming.
-- Redshift no permite AUTO REFRESH en una Materialized View
-- definida sobre otra Materialized View.
CREATE SCHEMA IF NOT EXISTS analytics_consumption;

CREATE OR REPLACE VIEW analytics_consumption.sensor_events_live AS
SELECT
    approximate_arrival_timestamp,
    payload.sensor_id::VARCHAR(50)    AS sensor_id,
    payload.event_time::VARCHAR(30)   AS event_time,
    payload.temperature::FLOAT8       AS temperature,
    payload.humidity::FLOAT8          AS humidity,
    payload.air_quality_index::INTEGER AS air_quality_index
FROM sensor_events_stream
WHERE payload IS NOT NULL;

-- ============================================================

-- ============================================================
-- PRE-ENTREGA 6
-- 04 - JOIN entre datos calientes e histórico Iceberg
-- ============================================================

WITH historico AS (
    SELECT
        sensor_id,
        AVG(avg_temperature) AS temperatura_promedio_historica
    FROM lakehouse_iceberg.sensor_events
    GROUP BY sensor_id
)
SELECT
    live.sensor_id,
    live.event_time,
    live.temperature AS temperatura_actual,
    hist.temperatura_promedio_historica,
    live.temperature - hist.temperatura_promedio_historica AS desvio
FROM analytics_consumption.sensor_events_live live
JOIN historico hist
    ON live.sensor_id = hist.sensor_id
ORDER BY live.approximate_arrival_timestamp DESC
LIMIT 20;

-- ============================================================

-- ============================================================
-- PRE-ENTREGA 6
-- 05 - Seguridad y RBAC
-- ============================================================

-- Restringir acceso público al esquema de ingesta cruda
REVOKE ALL ON SCHEMA kinesis_stream FROM PUBLIC;

-- Rol de consumo analítico
CREATE ROLE analytics_reader;

-- Permitir acceso únicamente a la capa preparada para consumo
GRANT USAGE
ON SCHEMA analytics_consumption
TO ROLE analytics_reader;

GRANT SELECT
ON TABLE analytics_consumption.sensor_events_live
TO ROLE analytics_reader;

-- ============================================================

-- ============================================================
-- PRE-ENTREGA 6
-- 06 - Monitoreo de Streaming Ingestion
-- ============================================================

-- Estado de lectura del stream
SELECT *
FROM SYS_STREAM_SCAN_STATES
ORDER BY record_time DESC
LIMIT 20;

-- Errores de lectura / parseo del stream
SELECT *
FROM SYS_STREAM_SCAN_ERRORS
ORDER BY record_time DESC
LIMIT 20;

-- Historial de refresh de Materialized Views
SELECT *
FROM SYS_MV_REFRESH_HISTORY
ORDER BY start_time DESC
LIMIT 20;

-- ============================================================