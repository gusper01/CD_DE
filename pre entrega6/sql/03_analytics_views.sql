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