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