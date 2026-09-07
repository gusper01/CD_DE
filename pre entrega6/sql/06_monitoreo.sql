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