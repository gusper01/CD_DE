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