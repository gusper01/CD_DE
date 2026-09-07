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