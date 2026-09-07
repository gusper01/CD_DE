-- ============================================================
-- PRE-ENTREGA 6
-- 01 - External Schema hacia AWS Glue / Apache Iceberg
-- ============================================================

CREATE EXTERNAL SCHEMA IF NOT EXISTS lakehouse_iceberg
FROM DATA CATALOG
DATABASE 'lakehouse_db'
IAM_ROLE default
--CREATE EXTERNAL DATABASE IF NOT EXISTS; ya se crea con Terraform