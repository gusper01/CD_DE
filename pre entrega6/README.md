# Pre-entrega 6 — Analítica avanzada in-stream con Amazon Redshift

## 1. Objetivo

La Pre-entrega 6 incorpora una capa de analítica de baja latencia sobre el pipeline construido en las entregas anteriores.

El objetivo es integrar:

- **Amazon Kinesis Data Streams** como fuente de eventos en tiempo real.
- **Amazon Managed Service for Apache Flink** como procesamiento streaming y persistencia histórica.
- **Apache Iceberg + AWS Glue + Amazon S3** como capa Lakehouse.
- **Amazon Redshift Serverless** como motor analítico de baja latencia.
- **Redshift Streaming Ingestion** para consumir Kinesis directamente, sin pasar por S3.
- **Materialized Views** para ingesta continua.
- **RBAC y monitoreo** para control de acceso y observabilidad.
- Un **JOIN entre datos calientes e históricos** en una misma sesión SQL.

La implementación reutiliza la arquitectura de la Pre-entrega 5 y agrega Redshift Serverless sin duplicar innecesariamente los componentes previos.

---

## 2. Arquitectura

```text
                         ┌──────────────────────────────┐
                         │       Producer Python        │
                         │ Kinesis_sensor_producer.py   │
                         └──────────────┬───────────────┘
                                        │
                                        ▼
                         ┌──────────────────────────────┐
                         │ Amazon Kinesis Data Streams  │
                         │      urban-sensors-dev       │
                         └──────────────┬───────────────┘
                                        │
                    ┌───────────────────┴───────────────────┐
                    │                                       │
                    ▼                                       ▼
        ┌─────────────────────────┐           ┌─────────────────────────────┐
        │ Managed Flink 1.19      │           │ Redshift Streaming         │
        │ Java                    │           │ Ingestion                  │
        │ urban-stream-processing│           │ sensor_events_stream       │
        └─────────────┬───────────┘           └──────────────┬──────────────┘
                      │                                      │
                      ▼                                      ▼
             ┌────────────────┐                 ┌───────────────────────────┐
             │ Apache Iceberg │                 │ analytics_consumption     │
             └───────┬────────┘                 │ sensor_events_live        │
                     │                          └─────────────┬─────────────┘
             ┌───────┴───────────┐                            │
             ▼                   ▼                            │
        AWS Glue             Amazon S3                        │
        lakehouse_db         Parquet + metadata              │
             │                   │                            │
             └──────────────┬────┘                            │
                            │                                 │
                            └──────────────┬──────────────────┘
                                           ▼
                             JOIN hot + historical
```

La arquitectura mantiene dos caminos complementarios:

1. **Camino histórico**: Kinesis → Flink → Iceberg → Glue/S3.
2. **Camino de baja latencia**: Kinesis → Redshift Streaming Ingestion → Materialized View.

---

## 3. Estructura del proyecto

```text
pre entrega6/
├── infra/
│   ├── main.tf
│   ├── redshift.tf
│   ├── variables.tf
│   └── outputs.tf
│
├── sql/
│   ├── 01_external_schema_glue.sql
│   ├── 02_kinesis_streaming_ingestion.sql
│   ├── 03_analytics_views.sql
│   ├── 04_join_hot_historical.sql
│   ├── 05_seguridad_rbac.sql
│   ├── 06_monitoreo.sql
│   └── preentrega6_redshift.sql
│
├── test/
│
├── evidencias/
│   └── ...
│
├── CheckPoint_Redshift_Pereyra_Gustavo.pdf
└── README.md
```

La infraestructura reutiliza los módulos existentes:

```text
pre entrega2/modules/kinesis
pre entrega2/modules/flink
```

y el productor de eventos existente:

```text
pre entrega4/scripts/Kinesis_sensor_producer.py
```

---

## 4. Región AWS

Toda la implementación se ejecutó en:

```text
us-east-1
```

La guía de clase utiliza ejemplos en `us-east-2`, pero en este proyecto se mantuvo `us-east-1` para conservar compatibilidad con la infraestructura construida en las entregas anteriores.

---

## 5. Infraestructura Terraform

La Pre-entrega 6 agrega los siguientes componentes a la infraestructura existente:

```text
Amazon Redshift Serverless
├── Namespace
├── Workgroup
├── IAM Role
├── IAM Policy
├── Security Group
├── Kinesis VPC Endpoint
└── Enhanced VPC Routing
```

Recursos principales:

```text
Redshift namespace : urban-lakehouse-dev
Redshift workgroup : urban-lakehouse-dev
Database           : dev
Admin user         : admin_lakehouse
Kinesis stream     : urban-sensors-dev
Flink application  : urban-stream-processing-dev
Glue database      : lakehouse_db
```

### Permisos IAM para Redshift

El rol de Redshift aplica mínimo privilegio para:

```text
Kinesis
- DescribeStream
- DescribeStreamSummary
- GetShardIterator
- GetRecords
- ListShards

Glue
- GetDatabase
- GetDatabases
- GetTable
- GetTables
- GetPartition
- GetPartitions
- BatchGetPartition

S3 Lakehouse
- ListBucket
- GetBucketLocation
- GetObject
```

El rol puede ser asumido por:

```text
redshift.amazonaws.com
redshift-serverless.amazonaws.com
```

---

## 6. Variables sensibles

La contraseña administrativa de Redshift no se guarda en el código.

Se utiliza:

```text
pre entrega6/infra/terraform.tfvars
```

Ejemplo:

```hcl
redshift_admin_password = "PASSWORD_LOCAL"
```

El archivo está excluido por `.gitignore` mediante:

```gitignore
*.tfvars
```

No debe versionarse ni incluirse en capturas.

---

## 7. Inicialización y validación Terraform

Desde:

```powershell
cd "C:\Users\Lenovo\Documents\GitHub\CD_DE\pre entrega6\infra"
```

ejecutar:

```powershell
terraform fmt
terraform init
terraform validate
```

Resultado validado:

```text
Success! The configuration is valid.
```

Luego:

```powershell
terraform plan
```

El plan inicial completo mostró:

```text
Plan: 22 to add, 0 to change, 0 to destroy.
```

---

## 8. Despliegue controlado del artefacto Flink

Managed Flink necesita que el JAR exista en S3 antes de crear la aplicación.

Primero se crea únicamente el bucket de artefactos:

```powershell
terraform plan "-target=aws_s3_bucket.data_lake_raw"
terraform apply "-target=aws_s3_bucket.data_lake_raw"
```

Luego se verifica el JAR local:

```powershell
Test-Path "..\..\pre entrega5\app\target\urban-streaming-flink.jar"
```

Se sube al bucket:

```powershell
aws s3 cp `
  "..\..\pre entrega5\app\target\urban-streaming-flink.jar" `
  "s3://coderhouse-urban-streaming-raw-gusper-dev/flink/urban-streaming-flink-v3.jar" `
  --region us-east-1
```

Verificación:

```powershell
aws s3 ls `
  "s3://coderhouse-urban-streaming-raw-gusper-dev/flink/" `
  --region us-east-1
```

Después se ejecuta el despliegue completo:

```powershell
terraform plan
terraform apply
```

Resultado:

```text
Apply complete! Resources: 21 added, 0 changed, 0 destroyed.
```

El total de recursos fue de 22 porque el bucket se había creado previamente mediante `-target`.

---

## 9. Validación de Redshift Serverless

Se verificó el workgroup mediante AWS CLI:

```powershell
aws redshift-serverless get-workgroup `
  --workgroup-name urban-lakehouse-dev `
  --region us-east-1 `
  --query "workgroup.{Status:status,Name:workgroupName,Endpoint:endpoint.address,BaseCapacity:baseCapacity}" `
  --output table
```

Resultado esperado y validado:

```text
Status       = AVAILABLE
Name         = urban-lakehouse-dev
BaseCapacity = 8
```

---

## 10. Conexión a Query Editor v2

La ejecución SQL se realizó desde:

```text
AWS Console
→ Amazon Redshift
→ Query Editor v2
```

Con:

```text
Workgroup : urban-lakehouse-dev
Database  : dev
User      : admin_lakehouse
```

Validación:

```sql
SELECT current_database(), current_user;
```

Resultado:

```text
current_database = dev
current_user     = admin_lakehouse
```

---

# 11. SQL

Todo el código SQL está consolidado en:

```text
pre entrega6/sql/preentrega6_redshift.sql
```

Los scripts individuales se ejecutaron y validaron en el siguiente orden.

---

## 11.1 External Schema hacia Glue / Iceberg

Archivo:

```text
01_external_schema_glue.sql
```

```sql
CREATE EXTERNAL SCHEMA IF NOT EXISTS lakehouse_iceberg
FROM DATA CATALOG
DATABASE 'lakehouse_db'
IAM_ROLE default;
```

Validación:

```sql
SELECT *
FROM lakehouse_iceberg.sensor_events
LIMIT 10;
```

Redshift pudo consultar correctamente la tabla Iceberg generada por Flink.

---

## 11.2 Streaming Ingestion desde Kinesis

Archivo:

```text
02_kinesis_streaming_ingestion.sql
```

```sql
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
```

Esta configuración implementa:

```text
Kinesis → Redshift
```

sin utilizar S3 como staging intermedio.

Validación:

```sql
SELECT COUNT(*) AS total_eventos
FROM sensor_events_stream;
```

Durante la prueba se observaron inicialmente:

```text
899 eventos
```

---

## 11.3 Capa analítica tipada

Archivo:

```text
03_analytics_views.sql
```

```sql
CREATE SCHEMA IF NOT EXISTS analytics_consumption;

CREATE OR REPLACE VIEW analytics_consumption.sensor_events_live AS
SELECT
    approximate_arrival_timestamp,
    payload.sensor_id::VARCHAR(50)       AS sensor_id,
    payload.event_time::VARCHAR(30)      AS event_time,
    payload.temperature::FLOAT8          AS temperature,
    payload.humidity::FLOAT8             AS humidity,
    payload.air_quality_index::INTEGER   AS air_quality_index
FROM sensor_events_stream
WHERE payload IS NOT NULL;
```

Campos JSON convertidos a tipos nativos:

```text
sensor_id          → VARCHAR
event_time         → VARCHAR
temperature        → FLOAT8
humidity           → FLOAT8
air_quality_index  → INTEGER
```

### Decisión técnica

Inicialmente se evaluó crear una segunda Materialized View con `AUTO REFRESH YES` sobre `sensor_events_stream`.

Redshift devolvió:

```text
Auto-refresh is not supported for materialized views defined on other materialized views.
```

Por este motivo, el diseño final utiliza:

```text
Kinesis
   ↓
sensor_events_stream
MATERIALIZED VIEW + AUTO REFRESH
   ↓
analytics_consumption.sensor_events_live
VIEW normal tipada
```

De esta manera existe una única Materialized View consumiendo directamente el stream.

---

## 11.4 Consulta analítica en vivo

Validación:

```sql
SELECT
    sensor_id,
    COUNT(*) AS cantidad_eventos,
    AVG(temperature) AS temperatura_promedio
FROM analytics_consumption.sensor_events_live
GROUP BY sensor_id
ORDER BY sensor_id;
```

Se validó agregación en tiempo real para los cinco sensores.

---

## 11.5 Validación de AUTO REFRESH

Consulta:

```sql
SELECT
    COUNT(*) AS total_eventos,
    GETDATE() AS momento
FROM sensor_events_stream;
```

Durante la prueba:

```text
Primera consulta : 899 eventos
Segunda consulta : 1088 eventos
Incremento       : +189 eventos
```

No se ejecutó ningún `INSERT` manual en Redshift.

Esto demuestra el refresco continuo:

```text
Producer
   ↓
Kinesis
   ↓
Streaming Ingestion
   ↓
sensor_events_stream
AUTO REFRESH
```

---

## 11.6 JOIN hot + historical

Archivo:

```text
04_join_hot_historical.sql
```

```sql
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
```

Esta consulta integra:

```text
Dato caliente
Kinesis → Redshift

+

Dato histórico
Flink → Iceberg → Glue → Redshift
```

en una única consulta SQL.

---

## 12. Seguridad y RBAC

Archivo:

```text
05_seguridad_rbac.sql
```

```sql
REVOKE ALL
ON SCHEMA kinesis_stream
FROM PUBLIC;

CREATE ROLE analytics_reader;

GRANT USAGE
ON SCHEMA analytics_consumption
TO ROLE analytics_reader;

GRANT SELECT
ON TABLE analytics_consumption.sensor_events_live
TO ROLE analytics_reader;
```

Modelo aplicado:

```text
kinesis_stream
   ↓
raw / restringido

analytics_consumption
   ↓
analytics_reader
   ↓
SELECT permitido
```

Esto separa la capa de ingesta cruda de la capa preparada para consumo analítico.

---

## 13. Monitoreo

Archivo:

```text
06_monitoreo.sql
```

### Estado del stream

```sql
SELECT *
FROM SYS_STREAM_SCAN_STATES
ORDER BY record_time DESC
LIMIT 20;
```

Se verificaron:

```text
stream_name
mv_name
record_time
partition_id
latest_position
```

### Errores de ingesta

```sql
SELECT *
FROM SYS_STREAM_SCAN_ERRORS
ORDER BY record_time DESC
LIMIT 20;
```

Resultado validado:

```text
Returned rows: 0
```

No se detectaron errores de lectura o parseo.

### Historial de refresh

```sql
SELECT *
FROM SYS_MV_REFRESH_HISTORY
ORDER BY start_time DESC
LIMIT 20;
```

Se verificó:

```text
mv_name      = sensor_events_stream
refresh_type = Auto
```

---

## 14. Evidencias

Las evidencias se almacenan en:

```text
pre entrega6/evidencias/
```

Inventario final:

```text
01_terraform_plan_redshift.txt
02_terraform_apply_redshift.png
03_redshift_serverless_available.png
04_query_editor_connection.png
05_external_schema_glue.png
06_iceberg_query.png
07_streaming_ingestion_mv.png
08_kinesis_live_data.png
09_parsed_live_view.png
10_live_aggregation.png
11_mv_auto_refresh.png
12_hot_historical_join.png
13_rbac_security.png
14_streaming_monitoring.png
15_terraform_destroy.png
```

---

## 15. Documento técnico

El entregable técnico de la actividad se encuentra en:

```text
pre entrega6/CheckPoint_Redshift_Pereyra_Gustavo.pdf
```

Contiene:

- diagrama de arquitectura;
- infraestructura Terraform;
- Streaming Ingestion;
- Materialized View;
- Glue/Iceberg;
- parseo JSON;
- capa analítica;
- auto refresh;
- JOIN hot + historical;
- RBAC;
- monitoreo;
- evidencias;
- destrucción final del ambiente.

---

## 16. Destrucción de infraestructura

Para evitar costos, al finalizar las pruebas se ejecutó:

```powershell
terraform plan -destroy
```

Resultado:

```text
Plan: 0 to add, 0 to change, 22 to destroy.
```

Luego:

```powershell
terraform destroy
```

Resultado:

```text
Destroy complete! Resources: 22 destroyed.
```

Después se verificó:

```powershell
terraform state list
```

sin recursos registrados.

También se comprobó mediante AWS CLI que ya no existían:

```text
Redshift Serverless workgroup
Managed Flink application
Kinesis stream
```

Los comandos devolvieron:

```text
ResourceNotFoundException
```

confirmando la eliminación efectiva de la infraestructura.

---

## 17. Resultado final

La Pre-entrega 6 implementó exitosamente un patrón híbrido de analítica:

```text
                       Kinesis
                      /       \
                     /         \
                    ▼           ▼
                  Flink      Redshift
                    │           │
                    ▼           ▼
                 Iceberg     Streaming MV
                    │           │
                    ▼           ▼
                 histórico    dato hot
                     \          /
                      \        /
                       ▼      ▼
                       JOIN SQL
```

Se validó:

```text
Kinesis → Redshift Streaming Ingestion
Kinesis → Flink → Iceberg
Redshift → Glue/Iceberg
JSON → tipos nativos
AUTO REFRESH
JOIN hot + historical
RBAC
Monitoreo
0 errores de streaming
```

Finalmente, toda la infraestructura fue destruida mediante Terraform para evitar costos.

---

## 18. Publicación en GitHub

Antes de hacer staging:

```powershell
git status --short --untracked-files=all -- "pre entrega6"
```

No deben versionarse:

```text
.terraform/
terraform.tfstate
terraform.tfstate.backup
terraform.tfvars
credenciales
logs temporales
```

Sí deben incluirse:

```text
README.md
infra/*.tf
sql/*.sql
evidencias/
CheckPoint_Redshift_Pereyra_Gustavo.pdf
```

La branch utilizada para esta entrega es:

```text
feature/preentrega6-redshift
```

Antes del commit se debe comprobar el staging:

```powershell
git diff --cached --name-status
git diff --cached --check
git status
```

Después del commit:

```powershell
git --no-pager show --stat --name-status HEAD
```

La branch se publica en GitHub y luego se crea un Pull Request hacia:

```text
main
```

Una vez revisado y mergeado el PR, se sincroniza `main` local con `origin/main`.
