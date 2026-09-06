# Pre-entrega 5 — Streaming Lakehouse con Amazon Managed Service for Apache Flink

## 1. Objetivo

La Pre-entrega 5 evoluciona el pipeline de streaming desarrollado en entregas anteriores, reutilizando la infraestructura y módulos Terraform existentes, y migrando el procesamiento principal de **PyFlink a Java** para ejecutarlo en **Amazon Managed Service for Apache Flink 1.19**.

El flujo procesa eventos de sensores urbanos enviados a **Amazon Kinesis Data Streams**, aplica procesamiento por ventanas con Apache Flink y persiste los resultados en un **Data Lakehouse basado en Apache Iceberg**, con catálogo en **AWS Glue** y almacenamiento en **Amazon S3**.

## 2. Arquitectura

```text
Kinesis_sensor_producer.py
        │
        ▼
Amazon Kinesis Data Streams
urban-sensors-dev
        │
        ▼
Amazon Managed Service for Apache Flink 1.19
LakehouseStreamingJob.java
        │
        ├── Parseo JSON
        ├── Event Time
        ├── Watermarks
        ├── Ventana de 1 minuto
        ├── AVG temperatura
        ├── AVG índice de calidad de aire
        └── COUNT de eventos
        │
        ▼
Apache Iceberg
        │
        ├── AWS Glue Catalog
        │      └── lakehouse_db.sensor_events
        │
        └── Amazon S3
               └── Parquet + metadata Iceberg
```

## 3. Reutilización de entregas anteriores

La solución no fue reconstruida desde cero. Se reutilizaron componentes desarrollados previamente:

- módulo Terraform de Flink en `pre entrega2/modules/flink`;
- infraestructura Kinesis/Firehose modelada previamente;
- logging en CloudWatch;
- productor `pre entrega4/scripts/Kinesis_sensor_producer.py`;
- estructura S3, Glue, IAM y Managed Flink.

El módulo compartido de Flink se extendió para soportar dos modos de ejecución:

```hcl
application_mode = "pyflink"
```

o:

```hcl
application_mode = "java"
```

El valor por defecto se mantuvo en `pyflink` para preservar compatibilidad con entregas anteriores.

## 4. Estructura relevante

```text
pre entrega5/
├── app/
│   ├── pom.xml
│   └── src/main/java/com/coderhouse/lakehouse/LakehouseStreamingJob.java
├── infra/
│   └── main.tf
├── evidencias/
└── README.md
```

La Pre-entrega 5 referencia el módulo reutilizado:

```hcl
source = "../../pre entrega2/modules/flink"
```

## 5. Aplicación Java

Clase principal:

```text
com.coderhouse.lakehouse.LakehouseStreamingJob
```

Ejemplo de evento:

```json
{
  "sensor_id": "sensor-01",
  "temperature": 24.7,
  "humidity": 60.2,
  "air_quality_index": 83,
  "event_time": "2026-09-05 20:05:20"
}
```

El procesamiento utiliza Event Time, watermarks y ventanas de un minuto para producir agregados que se escriben en Iceberg.

## 6. Construcción del artefacto Java

Desde:

```powershell
cd "C:\Users\Lenovo\Documents\GitHub\CD_DE\pre entrega5\app"
```

Ejecutar:

```powershell
mvn clean package -DskipTests
```

Artefacto resultante:

```text
target/urban-streaming-flink.jar
```

Verificación de clases:

```powershell
jar tf .\target\urban-streaming-flink.jar |
    Select-String "LakehouseStreamingJob"
```

Verificación del manifest:

```powershell
jar xf .\target\urban-streaming-flink.jar META-INF/MANIFEST.MF
Get-Content .\META-INF\MANIFEST.MF
```

Resultado esperado:

```text
Main-Class: com.coderhouse.lakehouse.LakehouseStreamingJob
```

## 7. Dependencias y troubleshooting

### 7.1 Hadoop runtime

El primer arranque falló por ausencia de:

```text
org/apache/hadoop/shaded/com/ctc/wstx/io/InputBootstrapper
```

Se incorporó `hadoop-client-runtime` 3.3.6 y se verificó:

```powershell
jar tf .\target\urban-streaming-flink.jar |
    Select-String "org/apache/hadoop/shaded/com/ctc/wstx/io/InputBootstrapper"
```

### 7.2 AWS SDK v2

El segundo arranque falló con:

```text
java.lang.NoSuchFieldError: AWS_AUTH_SCHEME_PREFERENCE
```

Se alineó AWS SDK v2 mediante BOM en versión `2.33.0`.

Verificación:

```powershell
mvn dependency:tree `
  "-Dincludes=software.amazon.awssdk"
```

Y:

```powershell
javap `
  -classpath .\target\urban-streaming-flink.jar `
  software.amazon.awssdk.core.SdkSystemSetting |
  Select-String "AWS_AUTH_SCHEME_PREFERENCE"
```

## 8. Versionado de artefactos

| Artefacto | Propósito |
|---|---|
| `urban-streaming-flink.jar` | primera versión Java |
| `urban-streaming-flink-v2.jar` | incorpora Hadoop runtime |
| `urban-streaming-flink-v3.jar` | alinea AWS SDK v2 en 2.33.0 |

Versión final validada:

```text
flink/urban-streaming-flink-v3.jar
```

## 9. Configuración Terraform

La Pre-entrega 5 utiliza el módulo Flink reutilizado:

```hcl
module "flink" {
  source = "../../pre entrega2/modules/flink"

  environment      = "dev"
  application_name = "urban-stream-processing-dev"
  application_mode = "java"

  kinesis_stream_name = module.kinesis.stream_name
  kinesis_stream_arn  = module.kinesis.stream_arn

  artifact_bucket_name = aws_s3_bucket.data_lake_raw.bucket
  artifact_bucket_arn  = aws_s3_bucket.data_lake_raw.arn
  artifact_key         = "flink/urban-streaming-flink-v3.jar"

  lakehouse_bucket_name = aws_s3_bucket.lakehouse.bucket
  lakehouse_bucket_arn  = aws_s3_bucket.lakehouse.arn
  glue_database_name    = aws_glue_catalog_database.lakehouse.name
}
```

## 10. Orden de despliegue

### 10.1 Validación Terraform

```powershell
cd "C:\Users\Lenovo\Documents\GitHub\CD_DE\pre entrega5\infra"
terraform fmt .\main.tf
terraform fmt "..\..\pre entrega2\modules\flink\main.tf"
terraform fmt "..\..\pre entrega2\modules\flink\variables.tf"
terraform validate
```

Resultado esperado:

```text
Success! The configuration is valid.
```

### 10.2 Crear primero el bucket del artefacto

```powershell
terraform plan "-target=aws_s3_bucket.data_lake_raw"
terraform apply "-target=aws_s3_bucket.data_lake_raw"
```

### 10.3 Subir el JAR

```powershell
aws s3 cp `
  "..\app\target\urban-streaming-flink.jar" `
  "s3://coderhouse-urban-streaming-raw-gusper-dev/flink/urban-streaming-flink-v3.jar"
```

Verificación:

```powershell
aws s3 ls `
  "s3://coderhouse-urban-streaming-raw-gusper-dev/flink/"
```

### 10.4 Desplegar infraestructura completa

```powershell
terraform plan
terraform apply
```

Durante la actualización del artefacto Java se verificó:

```text
Apply complete! Resources: 0 added, 1 changed, 0 destroyed.
```

## 11. Ejecución de Managed Flink

Aplicación:

```text
urban-stream-processing-dev
```

Runtime:

```text
Apache Flink 1.19
```

Inicio:

```powershell
aws kinesisanalyticsv2 start-application `
  --application-name urban-stream-processing-dev `
  --region us-east-1
```

Verificación:

```powershell
aws kinesisanalyticsv2 describe-application `
  --application-name urban-stream-processing-dev `
  --region us-east-1 `
  --query "ApplicationDetail.{Status:ApplicationStatus,Version:ApplicationVersionId}" `
  --output table
```

La versión final alcanzó `RUNNING`, versión `3`.

## 12. Prueba end-to-end

Se reutilizó:

```text
pre entrega4/scripts/Kinesis_sensor_producer.py
```

Ejecución:

```powershell
python "..\..\pre entrega4\scripts\Kinesis_sensor_producer.py"
```

Flujo validado:

```text
Producer Python
→ Kinesis Data Streams
→ Managed Flink Java
→ Agregaciones por ventana
→ Apache Iceberg
→ AWS Glue Catalog
→ Amazon S3 / Parquet
```

## 13. Validaciones realizadas

### Kinesis

```text
Status = ACTIVE
Shards = 2
```

### Glue

```text
Database  = lakehouse_db
Table     = sensor_events
TableType = EXTERNAL_TABLE
```

Ubicación:

```text
s3://coderhouse-urban-lakehouse-gusper-dev/lakehouse/lakehouse_db.db/sensor_events
```

### S3 / Iceberg

Se verificó la generación de:

```text
data/event_date=YYYY-MM-DD/*.parquet
metadata/*.metadata.json
metadata/*.avro
```

### CloudWatch

```powershell
aws logs tail `
  "/aws/kinesis-analytics/urban-stream-processing-dev" `
  --since 10m `
  --region us-east-1 `
  --format short |
Select-String '"messageType":"ERROR"'
```

Sin coincidencias durante la prueba final.

## 14. Evidencias

Las evidencias se almacenan en:

```text
pre entrega5/evidencias/
```

Incluyen, entre otras:

- compilación Maven exitosa;
- validación del JAR y `Main-Class`;
- validaciones Hadoop/AWS SDK;
- `terraform validate`;
- `terraform plan` y `terraform apply`;
- versiones del artefacto en S3;
- Kinesis `ACTIVE`;
- Managed Flink `READY` y `RUNNING`;
- tabla `sensor_events` en Glue;
- Parquet y metadata Iceberg en S3;
- ausencia de errores reales en CloudWatch;
- Parquet descargado localmente;
- `terraform destroy` exitoso;
- verificación posterior de eliminación de recursos AWS.

## 15. Destrucción de infraestructura

Detener primero Managed Flink:

```powershell
aws kinesisanalyticsv2 stop-application `
  --application-name urban-stream-processing-dev `
  --region us-east-1
```

Verificar que vuelva a `READY` y luego:

```powershell
terraform plan -destroy
terraform destroy
```

Resultado final:

```text
Destroy complete! Resources: 15 destroyed.
```

Finalmente:

```powershell
terraform state list
```

quedó sin recursos.

También se verificó que AWS devolviera `ResourceNotFoundException` para la aplicación Flink y el stream Kinesis eliminados.

## 16. Resultado final

La Pre-entrega 5 logró migrar el procesamiento de PyFlink a Java reutilizando la infraestructura y módulos Terraform existentes.

Se validó exitosamente:

```text
Kinesis
→ Managed Flink 1.19 / Java
→ Apache Iceberg
→ AWS Glue
→ Amazon S3 / Parquet
```

La infraestructura fue desplegada, probada end-to-end y posteriormente destruida con Terraform para evitar costos.

## 17. Publicación en GitHub

Antes de publicar:

```powershell
git status --short --untracked-files=all
git ls-files --others --ignored --exclude-standard -- "pre entrega5"
```
La branch utilizada para esta entrega es:

```text
feature/preentrega5-lakehouse

No deben incluirse:

```text
.terraform/
*.tfstate
*.tfstate.*
target/
*.jar
credenciales
logs temporales
```

Luego crear la branch de entrega, revisar el staging, hacer commit y push.
