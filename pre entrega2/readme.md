# Ingesta de datos con Amazon Kinesis, Firehose y Terraform

## Descripción

Este proyecto implementa una infraestructura de ingesta de datos en AWS utilizando **Terraform**, **Amazon Kinesis Data Streams**, **Amazon Data Firehose**, **Amazon S3**, **AWS IAM** y **Amazon CloudWatch**.

La arquitectura permite recibir eventos en un Kinesis Data Stream y entregarlos mediante Firehose a un bucket S3 utilizado como Data Lake.

## Arquitectura

```text
Productor / AWS CLI
        |
        v
Amazon Kinesis Data Stream
        |
        v
Amazon Data Firehose
        |
        +------> Amazon CloudWatch Logs
        |
        v
Amazon S3
(Data Lake)
```

---

## 1. Estructura del proyecto

La infraestructura correspondiente a Kinesis se encuentra organizada como un módulo Terraform:

```text
├── enviroments/
│   └── dev/
│       ├── main.tf    
├── imagenes/
│
├── modules/
│   └── identity/
│   └── kinesis/
│       ├── main.tf
│       └── variables.tf  
│   └── network/
├── scripts/
│   └── producers.py
│
└── README.md
```

El directorio:

```text
modules/kinesis/
```

contiene la definición de los recursos AWS necesarios para implementar el pipeline de ingesta.

---

## 2. Kinesis Data Stream

La infraestructura define un recurso:

```hcl
aws_kinesis_stream
```

El nombre del stream se obtiene dinámicamente a partir de variables Terraform y se utilizan tags para identificar el entorno.

El stream está configurado en modo:

```text
PROVISIONED
```

con al menos:

```text
2 shards
```

La configuración permite controlar explícitamente la capacidad disponible para la ingesta de eventos.

Además, el Kinesis Data Stream utiliza cifrado en reposo mediante AWS KMS:

```hcl
encryption_type = "KMS"
```

De esta manera, los registros almacenados temporalmente en Kinesis permanecen cifrados.

---

## 3. Kinesis Data Firehose

Se utiliza:

```hcl
aws_kinesis_firehose_delivery_stream
```

para consumir los registros provenientes del Kinesis Data Stream y entregarlos al Data Lake almacenado en Amazon S3.

El flujo implementado es:

```text
Kinesis Data Stream
        |
        v
Kinesis Data Firehose
        |
        v
Amazon S3
```

Firehose utiliza como origen el Kinesis Data Stream creado por Terraform.

El destino configurado es Amazon S3.

### Buffering

Para el ambiente de desarrollo se utiliza una política de buffering agresiva:

```text
Buffer size     = 5 MB
Buffer interval = 60 segundos
```

Firehose entrega los datos cuando se cumple alguna de las condiciones de buffering configuradas.

Esto permite reducir el tiempo de espera durante las pruebas de desarrollo.

### Organización de datos en S3

Los registros entregados correctamente se almacenan utilizando un prefijo similar a:

```text
ingesta/year=YYYY/
```

Los registros que presentan errores de procesamiento o entrega utilizan un prefijo separado:

```text
ingesta-errores/type=<error-output-type>/year=YYYY/
```

Esto permite separar los datos procesados correctamente de aquellos que requieren análisis o reprocesamiento.

---

## 4. Seguridad e IAM

Se define un IAM Role específico para Amazon Data Firehose.

El rol permite realizar las operaciones necesarias para el funcionamiento del pipeline, incluyendo:

- lectura del Kinesis Data Stream;
- escritura de objetos en el bucket S3;
- acceso al bucket utilizado como Data Lake;
- escritura de eventos de logging en Amazon CloudWatch Logs.

El objetivo es aplicar el principio de **mínimo privilegio (Least Privilege)**, otorgando al servicio únicamente los permisos necesarios para ejecutar el flujo de ingesta.

La arquitectura de seguridad puede resumirse como:

```text
Kinesis Data Stream
        |
        | IAM Role
        v
Amazon Data Firehose
       / \
      /   \
     v     v
    S3   CloudWatch
```

---

## 5. CloudWatch Logging

Firehose tiene habilitado el logging mediante Amazon CloudWatch.

Se utilizan recursos de:

```text
CloudWatch Log Group
CloudWatch Log Stream
```

para registrar información relacionada con la entrega de datos.

Esto permite diagnosticar problemas relacionados con:

- entrega de registros;
- acceso a S3;
- permisos IAM;
- errores de procesamiento de Firehose.

---

## 6. Despliegue con Terraform

Desde el directorio correspondiente al módulo:

```bash
cd modules/kinesis
```

se inicializa Terraform:

```bash
terraform init
```

Se formatea la configuración:

```bash
terraform fmt
```

Se valida:

```bash
terraform validate
```

Se genera el plan:

```bash
terraform plan
```

Finalmente, se despliega la infraestructura:

```bash
terraform apply
```

---


## 8. Validación de datos en Amazon S3

Una vez enviado el evento, Firehose lo consume desde Kinesis y posteriormente lo entrega al bucket S3 de acuerdo con las condiciones de buffering configuradas.

La existencia de los objetos generados puede comprobarse mediante la consola de AWS


### Evidencia

Se incluyen capturas de pantalla de la consola de Amazon S3 mostrando los objetos generados por Firehose y las configuraciones realizadas 

![Kinesis Stream1](./imagenes/kinesis4.jpg)

> **Evidencia de ejecución:** reemplazar este texto por la captura de pantalla o la salida obtenida durante la prueba.

![Kinesis Stream2](./imagenes/kinesis1.jpg)

![Kinesis Stream3](./imagenes/kinesis2.jpg)

![Kinesis Stream4](./imagenes/kinesis3.jpg)

![Kinesis Stream5](./imagenes/kinesis4.jpg)

![Kinesis Stream6](./imagenes/kinesis5.jpg)

![Kinesis Stream7](./imagenes/kinesis6.jpg)

Ejecución de script python para la generacion de datos
![Kinesis Stream8](./imagenes/kinesis7.jpg)

## 9. Eliminación de la infraestructura

Una vez finalizadas las pruebas, se puede eliminar la infraestructura administrada por Terraform para evitar costos innecesarios.

Primero se verifican qué recursos serán eliminados:

```bash
terraform plan -destroy
```

Luego:

```bash
terraform destroy
```

Terraform solicitará confirmación antes de eliminar los recursos.

Los recursos externos que no formen parte del Terraform State, como un bucket S3 creado manualmente o administrado por otro módulo, no serán eliminados automáticamente.

---

## Tecnologías utilizadas

| Tecnología | Función |
|---|---|
| Terraform | Infrastructure as Code |
| Amazon Kinesis Data Streams | Ingesta de eventos en streaming |
| Amazon Data Firehose | Entrega de datos |
| Amazon S3 | Data Lake / almacenamiento |
| AWS IAM | Roles y permisos |
| AWS KMS | Cifrado del stream |
| Amazon CloudWatch | Logging y monitoreo |
| AWS CLI | Pruebas y validación |
| Python / Boto3 | Productor de eventos |

---

## Resultado

La infraestructura implementa un pipeline de ingesta basado en servicios administrados de AWS, desplegado mediante Infrastructure as Code con Terraform.

La solución cumple con los siguientes requisitos:

- Kinesis Data Stream administrado mediante Terraform.
- Nombre y tags configurables mediante variables.
- Modo `PROVISIONED`.
- Al menos 2 shards.
- Cifrado KMS.
- Firehose conectado al Kinesis Data Stream.
- Amazon S3 como destino.
- Buffer de 5 MB / 60 segundos para desarrollo.
- IAM Role específico para Firehose.
- Acceso controlado a Kinesis, S3 y CloudWatch.
- Logging mediante CloudWatch.
- Validación mediante AWS CLI.
- Verificación de los objetos generados en S3.