# Pre-entrega 3 --- Plataforma de Muestreo Urbano en Tiempo Real

## Objetivo

Implementar una plataforma de procesamiento distribuido utilizando:

-   **Kubernetes** como orquestador.
-   **Apache Kafka** como bus de eventos.
-   **Apache Spark Structured Streaming** para procesamiento en tiempo
    real.
-   **Python** para simular sensores urbanos y publicar eventos.

El flujo de la solución es:

``` text
sensor_producer.py
       ↓
JSON
       ↓
Apache Kafka
       ↓
urban_sensors
(3 o más particiones)
       ↓
Spark Structured Streaming
       ↓
spark_sensor_processor.py
       ↓
Window de 1 minuto
       ↓
Agrupación por sensor_id
       ├── AVG temperature
       └── AVG air_quality_index
```

------------------------------------------------------------------------

## 1. Estructura del proyecto

``` text
pre entrega3/
│
├── README.md
│
├── k8s/
│   ├── namespace.yaml
│   ├── zookeeper-deployment.yaml
│   ├── zookeeper-service.yaml
│   ├── kafka-configmap.yaml
│   ├── kafka-deployment.yaml
│   ├── kafka-service.yaml
│   ├── kafka-topic-job.yaml
│   ├── spark-configmap.yaml
│   ├── spark-code-configmap.yaml
│   ├── spark-deployment.yaml
│   └── spark-service.yaml
│
├── scripts/
│   └── sensor_producer.py
│
├── spark/
│   └── spark_sensor_processor.py
│
├── imagenes/
│   ├── 01-kubernetes.png
│   ├── 02-kafka-topic.png
│   ├── 03-producer.png
│   └── 04-spark-output.png
│
└── evidencias/
    ├── 01-kubernetes-node.txt
    ├── 02-kafka-topic.txt
    ├── 03-kubernetes-all.txt
    └── 04-spark-output.txt
```

Los manifiestos Kubernetes se mantienen centralizados en `/k8s/`.

------------------------------------------------------------------------

# 2. Requisitos previos

Entorno utilizado:

-   Windows 10
-   Docker Desktop
-   Kubernetes habilitado en Docker Desktop
-   Python
-   OpenJDK 17
-   VS Code
-   `kubectl`

Verificar Java:

``` bash
java -version
```

Verificar Python:

``` bash
python --version
```

Verificar Kubernetes:

``` bash
kubectl version --client
kubectl cluster-info
kubectl get nodes
```

## Evidencia 1 --- Kubernetes operativo

Guardar la salida:

``` bash
kubectl get nodes > evidencias/01-kubernetes-node.txt
```

También puede incorporarse una captura:

``` markdown
![Kubernetes operativo](imagenes/01-kubernetes.png)
```

------------------------------------------------------------------------

# 3. Crear namespace

Crear el namespace dedicado:

``` bash
kubectl apply -f k8s/namespace.yaml
```

Verificar:

``` bash
kubectl get namespaces
```

Debe aparecer:

``` text
urban-streaming
```

------------------------------------------------------------------------

# 4. Levantar ZooKeeper

Aplicar:

``` bash
kubectl apply -f k8s/zookeeper-deployment.yaml
kubectl apply -f k8s/zookeeper-service.yaml
```

Esperar hasta que esté disponible:

``` bash
kubectl wait --for=condition=available deployment/zookeeper -n urban-streaming --timeout=120s
```

Verificar:

``` bash
kubectl get pods -n urban-streaming
```

ZooKeeper debe aparecer en estado `Running`.

------------------------------------------------------------------------

# 5. Levantar Kafka

Aplicar primero el ConfigMap:

``` bash
kubectl apply -f k8s/kafka-configmap.yaml
```

Luego:

``` bash
kubectl apply -f k8s/kafka-deployment.yaml
kubectl apply -f k8s/kafka-service.yaml
```

Esperar:

``` bash
kubectl wait --for=condition=available deployment/kafka -n urban-streaming --timeout=180s
```

Verificar:

``` bash
kubectl get pods -n urban-streaming
kubectl get services -n urban-streaming
```

Si Kafka no inicia correctamente:

``` bash
kubectl logs deployment/kafka -n urban-streaming
```

------------------------------------------------------------------------

# 6. Crear tópico `urban_sensors`

La consigna requiere un tópico:

``` text
urban_sensors
```

con al menos:

``` text
3 particiones
```

Crear mediante el Job:

``` bash
kubectl apply -f k8s/kafka-topic-job.yaml
```

Verificar:

``` bash
kubectl get jobs -n urban-streaming
```

Consultar el resultado:

``` bash
kubectl logs job/create-urban-sensors-topic -n urban-streaming
```

La salida debe permitir verificar:

``` text
Topic: urban_sensors
PartitionCount: 3
```

## Evidencia 2 --- tópico Kafka

Guardar:

``` bash
kubectl logs job/create-urban-sensors-topic -n urban-streaming > evidencias/02-kafka-topic.txt
```

Captura sugerida:

``` markdown
![Tópico Kafka urban_sensors](images/02-kafka-topic.png)
```

------------------------------------------------------------------------

# 7. Productor de sensores

El archivo:

``` text
scripts/sensor_producer.py
```

simula cinco sensores urbanos y publica eventos en Kafka.

Cada evento contiene:

``` json
{
  "sensor_id": "sensor_zona_3",
  "temperature": 25.4,
  "humidity": 61.2,
  "air_quality_index": 84,
  "timestamp": "2026-08-18 10:25:34"
}
```

Los campos requeridos son:

  Campo                 Tipo esperado
  --------------------- ------------------
  `sensor_id`           String
  `temperature`         Float
  `humidity`            Float
  `air_quality_index`   Integer
  `timestamp`           String/Timestamp

El productor utiliza `sensor_id` como **key de Kafka**, de modo que los
eventos correspondientes a un mismo sensor mantengan una asignación
consistente de partición.

La generación se realiza aproximadamente cada 500 ms.

## Corrección aplicada

El valor de `air_quality_index` debe ser numérico:

``` python
"air_quality_index": random.randint(0, 200),
```

y no:

``` python
"air_quality_index": "Error",
```

porque Spark lo interpreta mediante `IntegerType()`.

------------------------------------------------------------------------

# 8. Procesamiento con Spark Structured Streaming

El archivo:

``` text
spark/spark_sensor_processor.py
```

consume el tópico:

``` text
urban_sensors
```

y realiza:

``` text
Kafka
  ↓
readStream
  ↓
Parse JSON
  ↓
Conversión de timestamp
  ↓
Window de 1 minuto
  ↓
GROUP BY sensor_id
  ↓
AVG temperature
AVG air_quality_index
```

## Ventana requerida

La consigna requiere una ventana de **1 minuto**.

Por lo tanto:

``` python
window(col("event_time"), "1 minute")
```

El procesamiento esperado es:

``` python
aggregated_metrics = parsed_stream \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("sensor_id")
    ) \
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("air_quality_index").alias("avg_air_quality")
    )
```

------------------------------------------------------------------------

# 9. Configuración de Spark mediante ConfigMap

Aplicar:

``` bash
kubectl apply -f k8s/spark-configmap.yaml
kubectl apply -f k8s/spark-code-configmap.yaml
```

El ConfigMap proporciona al proceso Spark, entre otras, las variables:

``` text
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_TOPIC=urban_sensors
```

Estas variables son utilizadas por `spark_sensor_processor.py`.

------------------------------------------------------------------------

# 10. Levantar Spark

Aplicar:

``` bash
kubectl apply -f k8s/spark-deployment.yaml
kubectl apply -f k8s/spark-service.yaml
```

Verificar:

``` bash
kubectl get pods -n urban-streaming
```

El objetivo es observar:

``` text
zookeeper-...        1/1   Running
kafka-...            1/1   Running
spark-streaming-...  1/1   Running
```

Verificar todos los recursos:

``` bash
kubectl get all -n urban-streaming
```

## Evidencia 3 --- infraestructura completa

Guardar:

``` bash
kubectl get all -n urban-streaming > evidencias/03-kubernetes-all.txt
```

------------------------------------------------------------------------

# 11. Abrir los logs de Spark

Antes de ejecutar el productor, abrir una terminal independiente:

``` bash
kubectl logs -f deployment/spark-streaming -n urban-streaming
```

Mantener esta terminal abierta.

Spark quedará esperando eventos nuevos de:

``` text
urban_sensors
```

------------------------------------------------------------------------

# 12. Exponer Kafka hacia Windows

El productor se ejecuta desde Windows y utiliza por defecto:

``` text
localhost:9092
```

Kafka se encuentra dentro de Kubernetes.

Abrir otra terminal y ejecutar:

``` bash
kubectl port-forward service/kafka 9092:9092 -n urban-streaming
```

Debe aparecer:

``` text
Forwarding from 127.0.0.1:9092 -> 9092
```

Mantener esta terminal abierta mientras se ejecuta el productor.

Flujo:

``` text
sensor_producer.py
       ↓
localhost:9092
       ↓
kubectl port-forward
       ↓
Kafka Service
       ↓
Kafka Pod
```

------------------------------------------------------------------------

# 13. Ejecutar el productor

En otra terminal:

``` bash
python scripts/sensor_producer.py
```

La consola debe comenzar a mostrar eventos similares a:

``` text
📡 Evento emitido a Kafka:
{
  'sensor_id': 'sensor_zona_3',
  'temperature': 27.34,
  'humidity': 61.21,
  'air_quality_index': 84,
  'timestamp': '2026-08-18 10:25:34'
}
```

Dejarlo ejecutar durante aproximadamente **90 a 120 segundos** para
generar suficientes datos para varias ventanas.

## Evidencia 4 --- productor

Realizar una captura y guardarla como:

``` text
images/03-producer.png
```

Agregar al README:

``` markdown
![Eventos enviados a Kafka](images/03-producer.png)
```

------------------------------------------------------------------------

# 14. Verificar Kafka → Spark

Volver a la terminal que contiene:

``` bash
kubectl logs -f deployment/spark-streaming -n urban-streaming
```

Spark debe mostrar las agregaciones correspondientes a los eventos
recibidos.

Ejemplo orientativo:

``` text
+------------------------------------------+-------------+---------------+---------------+
|window                                    |sensor_id    |avg_temperature|avg_air_quality|
+------------------------------------------+-------------+---------------+---------------+
|{2026-08-18 10:25, 2026-08-18 10:26}     |sensor_zona_1|24.83          |72.4           |
|{2026-08-18 10:25, 2026-08-18 10:26}     |sensor_zona_2|27.15          |63.8           |
|{2026-08-18 10:25, 2026-08-18 10:26}     |sensor_zona_3|25.72          |81.2           |
+------------------------------------------+-------------+---------------+---------------+
```

> El cuadro anterior es solamente ilustrativo. En la entrega debe
> utilizarse la salida real obtenida.

Guardar los logs:

``` bash
kubectl logs deployment/spark-streaming -n urban-streaming > evidencias/04-spark-output.txt
```

## Evidencia 5 --- procesamiento Spark

Captura:

``` text
images/04-spark-output.png
```

Agregar:

``` markdown
![Procesamiento Spark Structured Streaming](images/04-spark-output.png)
```

Esta es la evidencia principal de la comunicación:

``` text
sensor_producer.py
       ↓
Kafka / urban_sensors
       ↓
Spark Structured Streaming
       ↓
Window 1 minuto
       ↓
AVG temperature
AVG air_quality_index
```

------------------------------------------------------------------------

# 15. Orden completo de ejecución

``` text
1. Iniciar Docker Desktop
          ↓
2. Verificar Kubernetes
          ↓
3. Crear namespace
          ↓
4. Levantar ZooKeeper
          ↓
5. Levantar Kafka
          ↓
6. Crear urban_sensors (3 particiones)
          ↓
7. Levantar Spark
          ↓
8. Verificar Pods/Services
          ↓
9. Abrir logs de Spark
          ↓
10. Port-forward Kafka 9092
          ↓
11. Ejecutar sensor_producer.py
          ↓
12. Esperar 90–120 segundos
          ↓
13. Verificar resultados Spark
          ↓
14. Guardar logs y capturas
```

------------------------------------------------------------------------

# 16. Evidencias de la entrega

  ------------------------------------------------------------------------------------
  Evidencia               Qué demuestra           Archivo sugerido
  ----------------------- ----------------------- ------------------------------------
  Kubernetes              Cluster operativo       `images/01-kubernetes.png`

  Kafka Topic             `urban_sensors` y 3     `images/02-kafka-topic.png`
                          particiones             

  Producer                Eventos JSON enviados   `images/03-producer.png`

  Spark                   Procesamiento y         `images/04-spark-output.png`
                          agregaciones            

  Kubernetes TXT          Recursos desplegados    `evidencias/03-kubernetes-all.txt`

  Kafka TXT               Configuración del       `evidencias/02-kafka-topic.txt`
                          tópico                  

  Spark TXT               Resultado del           `evidencias/04-spark-output.txt`
                          procesamiento           
  ------------------------------------------------------------------------------------

------------------------------------------------------------------------

# 17. Seguridad

Antes de realizar el commit verificar:

-   No incluir passwords.
-   No incluir tokens.
-   No incluir credenciales.
-   No incluir secretos hardcoded.
-   No incluir archivos comprimidos en la entrega final.
-   No incluir carpetas locales generadas por herramientas.
-   Mantener las variables de configuración de Kafka y Spark en
    ConfigMaps cuando corresponda.

------------------------------------------------------------------------

# 18. Checklist de validación

## Kubernetes

-   [ ] Docker Desktop operativo.
-   [ ] Kubernetes habilitado.
-   [ ] Namespace dedicado creado.
-   [ ] YAML organizados en `/k8s/`.
-   [ ] ZooKeeper en ejecución.
-   [ ] Kafka en ejecución.
-   [ ] Spark en ejecución.
-   [ ] Services definidos.
-   [ ] ConfigMaps utilizados para Kafka y Spark.

## Kafka

-   [ ] Existe `urban_sensors`.
-   [ ] Tiene al menos 3 particiones.
-   [ ] El productor logra conectarse.
-   [ ] Los eventos son publicados correctamente.

## Productor

-   [ ] `sensor_producer.py` funciona.
-   [ ] `sensor_id` presente.
-   [ ] `temperature` presente.
-   [ ] `humidity` presente.
-   [ ] `air_quality_index` es Integer.
-   [ ] `timestamp` presente.
-   [ ] `sensor_id` se utiliza como Kafka key.

## Spark

-   [ ] `spark_sensor_processor.py` funciona.
-   [ ] Consume `urban_sensors`.
-   [ ] Deserializa JSON.
-   [ ] Convierte `timestamp`.
-   [ ] Utiliza ventana de 1 minuto.
-   [ ] Agrupa por `sensor_id`.
-   [ ] Calcula `AVG temperature`.
-   [ ] Calcula `AVG air_quality_index`.
-   [ ] Muestra resultados en consola/logs.

## Evidencias

-   [ ] Captura Kubernetes.
-   [ ] Evidencia tópico Kafka.
-   [ ] Captura del productor.
-   [ ] Captura de Spark.
-   [ ] Logs guardados.
-   [ ] README actualizado con outputs reales.

------------------------------------------------------------------------

# 19. Detener la simulación

Detener el productor:

``` text
Ctrl+C
```

Detener el `port-forward`:

``` text
Ctrl+C
```

------------------------------------------------------------------------

# 20. Limpieza del entorno

Eliminar los recursos:

``` bash
kubectl delete -f k8s/
```

Alternativamente, si todos los recursos pertenecen exclusivamente al
namespace:

``` bash
kubectl delete namespace urban-streaming
```

Verificar:

``` bash
kubectl get namespaces
```

------------------------------------------------------------------------

# 21. Publicación en GitHub

Se recomienda desarrollar la Pre-entrega 3 en:

``` text
feature/urban-streaming
```

Flujo:

``` text
main
  ↓
feature/urban-streaming
  ↓
desarrollo
  ↓
pruebas
  ↓
evidencias
  ↓
Commit
  ↓
Publish branch / Push origin
  ↓
Pull Request
  ↓
main
```

En GitHub Desktop:

1.  Partir desde `main`.
2.  Crear `feature/urban-streaming`.
3.  Incorporar código, YAML, README y evidencias.
4.  Revisar **Changes**.
5.  Realizar el commit.
6.  Seleccionar **Publish branch** o **Push origin**.
7.  Crear el Pull Request hacia `main`.
8.  Conservar la URL del repositorio y del Pull Request.

------------------------------------------------------------------------

# Resultado esperado

La implementación final debe demostrar:

``` text
Python sensor_producer.py
          ↓
       eventos JSON
          ↓
       Apache Kafka
          ↓
 urban_sensors / 3+ partitions
          ↓
 Spark Structured Streaming
          ↓
  ventana de 1 minuto
          ↓
     sensor_id
       ├── AVG temperature
       └── AVG air_quality_index
```

El README debe completarse con **outputs y capturas reales obtenidos
durante la ejecución**, de modo que permita reproducir y verificar el
entorno.
