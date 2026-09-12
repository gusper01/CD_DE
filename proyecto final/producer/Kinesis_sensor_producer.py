import json
import time
import random
import os
from datetime import datetime, timezone
import boto3

# 1. LECTURA DE CONFIGURACIÓN
# Se obtienen los parámetros desde las variables del sistema (ConfigMap)
STREAM_NAME = os.getenv("KINESIS_STREAM_NAME", "urban-sensors-dev")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


# 2. CLIENTE KINESIS
kinesis = boto3.client(
    "kinesis",
    region_name=AWS_REGION
)

sensores = [f"sensor_zona_{i}" for i in range(1, 6)] # Genera 5 IDs de sensores fijos

try:
    while True:
        sensor_id = random.choice(sensores) # Selección aleatoria para simular concurrencia
        
        payload = {
            "sensor_id": sensor_id,
            "temperature": round(random.uniform(15.0, 38.0), 2),
            "humidity": round(random.uniform(30.0, 90.0), 2),
            #"air_quality_index": "Error",
            "air_quality_index": random.randint(0, 200),
             "event_time": datetime.now(
                timezone.utc
            ).strftime("%Y-%m-%d %H:%M:%S")
           # "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
           # "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        }
        
        response = kinesis.put_record(
                StreamName=STREAM_NAME,
                Data=json.dumps(
                    payload
                ).encode("utf-8"),
                PartitionKey=sensor_id
            )
        print(f"Evento emitido a Kinesis: {payload}")
        time.sleep(0.5)
      
        
except KeyboardInterrupt:
    print("Deteniendo productor...")
