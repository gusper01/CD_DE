variable "environment" {
  description = "Entorno de despliegue"
  type        = string
  default     = "dev"
}

variable "raw_stream_name" {
  description = "Nombre del stream Kinesis que recibe los eventos originales"
  type        = string
}

variable "processed_stream_name" {
  description = "Nombre del stream Kinesis que recibe la salida procesada por Flink"
  type        = string
}

variable "raw_shard_count" {
  description = "Cantidad de shards del stream RAW"
  type        = number
  default     = 2
}

variable "processed_shard_count" {
  description = "Cantidad de shards del stream PROCESSED"
  type        = number
  default     = 2
}

variable "retention_hours" {
  description = "Retencion de los streams Kinesis en horas"
  type        = number
  default     = 24
}

variable "bucket_name" {
  description = "Bucket S3 destino del Firehose que archiva el stream RAW"
  type        = string
}

variable "buffer_size_mb" {
  description = "Tamanio del buffer de Firehose en MB"
  type        = number
  default     = 5
}

variable "buffer_interval_sec" {
  description = "Intervalo del buffer de Firehose en segundos"
  type        = number
  default     = 60
}

variable "shard_level_metrics" {
  description = "Metricas de monitoreo habilitadas a nivel de shard"
  type        = list(string)

  default = [
    "IncomingBytes",
    "IncomingRecords",
    "OutgoingBytes",
    "OutgoingRecords",
    "ReadProvisionedThroughputExceeded",
    "WriteProvisionedThroughputExceeded",
    "IteratorAgeMilliseconds"
  ]
}
