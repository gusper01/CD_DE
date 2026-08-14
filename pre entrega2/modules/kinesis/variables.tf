# ------------------------------------------------------------------------------
variable "aws_region" {
  type        = string
  description = "Región de AWS donde se alojarán el Bucket S3 y DynamoDB del backend"
  default     = "us-east-1"
}
variable "environment" {
  type        = string
  description = "Ambiente asociado a la infraestructura del backend"
  default     = "dev"
}

variable "stream_name" {
  type        = string
  description = "Nombre del kinesis data stream"
  default     = "transacciones"
}

variable "shard_count" {
  type        = number
  description = "Cantidad de shards (2 mb/s de entrada => 2 shards)"
  default     = 2
}

variable "bucket_name" {
  type        = string
  description = "bucket s3 destino de firehose (el del modulo1)"

}

variable "buffer_size_mb" {
  type        = number
  description = "tamaño del buffer de firehose en MB"
  default     = 5

}

variable "buffer_interval_sec" {
  type        = number
  description = "intervalo del buffer de firehorse en segundos"
  default     = 60

}
