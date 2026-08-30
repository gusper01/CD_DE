variable "environment" {
  description = "Ambiente de despliegue"
  type        = string
  default     = "dev"
}

variable "application_name" {
  description = "Nombre de la aplicación Managed Flink"
  type        = string
}

variable "kinesis_stream_arn" {
  description = "ARN del Kinesis Data Stream de entrada"
  type        = string
}

variable "kinesis_stream_name" {
  description = "Nombre del Kinesis Data Stream de entrada"
  type        = string
}

variable "artifact_bucket_arn" {
  description = "ARN del bucket S3 que contiene el código PyFlink"
  type        = string
}

variable "artifact_bucket_name" {
  description = "Nombre del bucket S3 que contiene el código PyFlink"
  type        = string
}

variable "artifact_key" {
  description = "Key del ZIP PyFlink dentro de S3"
  type        = string
  default     = "flink/urban_flink.zip"
}