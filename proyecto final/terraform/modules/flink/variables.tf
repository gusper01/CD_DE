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

variable "lakehouse_bucket_arn" {
  description = "ARN del bucket S3 usado como Lakehouse Iceberg"
  type        = string
  default     = null
}

variable "glue_database_name" {
  description = "Nombre de la base de datos de AWS Glue usada por Iceberg"
  type        = string
  default     = null
}

variable "python_file" {
  description = "Archivo Python principal que ejecuta Managed Flink"
  type        = string
  default     = "urban_flink.py"
}

variable "lakehouse_bucket_name" {
  description = "Nombre del bucket S3 usado como Lakehouse Iceberg"
  type        = string
  default     = null
}

variable "application_mode" {
  description = "Modo de ejecución de la aplicación Flink: pyflink o java"
  type        = string
  default     = "pyflink"

  validation {
    condition     = contains(["pyflink", "java"], var.application_mode)
    error_message = "application_mode debe ser 'pyflink' o 'java'."
  }
}
variable "processed_stream_name" {
  description = "Nombre del stream Kinesis que recibe la salida procesada por Flink"
  type        = string
}

variable "processed_stream_arn" {
  description = "ARN del stream Kinesis que recibe la salida procesada por Flink"
  type        = string
}

variable "aws_region" {
  description = "Region AWS del pipeline"
  type        = string
  default     = "us-east-1"
}
