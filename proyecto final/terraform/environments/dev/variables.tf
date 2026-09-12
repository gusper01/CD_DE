variable "project_name" {
  description = "Nombre base del proyecto"
  type        = string
  default     = "urban-streaming"
}

variable "environment" {
  description = "Entorno de despliegue"
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "Region AWS"
  type        = string
  default     = "us-east-1"
}

# ==============================================================================
# NETWORK
# ==============================================================================

variable "vpc_cidr_block" {
  description = "CIDR de la VPC"
  type        = string
  default     = "10.20.0.0/16"
}

variable "subnet_count" {
  description = "Cantidad de subnets privadas"
  type        = number
  default     = 3
}

# ==============================================================================
# KINESIS
# ==============================================================================

variable "raw_stream_name" {
  description = "Stream de eventos originales"
  type        = string
  default     = "urban-sensors-raw-dev"
}

variable "processed_stream_name" {
  description = "Stream de eventos procesados por Flink"
  type        = string
  default     = "urban-sensors-processed-dev"
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

variable "kinesis_retention_hours" {
  description = "Retencion de ambos streams Kinesis"
  type        = number
  default     = 24
}

# ==============================================================================
# LAKEHOUSE
# ==============================================================================

variable "glue_database_name" {
  description = "Base Glue del Lakehouse"
  type        = string
  default     = "lakehouse_db"
}

# ==============================================================================
# FLINK
# ==============================================================================

variable "flink_application_name" {
  description = "Nombre de la aplicacion Managed Flink"
  type        = string
  default     = "urban-stream-processing-dev"
}

variable "flink_artifact_key" {
  description = "Key del JAR dentro del bucket de artefactos"
  type        = string
  default     = "flink/urban-streaming-flink.jar"
}

# ==============================================================================
# REDSHIFT
# ==============================================================================

variable "redshift_admin_password" {
  description = "Password del administrador Redshift Serverless"
  type        = string
  sensitive   = true
}

variable "redshift_base_capacity" {
  description = "Capacidad base Redshift Serverless"
  type        = number
  default     = 8
}

variable "redshift_max_capacity" {
  description = "Capacidad maxima Redshift Serverless"
  type        = number
  default     = 8
}

variable "redshift_publicly_accessible" {
  description = "Exposicion publica de Redshift"
  type        = bool
  default     = false
}
