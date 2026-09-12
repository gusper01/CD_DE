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

variable "processed_stream_arn" {
  description = "ARN del stream Kinesis que contiene la salida procesada por Flink"
  type        = string
}

variable "glue_database_name" {
  description = "Nombre de la base de datos de AWS Glue"
  type        = string
}

variable "lakehouse_bucket_arn" {
  description = "ARN del bucket S3 utilizado por el Lakehouse"
  type        = string
}

variable "vpc_id" {
  description = "VPC utilizada por Redshift Serverless"
  type        = string
}

variable "vpc_cidr_block" {
  description = "CIDR de la VPC"
  type        = string
}

variable "subnet_ids" {
  description = "Subnets utilizadas por Redshift Serverless y el endpoint de Kinesis"
  type        = list(string)
}

variable "redshift_admin_password" {
  description = "Password del administrador de Redshift Serverless"
  type        = string
  sensitive   = true
}

variable "redshift_admin_username" {
  description = "Usuario administrador de Redshift"
  type        = string
  default     = "admin_lakehouse"
}

variable "redshift_database_name" {
  description = "Base de datos inicial de Redshift"
  type        = string
  default     = "dev"
}

variable "redshift_base_capacity" {
  description = "Capacidad base de Redshift Serverless en RPUs"
  type        = number
  default     = 8
}

variable "redshift_max_capacity" {
  description = "Capacidad maxima de Redshift Serverless en RPUs"
  type        = number
  default     = 8
}

variable "publicly_accessible" {
  description = "Indica si Redshift Serverless es p�blicamente accesible"
  type        = bool
  default     = false
}
