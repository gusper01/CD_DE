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

variable "glue_database_name" {
  description = "Nombre de la base de datos de AWS Glue"
  type        = string
  default     = "lakehouse_db"
}

variable "force_destroy" {
  description = "Permite eliminar buckets con objetos. Solo recomendado para entornos de laboratorio/dev."
  type        = bool
  default     = true
}
