variable "aws_region" {
  description = "Region AWS"
  type        = string
  default     = "us-east-1"
}

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

variable "backend_force_destroy" {
  description = "Permite destruir el bucket de estado aunque tenga objetos"
  type        = bool
  default     = false
}
