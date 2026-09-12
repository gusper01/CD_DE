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

variable "vpc_cidr_block" {
  description = "CIDR principal de la VPC"
  type        = string
  default     = "10.20.0.0/16"
}

variable "subnet_count" {
  description = "Cantidad de subnets privadas"
  type        = number
  default     = 3
}
