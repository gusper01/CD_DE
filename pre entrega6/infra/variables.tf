
variable "redshift_admin_password" {
  description = "Password del usuario administrador de Redshift Serverless"
  type        = string
  sensitive   = true
}