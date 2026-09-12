output "application_name" {
  description = "Nombre de la aplicación Managed Flink"
  value       = aws_kinesisanalyticsv2_application.main.name
}

output "application_arn" {
  description = "ARN de la aplicación Managed Flink"
  value       = aws_kinesisanalyticsv2_application.main.arn
}

output "application_status" {
  description = "Estado de la aplicación Managed Flink"
  value       = aws_kinesisanalyticsv2_application.main.status
}