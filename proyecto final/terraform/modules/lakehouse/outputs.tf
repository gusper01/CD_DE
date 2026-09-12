output "artifact_bucket_name" {
  description = "Nombre del bucket RAW utilizado tambien para artefactos de Flink"
  value       = aws_s3_bucket.raw.bucket
}

output "artifact_bucket_arn" {
  description = "ARN del bucket RAW / artefactos"
  value       = aws_s3_bucket.raw.arn
}

output "lakehouse_bucket_name" {
  description = "Nombre del bucket Lakehouse"
  value       = aws_s3_bucket.lakehouse.bucket
}

output "lakehouse_bucket_arn" {
  description = "ARN del bucket Lakehouse"
  value       = aws_s3_bucket.lakehouse.arn
}

output "glue_database_name" {
  description = "Nombre de la base Glue"
  value       = aws_glue_catalog_database.lakehouse.name
}
