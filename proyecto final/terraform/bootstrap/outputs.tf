output "state_bucket_name" {
  description = "Nombre del bucket S3 utilizado como backend remoto"
  value       = aws_s3_bucket.tf_state.bucket
}



output "aws_region" {
  description = "Region del backend Terraform"
  value       = var.aws_region
}
