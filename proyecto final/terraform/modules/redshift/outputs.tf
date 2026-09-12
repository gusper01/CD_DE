output "namespace_name" {
  description = "Nombre del namespace Redshift Serverless"
  value       = aws_redshiftserverless_namespace.lakehouse.namespace_name
}

output "workgroup_name" {
  description = "Nombre del workgroup Redshift Serverless"
  value       = aws_redshiftserverless_workgroup.lakehouse.workgroup_name
}

output "workgroup_endpoint" {
  description = "Endpoint de Redshift Serverless"
  value       = aws_redshiftserverless_workgroup.lakehouse.endpoint
}

output "iam_role_arn" {
  description = "ARN del rol IAM utilizado por Redshift"
  value       = aws_iam_role.redshift_streaming.arn
}
