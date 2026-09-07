output "redshift_namespace_name" {
  value = aws_redshiftserverless_namespace.lakehouse.namespace_name
}

output "redshift_workgroup_name" {
  value = aws_redshiftserverless_workgroup.lakehouse.workgroup_name
}

output "redshift_workgroup_endpoint" {
  value = aws_redshiftserverless_workgroup.lakehouse.endpoint
}

output "redshift_iam_role_arn" {
  value = aws_iam_role.redshift_streaming.arn
}