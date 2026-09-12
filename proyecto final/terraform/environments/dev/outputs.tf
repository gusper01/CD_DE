output "vpc_id" {
  value = module.network.vpc_id
}

output "private_subnet_ids" {
  value = module.network.private_subnet_ids
}

output "raw_stream_name" {
  value = module.kinesis.raw_stream_name
}

output "processed_stream_name" {
  value = module.kinesis.processed_stream_name
}

output "flink_application_name" {
  value = module.flink.application_name
}

output "glue_database_name" {
  value = module.lakehouse.glue_database_name
}

output "lakehouse_bucket_name" {
  value = module.lakehouse.lakehouse_bucket_name
}

output "artifact_bucket_name" {
  value = module.lakehouse.artifact_bucket_name
}

output "redshift_namespace_name" {
  value = module.redshift.namespace_name
}

output "redshift_workgroup_name" {
  value = module.redshift.workgroup_name
}

output "redshift_workgroup_endpoint" {
  value = module.redshift.workgroup_endpoint
}
