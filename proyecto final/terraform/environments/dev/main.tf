# ==============================================================================
# NETWORK
# ==============================================================================

module "network" {
  source = "../../modules/network"

  project_name   = var.project_name
  environment    = var.environment
  vpc_cidr_block = var.vpc_cidr_block
  subnet_count   = var.subnet_count
}

# ==============================================================================
# LAKEHOUSE
# ==============================================================================

module "lakehouse" {
  source = "../../modules/lakehouse"

  project_name       = var.project_name
  environment        = var.environment
  glue_database_name = var.glue_database_name
  force_destroy      = true
}

# ==============================================================================
# KINESIS RAW + PROCESSED
# ==============================================================================

module "kinesis" {
  source = "../../modules/kinesis"

  environment = var.environment

  raw_stream_name       = var.raw_stream_name
  processed_stream_name = var.processed_stream_name

  raw_shard_count       = var.raw_shard_count
  processed_shard_count = var.processed_shard_count

  retention_hours = var.kinesis_retention_hours

  bucket_name = module.lakehouse.artifact_bucket_name
}

# ==============================================================================
# FLINK JAR
# Terraform administra tambien la carga del artefacto.
# ==============================================================================

resource "aws_s3_object" "flink_jar" {
  bucket = module.lakehouse.artifact_bucket_name
  key    = var.flink_artifact_key

  source = "${path.module}/../../../flink-app/target/urban-streaming-flink.jar"

  etag = filemd5(
    "${path.module}/../../../flink-app/target/urban-streaming-flink.jar"
  )

  tags = {
    Component = "flink-application"
  }
}

# ==============================================================================
# MANAGED SERVICE FOR APACHE FLINK
# ==============================================================================

module "flink" {
  source = "../../modules/flink"

  environment      = var.environment
  application_name = var.flink_application_name

  # Source RAW
  kinesis_stream_name = module.kinesis.raw_stream_name
  kinesis_stream_arn  = module.kinesis.raw_stream_arn

  # Sink PROCESSED
  processed_stream_name = module.kinesis.processed_stream_name
  processed_stream_arn  = module.kinesis.processed_stream_arn

  # Artefacto Java
  artifact_bucket_name = module.lakehouse.artifact_bucket_name
  artifact_bucket_arn  = module.lakehouse.artifact_bucket_arn
  artifact_key         = aws_s3_object.flink_jar.key

  application_mode = "java"

  # Lakehouse
  lakehouse_bucket_name = module.lakehouse.lakehouse_bucket_name
  lakehouse_bucket_arn  = module.lakehouse.lakehouse_bucket_arn
  glue_database_name    = module.lakehouse.glue_database_name

  aws_region = var.aws_region

  depends_on = [
    aws_s3_object.flink_jar
  ]
}

# ==============================================================================
# REDSHIFT SERVERLESS
# ==============================================================================

module "redshift" {
  source = "../../modules/redshift"

  project_name = var.project_name
  environment  = var.environment

  # Redshift consume la salida procesada por Flink
  processed_stream_arn = module.kinesis.processed_stream_arn

  glue_database_name   = module.lakehouse.glue_database_name
  lakehouse_bucket_arn = module.lakehouse.lakehouse_bucket_arn

  vpc_id         = module.network.vpc_id
  vpc_cidr_block = module.network.vpc_cidr_block
  subnet_ids     = module.network.private_subnet_ids

  redshift_admin_password = var.redshift_admin_password

  redshift_base_capacity = var.redshift_base_capacity
  redshift_max_capacity  = var.redshift_max_capacity

  publicly_accessible = var.redshift_publicly_accessible
}
