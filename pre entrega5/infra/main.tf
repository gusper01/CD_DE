# ==============================================================================
# PRE-ENTREGA 5
# Pipeline Lakehouse con Apache Iceberg + AWS Glue
# ==============================================================================

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}

# ==============================================================================
# S3 - BUCKET RAW + ARTEFACTO FLINK
# ==============================================================================

resource "aws_s3_bucket" "data_lake_raw" {
  bucket        = "coderhouse-urban-streaming-raw-gusper-dev"
  force_destroy = true

  tags = {
    Environment = "dev"
    Project     = "urban-streaming"
    Layer       = "raw"
    ManagedBy   = "Terraform"
  }
}

# ==============================================================================
# S3 - LAKEHOUSE ICEBERG
# ==============================================================================

resource "aws_s3_bucket" "lakehouse" {
  bucket        = "coderhouse-urban-lakehouse-gusper-dev"
  force_destroy = true

  tags = {
    Environment = "dev"
    Project     = "urban-streaming"
    Layer       = "lakehouse"
    ManagedBy   = "Terraform"
  }
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  versioning_configuration {
    status = "Enabled"
  }
}

# ==============================================================================
# AWS GLUE DATA CATALOG
# ==============================================================================

resource "aws_glue_catalog_database" "lakehouse" {
  name        = "lakehouse_db"
  description = "Catalogo Glue para tablas Apache Iceberg del pipeline urbano"
}

# ==============================================================================
# KINESIS + FIREHOSE
# Reutilizamos el modulo desarrollado en las entregas anteriores
# ==============================================================================

module "kinesis" {
  source = "../../pre entrega2/modules/kinesis"

  environment = "dev"

  stream_name = "urban-sensors-dev"
  shard_count = 2

  bucket_name = aws_s3_bucket.data_lake_raw.bucket
}

# ==============================================================================
# MANAGED SERVICE FOR APACHE FLINK
# ==============================================================================

module "flink" {
  source = "../../pre entrega2/modules/flink"

  environment         = "dev"
  application_name    = "urban-stream-processing-dev"
  application_mode    = "java"
  kinesis_stream_name = module.kinesis.stream_name
  kinesis_stream_arn  = module.kinesis.stream_arn

  artifact_bucket_name = aws_s3_bucket.data_lake_raw.bucket
  artifact_bucket_arn  = aws_s3_bucket.data_lake_raw.arn

  #artifact_key = "flink/urban_flink.zip"
  artifact_key = "flink/urban-streaming-flink-v3.jar"
  #python_file  = "urban_flink_iceberg.py"
  # Pre-entrega 5: acceso al Lakehouse y Glue
  #lakehouse_bucket_arn = aws_s3_bucket.lakehouse.arn
  #glue_database_name   = aws_glue_catalog_database.lakehouse.name
  lakehouse_bucket_name = aws_s3_bucket.lakehouse.bucket
  lakehouse_bucket_arn  = aws_s3_bucket.lakehouse.arn
  glue_database_name    = aws_glue_catalog_database.lakehouse.name
}

# ==============================================================================
# OUTPUTS
# ==============================================================================

output "stream_arn" {
  value = module.kinesis.stream_arn
}

output "stream_name" {
  value = module.kinesis.stream_name
}

output "firehose_arn" {
  value = module.kinesis.firehose_arn
}

output "firehose_name" {
  value = module.kinesis.firehose_name
}

output "flink_application_name" {
  value = "urban-stream-processing-dev"
}

output "artifact_bucket_name" {
  value = aws_s3_bucket.data_lake_raw.bucket
}

output "lakehouse_bucket_name" {
  value = aws_s3_bucket.lakehouse.bucket
}

output "lakehouse_bucket_arn" {
  value = aws_s3_bucket.lakehouse.arn
}

output "glue_database_name" {
  value = aws_glue_catalog_database.lakehouse.name
}