#. Configurar nube
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~>5.0"
    }
  }
}

# configurar region
provider "aws" {
  region = "us-east-1"

}

# configurar recursos bucket datos crudos
resource "aws_s3_bucket" "data_lake_rw" {
  #  bucket = "coderhouse-datalake-raw-prueba-semana1-gusper"
  bucket        = "coderhouse-urban-streaming-raw-gusper-dev"
  force_destroy = true
  tags = {
    Environment = "dev"
    Project     = "urban-streaming"
    ManagedBy   = "Terraform"
  }
}

# Modulo Ingesta pre entrega 2

# ------------------------------------

# MÓDULO DE INGESTA DE DATOS (KINESIS)
# ------------------------------------

module "kinesis" {
  source      = "../../modules/kinesis"
  environment = "dev"

  stream_name = "urban-sensors-dev"
  shard_count = 2

  bucket_name = aws_s3_bucket.data_lake_rw.bucket

  # Buffering agresivo para ver resultados rápido en dev
  #buffer_size_mb      = 5
  #buffer_interval_sec = 60
}

module "flink" {
  source = "../../modules/flink"

  environment      = "dev"
  application_name = "urban-stream-processing-dev"

  kinesis_stream_name = module.kinesis.stream_name
  kinesis_stream_arn  = module.kinesis.stream_arn

  artifact_bucket_name = aws_s3_bucket.data_lake_rw.bucket
  artifact_bucket_arn  = aws_s3_bucket.data_lake_rw.arn

  artifact_key = "flink/urban_flink.zip"
}

# ------------------------------------------------------------------------------
# OUTPUTS
# ------------------------------------------------------------------------------
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
