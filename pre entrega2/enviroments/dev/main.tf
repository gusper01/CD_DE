#. Configurar nube
terraform {
  required_providers {
    aws = {
        source = "hashicorp/aws"
        version = "~>5.0"
  }
}
}

# configurar region
provider"aws" {
    region = "us-east-1"
  
}

# configurar recursos bucket datos crudos
resource "aws_s3_bucket" "data_lake_rw" {
  bucket = "coderhouse-datalake-raw-prueba-semana1-gusper"
  force_destroy = true
  tags = {
        Enviroment = "Dev"
        Project ="DataOps-Course"
}    
}

# Modulo Ingesta pre entrega 2

# ------------------------------------

# MÓDULO DE INGESTA DE DATOS (KINESIS)
# ------------------------------------

module "kinesis" {
    source = "../../modules/kinesis"
    environment = "dev"

    stream_name = "clicks-ecommerce-dev"
    shard_count = 2

    bucket_name = "coderhouse-datalake-algo"

    # Buffering agresivo para ver resultados rápido en dev
    #buffer_size_mb      = 5
    #buffer_interval_sec = 60
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
