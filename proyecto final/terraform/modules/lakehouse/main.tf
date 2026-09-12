data "aws_caller_identity" "current" {}

locals {
  name_prefix = lower("${var.project_name}-${data.aws_caller_identity.current.account_id}-${var.environment}")
}

# ==============================================================================
# S3 - RAW / ARTEFACTOS
# ==============================================================================

resource "aws_s3_bucket" "raw" {
  bucket        = "${local.name_prefix}-raw"
  force_destroy = var.force_destroy

  tags = {
    Environment = var.environment
    Project     = var.project_name
    Layer       = "raw"
    ManagedBy   = "Terraform"
  }
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket = aws_s3_bucket.raw.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ==============================================================================
# S3 - LAKEHOUSE / ICEBERG
# ==============================================================================

resource "aws_s3_bucket" "lakehouse" {
  bucket        = "${local.name_prefix}-lakehouse"
  force_destroy = var.force_destroy

  tags = {
    Environment = var.environment
    Project     = var.project_name
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

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ==============================================================================
# AWS GLUE DATA CATALOG
# ==============================================================================

resource "aws_glue_catalog_database" "lakehouse" {
  name        = var.glue_database_name
  description = "Catalogo Glue para tablas Apache Iceberg del pipeline de streaming"

  tags = {
    Environment = var.environment
    Project     = var.project_name
    ManagedBy   = "Terraform"
  }
}
