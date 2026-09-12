terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "current" {}

locals {
  state_bucket_name = lower(
    "${var.project_name}-tfstate-${data.aws_caller_identity.current.account_id}-${var.environment}"
  )

  lock_table_name = lower(
    "${var.project_name}-terraform-locks-${var.environment}"
  )
}

# ==============================================================================
# S3 - TERRAFORM REMOTE STATE
# ==============================================================================

resource "aws_s3_bucket" "tf_state" {
  bucket        = local.state_bucket_name
  force_destroy = var.backend_force_destroy

  tags = {
    Name        = "Terraform State Storage"
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform-Bootstrap"
  }
}

resource "aws_s3_bucket_versioning" "tf_state" {
  bucket = aws_s3_bucket.tf_state.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "tf_state" {
  bucket = aws_s3_bucket.tf_state.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "tf_state" {
  bucket = aws_s3_bucket.tf_state.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}


