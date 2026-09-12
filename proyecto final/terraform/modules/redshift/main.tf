data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

locals {
  name_prefix = lower("${var.project_name}-${var.environment}")
}

# ==============================================================================
# IAM ROLE - REDSHIFT
# Acceso al stream procesado, Glue e Iceberg/S3
# ==============================================================================

resource "aws_iam_role" "redshift_streaming" {
  name = "${local.name_prefix}-redshift-streaming"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Effect = "Allow"

        Principal = {
          Service = [
            "redshift.amazonaws.com",
            "redshift-serverless.amazonaws.com"
          ]
        }

        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Environment = var.environment
    Project     = var.project_name
    ManagedBy   = "Terraform"
  }
}

resource "aws_iam_role_policy" "redshift_streaming" {
  name = "${local.name_prefix}-redshift-streaming-policy"
  role = aws_iam_role.redshift_streaming.id

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Sid    = "ReadProcessedKinesisStream"
        Effect = "Allow"

        Action = [
          "kinesis:DescribeStream",
          "kinesis:DescribeStreamSummary",
          "kinesis:GetShardIterator",
          "kinesis:GetRecords",
          "kinesis:ListShards"
        ]

        Resource = var.processed_stream_arn
      },

      {
        Sid    = "ReadGlueCatalog"
        Effect = "Allow"

        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:BatchGetPartition"
        ]

        Resource = [
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/${var.glue_database_name}",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${var.glue_database_name}/*"
        ]
      },

      {
        Sid    = "ReadLakehouseBucket"
        Effect = "Allow"

        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]

        Resource = var.lakehouse_bucket_arn
      },

      {
        Sid    = "ReadLakehouseObjects"
        Effect = "Allow"

        Action = [
          "s3:GetObject"
        ]

        Resource = "${var.lakehouse_bucket_arn}/*"
      }
    ]
  })
}

# ==============================================================================
# SECURITY GROUP - REDSHIFT SERVERLESS
# ==============================================================================

resource "aws_security_group" "redshift_serverless" {
  name        = "${local.name_prefix}-redshift"
  description = "Security group para Redshift Serverless"
  vpc_id      = var.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Environment = var.environment
    Project     = var.project_name
    ManagedBy   = "Terraform"
  }
}

# ==============================================================================
# KINESIS INTERFACE VPC ENDPOINT
# ==============================================================================

resource "aws_security_group" "kinesis_endpoint" {
  name        = "${local.name_prefix}-kinesis-endpoint"
  description = "HTTPS desde la VPC hacia Kinesis PrivateLink"
  vpc_id      = var.vpc_id

  ingress {
    description = "HTTPS desde la VPC"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr_block]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Environment = var.environment
    Project     = var.project_name
    ManagedBy   = "Terraform"
  }
}

resource "aws_vpc_endpoint" "kinesis" {
  vpc_id              = var.vpc_id
  service_name        = "com.amazonaws.${data.aws_region.current.name}.kinesis-streams"
  vpc_endpoint_type   = "Interface"
  private_dns_enabled = true

  subnet_ids = var.subnet_ids

  security_group_ids = [
    aws_security_group.kinesis_endpoint.id
  ]

  tags = {
    Name        = "${local.name_prefix}-kinesis"
    Environment = var.environment
    Project     = var.project_name
    ManagedBy   = "Terraform"
  }
}

# ==============================================================================
# REDSHIFT SERVERLESS
# ==============================================================================

resource "aws_redshiftserverless_namespace" "lakehouse" {
  namespace_name = "${local.name_prefix}-redshift"

  db_name             = var.redshift_database_name
  admin_username      = var.redshift_admin_username
  admin_user_password = var.redshift_admin_password

  default_iam_role_arn = aws_iam_role.redshift_streaming.arn

  iam_roles = [
    aws_iam_role.redshift_streaming.arn
  ]

  log_exports = [
    "userlog",
    "connectionlog",
    "useractivitylog"
  ]

  tags = {
    Environment = var.environment
    Project     = var.project_name
    ManagedBy   = "Terraform"
  }
}

resource "aws_redshiftserverless_workgroup" "lakehouse" {
  workgroup_name = "${local.name_prefix}-redshift"
  namespace_name = aws_redshiftserverless_namespace.lakehouse.namespace_name

  base_capacity = var.redshift_base_capacity
  max_capacity  = var.redshift_max_capacity

  enhanced_vpc_routing = true
  publicly_accessible  = var.publicly_accessible

  subnet_ids = var.subnet_ids

  security_group_ids = [
    aws_security_group.redshift_serverless.id
  ]

  config_parameter {
    parameter_key   = "require_ssl"
    parameter_value = "true"
  }

  tags = {
    Environment = var.environment
    Project     = var.project_name
    ManagedBy   = "Terraform"
  }

  depends_on = [
    aws_vpc_endpoint.kinesis
  ]
}
