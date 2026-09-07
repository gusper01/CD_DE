# ==============================================================================
# REDSHIFT SERVERLESS - PRE-ENTREGA 6
# ==============================================================================

data "aws_caller_identity" "current" {}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# ------------------------------------------------------------------------------
# IAM ROLE - REDSHIFT -> KINESIS / GLUE / S3
# ------------------------------------------------------------------------------

resource "aws_iam_role" "redshift_streaming" {
  name = "redshift-streaming-ingestion-dev"

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
    Environment = "dev"
    Project     = "urban-streaming"
    ManagedBy   = "Terraform"
  }
}

resource "aws_iam_role_policy" "redshift_streaming" {
  name = "redshift-streaming-ingestion-dev-policy"
  role = aws_iam_role.redshift_streaming.id

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Sid    = "ReadKinesisStream"
        Effect = "Allow"

        Action = [
          "kinesis:DescribeStream",
          "kinesis:DescribeStreamSummary",
          "kinesis:GetShardIterator",
          "kinesis:GetRecords",
          "kinesis:ListShards"
        ]

        Resource = module.kinesis.stream_arn
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
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.lakehouse.name}",
          "arn:aws:glue:us-east-1:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.lakehouse.name}/*"
        ]
      },

      {
        Sid    = "ReadLakehouseBucket"
        Effect = "Allow"

        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]

        Resource = aws_s3_bucket.lakehouse.arn
      },

      {
        Sid    = "ReadLakehouseObjects"
        Effect = "Allow"

        Action = [
          "s3:GetObject"
        ]

        Resource = "${aws_s3_bucket.lakehouse.arn}/*"
      }
    ]
  })
}

# ------------------------------------------------------------------------------
# SECURITY GROUP - REDSHIFT SERVERLESS
# ------------------------------------------------------------------------------

resource "aws_security_group" "redshift_serverless" {
  name        = "redshift-serverless-dev"
  description = "Security group para Redshift Serverless"
  vpc_id      = data.aws_vpc.default.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Environment = "dev"
    Project     = "urban-streaming"
    ManagedBy   = "Terraform"
  }
}

# ------------------------------------------------------------------------------
# KINESIS INTERFACE VPC ENDPOINT
# Mantiene Redshift -> Kinesis dentro de la red AWS
# ------------------------------------------------------------------------------

resource "aws_security_group" "kinesis_endpoint" {
  name        = "kinesis-endpoint-dev"
  description = "HTTPS desde la VPC hacia Kinesis PrivateLink"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "HTTPS desde la VPC"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [data.aws_vpc.default.cidr_block]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_vpc_endpoint" "kinesis" {
  vpc_id              = data.aws_vpc.default.id
  service_name        = "com.amazonaws.us-east-1.kinesis-streams"
  vpc_endpoint_type   = "Interface"
  private_dns_enabled = true

  subnet_ids = slice(data.aws_subnets.default.ids, 0, 3)

  security_group_ids = [
    aws_security_group.kinesis_endpoint.id
  ]

  tags = {
    Name        = "kinesis-streaming-dev"
    Environment = "dev"
    Project     = "urban-streaming"
    ManagedBy   = "Terraform"
  }
}

# ------------------------------------------------------------------------------
# REDSHIFT SERVERLESS
# ------------------------------------------------------------------------------

resource "aws_redshiftserverless_namespace" "lakehouse" {
  namespace_name = "urban-lakehouse-dev"

  db_name             = "dev"
  admin_username      = "admin_lakehouse"
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
    Environment = "dev"
    Project     = "urban-streaming"
    ManagedBy   = "Terraform"
  }
}

resource "aws_redshiftserverless_workgroup" "lakehouse" {
  workgroup_name = "urban-lakehouse-dev"
  namespace_name = aws_redshiftserverless_namespace.lakehouse.namespace_name

  base_capacity = 8
  max_capacity  = 8

  enhanced_vpc_routing = true
  publicly_accessible  = true

  subnet_ids = slice(data.aws_subnets.default.ids, 0, 3)

  security_group_ids = [
    aws_security_group.redshift_serverless.id
  ]

  config_parameter {
    parameter_key   = "require_ssl"
    parameter_value = "true"
  }

  tags = {
    Environment = "dev"
    Project     = "urban-streaming"
    ManagedBy   = "Terraform"
  }

  depends_on = [
    aws_vpc_endpoint.kinesis
  ]
}