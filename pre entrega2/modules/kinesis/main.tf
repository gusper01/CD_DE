# BLOQUE A: KINESIS DATA STREAM
# ------------------------------------------------------------------------------
# 1. KINESIS DATA STREAM (KDS) — PROVISIONED, 2 shards
# ------------------------------------------------------------------------------
resource "aws_kinesis_stream" "main" {
  name             = var.stream_name
  shard_count      = var.shard_count
  retention_period = 24 # horas, default 24

  # Pre-entrega: el stream debe estar cifrado
  encryption_type = "KMS"
  kms_key_id      = "alias/aws/kinesis"

  tags = {
    Name        = var.stream_name
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}
# ------------------------------------------------------------------------------
# X. Cloudwatch 
# grupo y recurso
# ------------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "firehose" {
  name = "/aws/kinesisfirehose/ingesta-${var.stream_name}"
}

resource "aws_cloudwatch_log_stream" "firehose" {
  name           = "S3Delivery"
  log_group_name = aws_cloudwatch_log_group.firehose.name
}
# ------------------------------------------------------------------------------
# 2. IAM ROLE PARA FIREHOSE
# Leer del stream + escribir en S3 + logs en CloudWatch
# ------------------------------------------------------------------------------
resource "aws_iam_role" "firehose" {
  name = "firehose-kinesis-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "firehose.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "firehose" {
  name = "firehose-kinesis-policy"
  role = aws_iam_role.firehose.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kinesis:DescribeStream",
          "kinesis:GetShardIterator",
          "kinesis:GetRecords"
        ]
        Resource = aws_kinesis_stream.main.arn
      },
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetBucketLocation",
          "s3:ListBucket",
          "s3:AbortMultipartUpload",
          "s3:ListBucketMultipartUploads",
          "s3:ListMultipartUploadParts"
        ]
        Resource = [
          "arn:aws:s3:::${var.bucket_name}",
          "arn:aws:s3:::${var.bucket_name}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:PutLogEvents",
          "logs:CreateLogGroup",
          "logs:CreateLogStream"
        ]
        Resource = "*"
      }
    ]
  })
}

# ------------------------------------------------------------------------------
# 3. KINESIS DATA FIREHOSE (KDF) — source = KDS, destination = S3
# ------------------------------------------------------------------------------
resource "aws_kinesis_firehose_delivery_stream" "main" {
  name        = "ingesta-${var.stream_name}"
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.main.arn
    role_arn           = aws_iam_role.firehose.arn
  }

  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose.arn
    bucket_arn = "arn:aws:s3:::${var.bucket_name}"

    prefix              = "ingesta/year=!{timestamp:yyyy}/"
    error_output_prefix = "ingesta-errores/type=!{firehose:error-output-type}/year=!{timestamp:yyyy}/"

    buffering_size     = var.buffer_size_mb
    buffering_interval = var.buffer_interval_sec

    compression_format = "GZIP"

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.firehose.name
      log_stream_name = aws_cloudwatch_log_stream.firehose.name
    }
  }
}