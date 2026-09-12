# ==============================================================================
# KINESIS RAW
# Eventos originales enviados por el producer
# ==============================================================================

resource "aws_kinesis_stream" "raw" {
  name             = var.raw_stream_name
  shard_count      = var.raw_shard_count
  retention_period = var.retention_hours

  encryption_type = "KMS"
  kms_key_id      = "alias/aws/kinesis"

  shard_level_metrics = var.shard_level_metrics

  tags = {
    Name        = var.raw_stream_name
    Environment = var.environment
    Layer       = "raw"
    ManagedBy   = "Terraform"
  }
}

# ==============================================================================
# KINESIS PROCESSED
# Salida procesada por Apache Flink consumida por Redshift
# ==============================================================================

resource "aws_kinesis_stream" "processed" {
  name             = var.processed_stream_name
  shard_count      = var.processed_shard_count
  retention_period = var.retention_hours

  encryption_type = "KMS"
  kms_key_id      = "alias/aws/kinesis"

  shard_level_metrics = var.shard_level_metrics

  tags = {
    Name        = var.processed_stream_name
    Environment = var.environment
    Layer       = "processed"
    ManagedBy   = "Terraform"
  }
}

# ==============================================================================
# CLOUDWATCH LOGS - FIREHOSE
# ==============================================================================

resource "aws_cloudwatch_log_group" "firehose" {
  name = "/aws/kinesisfirehose/ingesta-${var.raw_stream_name}"

  retention_in_days = 7

  tags = {
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

resource "aws_cloudwatch_log_stream" "firehose" {
  name           = "S3Delivery"
  log_group_name = aws_cloudwatch_log_group.firehose.name
}

# ==============================================================================
# IAM ROLE - FIREHOSE
# ==============================================================================

resource "aws_iam_role" "firehose" {
  name = "firehose-${var.raw_stream_name}"

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

  tags = {
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

resource "aws_iam_role_policy" "firehose" {
  name = "firehose-${var.raw_stream_name}-policy"
  role = aws_iam_role.firehose.id

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Sid    = "ReadRawKinesisStream"
        Effect = "Allow"

        Action = [
          "kinesis:DescribeStream",
          "kinesis:DescribeStreamSummary",
          "kinesis:GetShardIterator",
          "kinesis:GetRecords",
          "kinesis:ListShards"
        ]

        Resource = aws_kinesis_stream.raw.arn
      },

      {
        Sid    = "WriteRawArchiveToS3"
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
        Sid    = "WriteFirehoseLogs"
        Effect = "Allow"

        Action = [
          "logs:PutLogEvents"
        ]

        Resource = aws_cloudwatch_log_stream.firehose.arn
      }
    ]
  })
}

# ==============================================================================
# FIREHOSE
# Archivo persistente del stream RAW en S3
# ==============================================================================

resource "aws_kinesis_firehose_delivery_stream" "raw" {
  name        = "ingesta-${var.raw_stream_name}"
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.raw.arn
    role_arn           = aws_iam_role.firehose.arn
  }

  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose.arn
    bucket_arn = "arn:aws:s3:::${var.bucket_name}"

    prefix = "ingesta/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"

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
