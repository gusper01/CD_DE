data "aws_caller_identity" "current" {}

data "aws_region" "current" {}
resource "aws_iam_role" "flink" {
  name = "flink-${var.application_name}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Effect = "Allow"

        Principal = {
          Service = "kinesisanalytics.amazonaws.com"
        }

        Action = "sts:AssumeRole"
      }
    ]
  })
}


resource "aws_iam_role_policy" "flink" {
  name = "flink-${var.application_name}-policy"

  role = aws_iam_role.flink.id

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [

      {
        Effect = "Allow"

        Action = [
          "kinesis:DescribeStream",
          "kinesis:DescribeStreamSummary",
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:ListShards"
        ]

        Resource = var.kinesis_stream_arn
      },
      {
        Sid    = "WriteProcessedKinesisStream"
        Effect = "Allow"

        Action = [
          "kinesis:PutRecord",
          "kinesis:PutRecords"
        ]

        Resource = var.processed_stream_arn
      },
      {
        Effect = "Allow"

        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion"
        ]

        Resource = "${var.artifact_bucket_arn}/*"
      },

      {
        Effect = "Allow"

        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]

        Resource = [
          var.lakehouse_bucket_arn,
          "${var.lakehouse_bucket_arn}/*"
        ]
      },

      {
        Sid    = "GlueCatalogAccess"
        Effect = "Allow"

        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:CreateTable",
          "glue:UpdateTable"
        ]

        Resource = [
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/${var.glue_database_name}",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${var.glue_database_name}/*"
        ]
      },

      {
        Sid    = "WriteFlinkApplicationLogs"
        Effect = "Allow"

        Action = [
          "logs:PutLogEvents"
        ]

        Resource = aws_cloudwatch_log_stream.flink.arn
      }
    ]
  })
}
