resource "aws_kinesisanalyticsv2_application" "main" {
  name = var.application_name
  #description            = "Procesamiento streaming de sensores urbanos con PyFlink"
  description            = "Procesamiento streaming de sensores urbanos con Apache Flink"
  runtime_environment    = "FLINK-1_19"
  service_execution_role = aws_iam_role.flink.arn
  start_application      = false

  # CloudWatch Logs para Managed Flink
  cloudwatch_logging_options {
    log_stream_arn = aws_cloudwatch_log_stream.flink.arn
  }
  application_configuration {

    application_code_configuration {
      code_content {
        s3_content_location {
          bucket_arn = var.artifact_bucket_arn
          file_key   = var.artifact_key
        }
      }

      code_content_type = "ZIPFILE"
    }

    environment_properties {
      dynamic "property_group" {
        for_each = var.application_mode == "pyflink" ? [1] : []

        content {
          property_group_id = "kinesis.analytics.flink.run.options"

          property_map = {
            python  = var.python_file
            jarfile = "lib/pyflink-dependencies.jar"
          }
        }
      }

      property_group {
        property_group_id = "consumer.config.0"

        property_map = {
          "stream.name"          = var.kinesis_stream_name
          "stream.arn"           = var.kinesis_stream_arn
          "aws.region"           = var.aws_region
          "flink.stream.initpos" = "LATEST"
        }
      }

      property_group {
        property_group_id = "producer.config.0"

        property_map = {
          "stream.name" = var.processed_stream_name
          "stream.arn"  = var.processed_stream_arn
          "aws.region"  = var.aws_region
        }
      }
      property_group {
        property_group_id = "lakehouse.config.0"

        property_map = {
          "glue.database"    = var.glue_database_name
          "lakehouse.bucket" = var.lakehouse_bucket_name
          "aws.region"       = var.aws_region
        }
      }
    }

    flink_application_configuration {

      checkpoint_configuration {
        configuration_type            = "CUSTOM"
        checkpointing_enabled         = true
        checkpoint_interval           = 60000
        min_pause_between_checkpoints = 5000
      }

      monitoring_configuration {
        configuration_type = "CUSTOM"
        log_level          = "INFO"
        metrics_level      = "APPLICATION"
      }

      parallelism_configuration {
        configuration_type   = "CUSTOM"
        auto_scaling_enabled = false
        parallelism          = 1
        parallelism_per_kpu  = 1
      }
    }
  }

  tags = {
    Environment = var.environment
    Project     = "urban-streaming"
    ManagedBy   = "Terraform"
  }
}

/* resource "aws_cloudwatch_log_group" "flink" {
  name              = "/aws/kinesis-analytics/${var.application_name}"
  retention_in_days = 7
} */

/* resource "aws_cloudwatch_log_stream" "flink" {
  name           = "kinesis-analytics-log-stream"
  log_group_name = aws_cloudwatch_log_group.flink.name
} */