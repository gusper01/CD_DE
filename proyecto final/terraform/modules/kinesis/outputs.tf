output "raw_stream_name" {
  description = "Nombre del stream RAW"
  value       = aws_kinesis_stream.raw.name
}

output "raw_stream_arn" {
  description = "ARN del stream RAW"
  value       = aws_kinesis_stream.raw.arn
}

output "processed_stream_name" {
  description = "Nombre del stream PROCESSED"
  value       = aws_kinesis_stream.processed.name
}

output "processed_stream_arn" {
  description = "ARN del stream PROCESSED"
  value       = aws_kinesis_stream.processed.arn
}

output "firehose_name" {
  description = "Nombre del Firehose asociado al stream RAW"
  value       = aws_kinesis_firehose_delivery_stream.raw.name
}

output "firehose_arn" {
  description = "ARN del Firehose asociado al stream RAW"
  value       = aws_kinesis_firehose_delivery_stream.raw.arn
}
