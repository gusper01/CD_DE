output "stream_name" {
  description = "Nombre del Kinesis Data Stream"
  value       = aws_kinesis_stream.main.name
}

output "stream_arn" {
  description = "ARN del Kinesis Data Stream"
  value       = aws_kinesis_stream.main.arn
}

output "firehose_name" {
  description = "Nombre del Kinesis Firehose Delivery Stream"
  value       = aws_kinesis_firehose_delivery_stream.main.name
}

output "firehose_arn" {
  description = "ARN del Kinesis Firehose Delivery Stream"
  value       = aws_kinesis_firehose_delivery_stream.main.arn
}