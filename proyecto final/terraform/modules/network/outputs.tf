output "vpc_id" {
  description = "ID de la VPC del proyecto"
  value       = aws_vpc.main.id
}

output "vpc_cidr_block" {
  description = "CIDR de la VPC"
  value       = aws_vpc.main.cidr_block
}

output "private_subnet_ids" {
  description = "IDs de las subnets privadas"
  value       = aws_subnet.private[*].id
}

output "availability_zones" {
  description = "Availability Zones utilizadas"
  value       = aws_subnet.private[*].availability_zone
}
