#. Configurar nube
terraform {
  required_providers {
    aws = {
        source = "hashicorp/aws"
        version = "~>5.0"
  }
}
}

# configurar region
provider"aws" {
    region = "us-east-1"
  
}

# configurar recursos bucket datos crudos
resource "aws_s3_bucket" "data_lake_rw" {
  bucket = "coderhouse-datalake-raw-prueba-semana1-gusper"
  force_destroy = true
  tags = {
        Enviroment = "Dev"
        Project ="DataOps-Course"
}    
}



