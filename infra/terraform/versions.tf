terraform {
  required_version = ">= 1.7.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Backend remoto opcional (recomendado en equipo, no obligatorio para portafolio individual).
  # Para un proyecto personal, el estado local (terraform.tfstate) es suficiente
  # siempre que NO lo subas a git (ver .gitignore).
  #
  # backend "s3" {
  #   bucket = "tu-bucket-de-estado-terraform"
  #   key    = "smart-energy-pipeline/terraform.tfstate"
  #   region = "us-east-1"
  # }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "smart-energy-pipeline"
      ManagedBy   = "terraform"
      Environment = var.environment
    }
  }
}
