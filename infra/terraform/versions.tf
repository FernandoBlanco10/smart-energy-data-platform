terraform {
  required_version = ">= 1.7.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      # v6.0 es GA desde 2025 y es la rama activa de desarrollo (v5.x quedó en
      # mantenimiento). No lo pude correr yo mismo (sin acceso a terraform/red
      # en este entorno) — al hacer `terraform init` en tu máquina, si algún
      # recurso de main.tf da error de sintaxis, esa es la primera señal de que
      # cambió entre v5 y v6; bajar a "~> 5.0" es el plan B.
      version = "~> 6.0"
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
