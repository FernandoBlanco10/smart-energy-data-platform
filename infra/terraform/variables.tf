variable "aws_region" {
  description = "Región de AWS donde se aprovisiona la infraestructura."
  type        = string
  default     = "us-east-1" # us-east-1 alinea con la región PJME del dataset
}

variable "environment" {
  description = "Entorno de despliegue (dev, staging, prod). Para portafolio, siempre 'dev'."
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Nombre base del proyecto, usado como prefijo de recursos."
  type        = string
  default     = "smart-energy-pipeline"
}

variable "bronze_bucket_name" {
  description = "Nombre del bucket S3 para la capa Bronze (datos crudos). Debe ser único globalmente."
  type        = string
}

variable "enable_versioning" {
  description = "Habilita versionado en los buckets. Recomendado en Bronze para poder auditar reprocesos."
  type        = bool
  default     = true
}
