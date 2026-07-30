# ---------------------------------------------------------------------------
# Bucket para la capa Bronze del Data Lakehouse.
# Ver docs/architecture-decisions.md -> ADR-005 (por qué Delta Lake sobre S3).
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "bronze" {
  bucket = var.bronze_bucket_name
}

resource "aws_s3_bucket_versioning" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  versioning_configuration {
    status = var.enable_versioning ? "Enabled" : "Suspended"
  }
}

# Bloqueo de acceso público por defecto: buena práctica obligatoria,
# incluso en un proyecto de aprendizaje. Un bucket abierto es el error
# de configuración más común y más costoso en la nube.
resource "aws_s3_bucket_public_access_block" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# ---------------------------------------------------------------------------
# Silver y Gold usan el mismo bucket con prefijos distintos (delta lake
# soporta esto vía rutas). Simplifica el aprovisionamiento sin perder
# la separación lógica de capas.
# ---------------------------------------------------------------------------
locals {
  layer_prefixes = ["silver", "gold"]
}

resource "aws_s3_object" "layer_folders" {
  for_each = toset(local.layer_prefixes)

  bucket  = aws_s3_bucket.bronze.id
  key     = "${each.value}/.keep"
  content = ""
}
