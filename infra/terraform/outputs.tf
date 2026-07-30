output "bronze_bucket_name" {
  description = "Nombre del bucket que contiene las capas Bronze/Silver/Gold."
  value       = aws_s3_bucket.bronze.id
}

output "bronze_bucket_arn" {
  description = "ARN del bucket, útil para configurar políticas IAM de Spark/Airflow."
  value       = aws_s3_bucket.bronze.arn
}
