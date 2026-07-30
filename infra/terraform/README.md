# Terraform — infraestructura del pipeline

## Uso local (gratis, recomendado para desarrollo)

Para no incurrir en costo mientras desarrollás, usá [LocalStack](https://www.localstack.cloud/) — emula S3 y otros servicios de AWS en un contenedor Docker local. Terraform habla con LocalStack exactamente igual que con AWS real, cambiando solo el endpoint:

```hcl
# override.tf (no versionar en git, o versionar solo para dev)
provider "aws" {
  region                      = var.aws_region
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_requesting_account_id  = true

  endpoints {
    s3 = "http://localhost:4566"
  }
}
```

Con esto podés correr `terraform plan` y `terraform apply` reales, contra un S3 que vive en tu máquina, sin cuenta de AWS ni costo.

## Uso con AWS real (free tier)

Si querés demostrar el despliegue en la nube real para el portafolio, el free tier de AWS incluye 5GB de S3 gratis por 12 meses — más que suficiente para un dataset de portafolio. Necesitás:

```bash
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
terraform init
terraform plan -var="bronze_bucket_name=tu-nombre-unico-bronze"
terraform apply
```

**Importante:** nunca subas tus credenciales de AWS al repo. Usá variables de entorno o un archivo `terraform.tfvars` que esté en `.gitignore`.

## Comandos básicos

```bash
terraform init      # descarga el provider de AWS
terraform validate  # valida sintaxis
terraform plan       # muestra qué se va a crear, sin aplicar
terraform apply      # aplica los cambios
terraform destroy    # elimina todo lo aprovisionado (importante para no dejar recursos huérfanos)
```
