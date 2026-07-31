# Terraform — infraestructura del pipeline

## Uso local con LocalStack (gratis, recomendado para desarrollo)

[LocalStack](https://www.localstack.cloud/) emula S3 (y el resto de AWS) en un contenedor Docker local — ya está en el `docker-compose.yml` de la raíz del proyecto, puerto `4566`. Terraform le habla exactamente igual que a AWS real, solo cambia el endpoint.

**Forma recomendada: [`tflocal`](https://github.com/localstack/terraform-local)**, el wrapper oficial de LocalStack. Hace automáticamente lo que antes había que armar a mano (ver más abajo): genera un override temporal de los endpoints del provider y lo borra al terminar. Instalación (una vez):

```bash
pipx install terraform-local
# alternativa sin pipx: pip install terraform-local --break-system-packages
```

Uso — es un reemplazo directo de `terraform`, mismos subcomandos:

```bash
cd infra/terraform
docker compose up -d localstack        # si el stack completo no está arriba
tflocal init
tflocal validate
tflocal plan -var="bronze_bucket_name=smart-energy-bronze-dev"
tflocal apply -var="bronze_bucket_name=smart-energy-bronze-dev"
```

**Cómo confirmar que se creó de verdad** (sin consola de AWS, porque no existe — es local):

```bash
aws --endpoint-url=http://localhost:4566 s3 ls
aws --endpoint-url=http://localhost:4566 s3 ls s3://smart-energy-bronze-dev
```

**Alternativa manual (`override.tf`)** — útil para entender qué hace `tflocal` por vos, o si preferís no instalar una herramienta más. Copiá `override.tf.example` a `override.tf` (ya está en `.gitignore`) y corré `terraform` normal en vez de `tflocal`:

```bash
cp override.tf.example override.tf
terraform init
terraform plan -var="bronze_bucket_name=smart-energy-bronze-dev"
```

## Uso con AWS real (free tier)

Si más adelante querés demostrar el despliegue en la nube real para el portafolio, el free tier de AWS incluye 5GB de S3 gratis por 12 meses. Necesitás borrar/no usar `override.tf` y no usar `tflocal` (son solo para LocalStack):

```bash
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
terraform init
terraform plan -var="bronze_bucket_name=tu-nombre-unico-bronze"
terraform apply
```

**Importante:** nunca subas tus credenciales de AWS al repo. Usá variables de entorno o un archivo `terraform.tfvars` que esté en `.gitignore`. Y no te olvides de `terraform destroy` cuando termines de mostrar la demo — el free tier tiene límites.

## Comandos básicos

```bash
terraform init      # descarga el provider de AWS (usá tflocal si es contra LocalStack)
terraform validate  # valida sintaxis, no necesita LocalStack corriendo
terraform plan      # muestra qué se va a crear, sin aplicar
terraform apply     # aplica los cambios
terraform destroy   # elimina todo lo aprovisionado (importante para no dejar recursos huérfanos)
```

## Nota

El provider de AWS está pineado a `~> 6.0` (versions.tf) — es la rama activa desde que v6 se volvió GA. No pude correr `terraform init` yo mismo en este entorno (sin acceso a Terraform ni a internet general desde mi sandbox), así que la primera vez que lo corras vos, si `aws_s3_bucket_versioning` o algún otro recurso da un error de sintaxis, avisame y lo ajustamos — es la señal de que algo cambió entre v5 y v6 que no detecté leyendo el changelog.
