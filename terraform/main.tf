terraform {
  required_providers {
    minio = {
      source  = "aminueza/minio"
      version = ">= 1.0.0"
    }
  }
}

provider "minio" {
  minio_server   = "localhost:9000"
  minio_access_key = "aws_certified"
  minio_secret_key = "super_senha_123"
  minio_ssl        = false
}

# --- BUCKETS DO PIPELINE DE CRIPTO ---
resource "minio_s3_bucket" "bucket_crypto_bronze" {
  bucket = "bronze"
  acl    = "private"
}

resource "minio_s3_bucket" "bucket_crypto_silver" {
  bucket = "silver"
  acl    = "private"
}

resource "minio_s3_bucket" "bucket_crypto_gold" {
  bucket = "gold"
  acl    = "private"
}

# --- BUCKETS DO NOVO PIPELINE DE CLIMA ---
resource "minio_s3_bucket" "bucket_clima_bronze" {
  bucket = "clima-bronze"
  acl    = "private"
}

resource "minio_s3_bucket" "bucket_clima_silver" {
  bucket = "clima-silver"
  acl    = "private"
}

resource "minio_s3_bucket" "bucket_clima_gold" {
  bucket = "clima-gold"
  acl    = "private"
}