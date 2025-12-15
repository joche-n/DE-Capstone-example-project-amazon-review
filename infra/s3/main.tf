# infra/s3/main.tf
#
# Data bucket used for raw/processed/archive/error prefixes.
# Modern AWS provider v5+ resources are used (no deprecated fields).
#
# The module creates:
#  - S3 bucket (with optional force_destroy)
#  - public access block (deny public access)
#  - ownership controls (BucketOwnerEnforced)
#  - versioning
#  - server-side encryption (SSE-S3 AES256)
#  - lifecycle rule for 'raw/' prefix and non-current versions

provider "aws" {
  region = var.aws_region != null ? var.aws_region : "us-east-1"
  # If provider configured at root, this provider block can be removed.
}

resource "aws_s3_bucket" "data" {
  bucket        = var.bucket_name
  force_destroy = var.force_destroy

  tags = {
    Name      = var.bucket_name
    Project   = var.project
    Env       = var.env
    ManagedBy = "terraform"
  }
}

# Prevent public access
resource "aws_s3_bucket_public_access_block" "data_block" {
  bucket = aws_s3_bucket.data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Enforce bucket owner ownership; disables ACLs (recommended)
resource "aws_s3_bucket_ownership_controls" "data_ownership" {
  bucket = aws_s3_bucket.data.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# Versioning
resource "aws_s3_bucket_versioning" "data_versioning" {
  bucket = aws_s3_bucket.data.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Server-side encryption (SSE-S3)
resource "aws_s3_bucket_server_side_encryption_configuration" "data_encryption" {
  bucket = aws_s3_bucket.data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Lifecycle: handle current and noncurrent versions for 'raw/' prefix
resource "aws_s3_bucket_lifecycle_configuration" "data_lifecycle_raw" {
  bucket = aws_s3_bucket.data.id

  rule {
    id     = "raw-current-tiering"
    status = "Enabled"

    filter {
      prefix = local.prefixes.raw
    }

    # Transition current (latest) objects to STANDARD_IA
    transition {
      days          = var.raw_transition_days
      storage_class = "STANDARD_IA"
    }

    # Expire current objects
    expiration {
      days = var.raw_expiration_days
    }

    # Non-current (previous versions) transition and expiration
    noncurrent_version_transition {
      noncurrent_days = var.noncurrent_days
      storage_class   = "STANDARD_IA"
    }

    noncurrent_version_expiration {
      noncurrent_days = var.raw_expiration_days
    }
  }
}

# Helpful locals: canonical prefixes to use elsewhere
locals {
  prefixes = {
    raw       = "raw/"       # for API responses (source of truth)
    scripts   = "scripts/"   # for Glue job scripts
    tmp       = "tmp/"       # Glue temp folder
    flattened = "flattened/" # Flatten
  }
}
