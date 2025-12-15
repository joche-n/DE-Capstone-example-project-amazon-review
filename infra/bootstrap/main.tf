# infra/bootstrap/main.tf
#
# Creates S3 bucket (for Terraform remote state) + DynamoDB table (for state locking)
#
# Usage:
#   cd infra/bootstrap
#   terraform init
#   terraform validate
#   terraform apply --auto-approve
#
# Notes:
# - Uses force_destroy = true so 'terraform destroy' deletes the bucket even if not empty.
# - Uses BucketOwnerEnforced ownership (ACLs disabled and not required).
# - Enables versioning and server-side encryption (AES256).
# - Adds lifecycle rules to tier and expire old state versions.
# - Creates a DynamoDB lock table for Terraform remote state locking.

provider "aws" {
  region = var.aws_region
}

# -------------------------------------------------------------------
# S3 bucket to hold Terraform remote state
# -------------------------------------------------------------------
resource "aws_s3_bucket" "tfstate" {
  bucket        = var.tfstate_bucket_name
  force_destroy = true # allow destroy even if bucket not empty

  tags = {
    Name      = var.tfstate_bucket_name
    Project   = "capstone-amazon"
    ManagedBy = "terraform-bootstrap"
  }
}

# -------------------------------------------------------------------
# Block all forms of public access
# -------------------------------------------------------------------
resource "aws_s3_bucket_public_access_block" "tfstate_block" {
  bucket = aws_s3_bucket.tfstate.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# -------------------------------------------------------------------
# Ownership controls (required instead of ACLs)
# Enforces BucketOwnerEnforced — disables ACL usage completely.
# -------------------------------------------------------------------
resource "aws_s3_bucket_ownership_controls" "tfstate_ownership" {
  bucket = aws_s3_bucket.tfstate.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# -------------------------------------------------------------------
# Versioning (keeps historical state versions for safety)
# -------------------------------------------------------------------
resource "aws_s3_bucket_versioning" "tfstate_versioning" {
  bucket = aws_s3_bucket.tfstate.id

  versioning_configuration {
    status = "Enabled"
  }
}

# -------------------------------------------------------------------
# Default encryption (AES256)
# -------------------------------------------------------------------
resource "aws_s3_bucket_server_side_encryption_configuration" "tfstate_encryption" {
  bucket = aws_s3_bucket.tfstate.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# -------------------------------------------------------------------
# Lifecycle rules — handle noncurrent (old) state file versions
# -------------------------------------------------------------------
resource "aws_s3_bucket_lifecycle_configuration" "tfstate_lifecycle" {
  bucket = aws_s3_bucket.tfstate.id

  rule {
    id     = "retain-tfstate-versions"
    status = "Enabled"

    # Move non-current (old) versions to STANDARD_IA after 30 days
    noncurrent_version_transition {
      noncurrent_days = 30
      storage_class   = "STANDARD_IA"
    }

    # Delete non-current (old) versions after 90 days
    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }
}

# -------------------------------------------------------------------
# DynamoDB table for Terraform state locking
# -------------------------------------------------------------------
resource "aws_dynamodb_table" "tf_lock" {
  name         = var.tfstate_dynamodb_table
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }

  tags = {
    Project   = "capstone-amazon"
    ManagedBy = "terraform-bootstrap"
  }
}
