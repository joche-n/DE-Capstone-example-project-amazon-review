# infra/s3/variables.tf
#
# Inputs for the S3 module.
# Provide bucket_name when running apply (must be globally unique).
# Set force_destroy = true for dev/test to allow terraform destroy to delete contents.

# Region for the S3 data bucket (injected from root or default)
variable "aws_region" {
  description = "AWS region for S3 bucket deployment"
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Project name used for tagging"
  type        = string
  default     = "capstone-amazon"
}

variable "env" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "bucket_name" {
  description = "Globally-unique S3 bucket name for raw/processed storage"
  type        = string
}

variable "force_destroy" {
  description = "Allow bucket to be forcefully destroyed even if non-empty (use with caution)"
  type        = bool
  default     = true
}

# Optional lifecycle timings: adjust for your retention policy
variable "raw_transition_days" {
  description = "Days before raw objects transition to STANDARD_IA"
  type        = number
  default     = 30
}

variable "noncurrent_days" {
  description = "Days before noncurrent (versioned) objects transition/expire"
  type        = number
  default     = 90
}

variable "raw_expiration_days" {
  description = "Days before raw objects expire permanently"
  type        = number
  default     = 365
}
