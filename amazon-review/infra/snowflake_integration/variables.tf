#############################################
# modules/snowflake_integration/variables.tf
#############################################

variable "s3_bucket" {
  type        = string
  description = "S3 bucket containing raw data (full bucket name)"
}

variable "s3_prefix" {
  type        = string
  description = "S3 prefix/path within the bucket that Snowflake should access"
  default     = ""
}

variable "integration_name" {
  type        = string
  description = "Name for Snowflake storage integration"
  default     = "capstone_amazon_snowflake_s3_integration"
}

variable "iam_role_name" {
  type        = string
  description = "Name for the IAM role that Snowflake will assume"
  default     = "capstone_amazon_snowflake_s3_integration_role"
}

variable "snowflake_aws_account_id" {
  type        = string
  description = "Snowflake AWS account ID for initial permissive trust (region-specific)"
  default     = "898466741470"
}

variable "project" {
  type        = string
  description = "Project tag (optional)"
  default     = "capstone-amazon"
}

variable "database_comment" {
  type        = string
  description = "Comment for Snowflake DB if needed by module callers (optional)"
  default     = ""
}

variable "schema_comment" {
  type        = string
  description = "Comment for Snowflake schema if needed by module callers (optional)"
  default     = ""
}

# Allow caller to override the local-exec script path if desired
variable "update_trust_script" {
  type        = string
  description = "Path to local script that updates the trust policy after integration creation. Default located in module ./scripts/update_trust.py"
  default     = ""
}
