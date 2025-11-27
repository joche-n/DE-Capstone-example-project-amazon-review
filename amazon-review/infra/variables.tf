# infra/variables.tf
variable "project" {
  description = "Project name used for tags and resource naming"
  type        = string
  default     = "capstone_amazon"
}

variable "env" {
  description = "Environment (dev/staging/prod)"
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "aws_profile" {
  description = "Optional AWS CLI profile for local runs"
  type        = string
  default     = ""
}

# S3 bucket name for project data (must be globally unique)
variable "data_bucket_name" {
  description = "S3 bucket for raw/scripts/tmp"
  type        = string
  default     = "capstone-amazon-project-bucket" # replace with your unique name
}

# SNS alert emails (optional)
variable "alert_emails" {
  description = "List of email addresses to subscribe to alerts (confirm required)"
  type        = list(string)
  default     = ["preciselyqa@gmail.com"]
}

# Snowflake provider & module inputs (supply these via TF_VAR_* or terraform.tfvars / CI secrets)

variable "snowflake_organization_name" {
  type        = string
  description = "Snowflake organization name (if applicable)"
  default     = ""
}

variable "snowflake_account_name" {
  type        = string
  description = "Snowflake account name / locator (e.g. xy12345)"
  default     = ""
}

variable "snowflake_user" {
  type        = string
  description = "Snowflake username Terraform should use"
  default     = ""
}

variable "snowflake_password" {
  type        = string
  description = "Snowflake user password (supply via CI secrets)"
  sensitive   = true
  default     = ""
}

variable "snowflake_role" {
  type        = string
  description = "Snowflake role to assume (e.g. ACCOUNTADMIN)"
  default     = "ACCOUNTADMIN"
}

variable "snowflake_warehouse" {
  type        = string
  description = "Warehouse name to use / create"
  default     = "CAPSTONE_WH"
}

variable "snowflake_region" {
  type        = string
  description = "Snowflake region if needed"
  default     = ""
}


