############################################
# IAM Module Variables
# Inputs for IAM module.
# Pass s3_bucket_arn (from module.s3.bucket_arn)
# Optionally scope Step Functions permissions
############################################

variable "project" {
  type    = string
  default = "capstone-amazon"
}

variable "env" {
  type    = string
  default = "dev"
}

# The data bucket ARN (e.g. module.s3.bucket_arn)
variable "s3_bucket_arn" {
  type = string
}

# The Lambda function ARN the Step Function will call (optional).
# If you don't have it yet, use "*" and restrict later.
variable "lambda_function_arn" {
  type    = string
  default = "*"
}

# Optional: scope Glue actions to a specific job name
variable "glue_job_name" {
  type    = string
  default = ""
}

# Region (automatically filled via data source in sfn_role.tf)
variable "aws_region" {
  type    = string
  default = ""
}