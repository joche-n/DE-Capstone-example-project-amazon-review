############################################
# Lambda module variables
############################################

variable "project" {
  type    = string
  default = "capstone-amazon"
}

variable "env" {
  type    = string
  default = "dev"
}

# Lambda execution role ARN (from iam module). This role must allow
# CloudWatch logging and any other AWS APIs the handler uses.
variable "lambda_role_arn" {
  description = "IAM role ARN for Lambda execution (from iam module)"
  type        = string
}

variable "bucket_name" {
  description = "S3 bucket name (for logs, artifacts, etc.)"
  type        = string
}

# Optional SNS topic ARN to receive notifications. If provided, Terraform
# will create a lambda permission allowing SNS to invoke the function.
variable "sns_topic_arn" {
  description = "Optional SNS topic ARN for notifications (empty = none)"
  type        = string
  default     = ""
}

variable "lambda_function_name" {
  description = "Lambda function name"
  type        = string
  default     = "capstone-amazon-lambda"
}

variable "runtime" {
  description = "Python runtime version"
  type        = string
  default     = "python3.12"
}

# Execution timeout (seconds)
variable "timeout_seconds" {
  description = "Lambda timeout in seconds"
  type        = number
  default     = 900
}