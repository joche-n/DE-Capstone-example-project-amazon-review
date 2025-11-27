variable "aws_region" {
  description = "AWS region to deploy backend resources"
  type        = string
  default     = "us-east-1"
}

variable "tfstate_bucket_name" {
  description = "Globally unique bucket name for Terraform state"
  type        = string
  default     = "capstone-amazon-state-bucket"
}

variable "tfstate_dynamodb_table" {
  description = "DynamoDB table name for state locking"
  type        = string
  default     = "capstone-amazon-lock-table"
}
