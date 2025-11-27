variable "project" {
  type    = string
  default = "capstone-amazon"
}

variable "env" {
  type    = string
  default = "dev"
}

variable "bucket_name" {
  description = "S3 bucket name where scripts and raw data live"
  type        = string
}

variable "glue_role_arn" {
  description = "IAM role ARN for Glue"
  type        = string
}

variable "download_job_name" {
  type    = string
  default = "Capstone-Amazon-Review-Dataset-Download-Job"
}

variable "flatten_job_name" {
  type    = string
  default = "Capstone-Amazon-Review-Flatten-Job"
}

# Glue versions
variable "python_shell_glue_version" {
  type    = string
  default = "3.0" # Glue Python shell job version
}

variable "glue_version" {
  type    = string
  default = "3.0" # Glue Spark job version
}

# Worker sizing for python shell and spark job
variable "download_max_capacity" {
  type    = number
  default = 0.0625 # 1/16 DPU for python shell
}

variable "flatten_worker_type" {
  type    = string
  default = "G.1X"
}

variable "flatten_number_of_workers" {
  type    = number
  default = 2
}

# Input/output defaults (overridable at runtime)
variable "input_prefix" {
  type    = string
  default = "raw"
}

variable "review_json_key" {
  type    = string
  default = "reviews/AMAZON_FASHION.json"
}

variable "meta_json_key" {
  type    = string
  default = "meta/meta_AMAZON_FASHION.json"
}

variable "review_output_prefix" {
  type    = string
  default = "flattened/reviews"
}

variable "meta_output_prefix" {
  type    = string
  default = "flattened/meta"
}

variable "output_format" {
  type    = string
  default = "parquet"
}

variable "compression" {
  type    = string
  default = "snappy"
}

variable "coalesce" {
  type    = number
  default = 1
}

variable "fail_on_error" {
  type    = bool
  default = true
}