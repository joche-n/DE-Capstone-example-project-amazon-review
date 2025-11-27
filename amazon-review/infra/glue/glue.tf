locals {
  scripts_prefix       = "scripts"
  download_script_name = "Capstone-Amazon-Review-Dataset-Download-Job.py"
  flatten_script_name  = "Capstone-Amazon-Review-Flatten-Job.py"

  download_key = "${local.scripts_prefix}/${local.download_script_name}"
  flatten_key  = "${local.scripts_prefix}/${local.flatten_script_name}"
}

# Upload download script
resource "aws_s3_object" "download_script" {
  bucket                  = var.bucket_name
  key                     = local.download_key
  source                  = "${path.module}/scripts/${local.download_script_name}"
  content_type            = "text/x-python"
  etag                    = filemd5("${path.module}/scripts/${local.download_script_name}")
  server_side_encryption  = "AES256"

  tags = {
    Project = var.project
    Env     = var.env
  }
}

# Upload flatten spark script
resource "aws_s3_object" "flatten_script" {
  bucket                  = var.bucket_name
  key                     = local.flatten_key
  source                  = "${path.module}/scripts/${local.flatten_script_name}"
  content_type            = "text/x-python"
  etag                    = filemd5("${path.module}/scripts/${local.flatten_script_name}")
  server_side_encryption  = "AES256"

  tags = {
    Project = var.project
    Env     = var.env
  }
}

# Glue Python Shell job (download + unzip)
resource "aws_glue_job" "download" {
  name     = var.download_job_name
  role_arn = var.glue_role_arn

  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "s3://${var.bucket_name}/${local.download_key}"
  }

  # Default arguments that can be overridden at StartJobRun time
  default_arguments = {
    "--TempDir"    = "s3://${var.bucket_name}/temporary/"
    "--s3-bucket"  = var.bucket_name
    "--s3-prefix"  = var.input_prefix
    "--upload-raw" = "true"
    "--retries"    = "3"
    "--timeout"    = "60"
    # do NOT hardcode secrets here; use AWS Secrets Manager or SSM if needed
  }

  glue_version = var.python_shell_glue_version
  max_capacity = var.download_max_capacity

  execution_property {
    max_concurrent_runs = 1
  }

  tags = {
    Project = var.project
    Env     = var.env
  }

  depends_on = [
    aws_s3_object.download_script
  ]
}

# Glue Spark job (flatten)
resource "aws_glue_job" "flatten" {
  name     = var.flatten_job_name
  role_arn = var.glue_role_arn

  command {
    # For Glue Spark jobs use "glueetl"
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${var.bucket_name}/${local.flatten_key}"
  }

  default_arguments = {
    "--TempDir"                 = "s3://${var.bucket_name}/temporary/"
    "--s3_bucket"               = var.bucket_name
    "--input_prefix"            = var.input_prefix
    "--review_json_key"         = var.review_json_key
    "--meta_json_key"           = var.meta_json_key
    "--review_output_prefix"    = var.review_output_prefix
    "--meta_output_prefix"      = var.meta_output_prefix
    "--output_format"           = var.output_format
    "--compression"             = var.compression
    "--coalesce"                = tostring(var.coalesce)
    "--fail_on_error"           = tostring(var.fail_on_error)
    # Glue adds arguments like --JOB_NAME automatically; don't duplicate sensitive values
  }

  glue_version = var.glue_version

  # Use worker_type / number_of_workers for Glue 2.0/3.0 Spark jobs.
  worker_type       = var.flatten_worker_type
  number_of_workers = var.flatten_number_of_workers

  execution_property {
    max_concurrent_runs = 1
  }

  tags = {
    Project = var.project
    Env     = var.env
  }

  depends_on = [
    aws_s3_object.flatten_script
  ]
}