
############################################
# Lambda deployment
# - Packages code from ./deploy into a zip
# - Creates Lambda function
# - Optionally grants SNS permission to invoke the Lambda if sns_topic_arn is provided
############################################

locals {
  lambda_name     = var.lambda_function_name
  lambda_zip_path = "${path.module}/deploy/lambda_package.zip"
}

# ------------------------------
# Package the deploy/ directory into a single ZIP
# - archive_file is from the 'archive' provider built into Terraform core
# - source_dir should contain handler.py and any dependencies you include in deploy/
# ------------------------------
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/deploy"
  output_path = local.lambda_zip_path
}

# ------------------------------
# Lambda function resource
# - handler: "handler.lambda_handler" matches handler.py above
# - runtime: param-driven (default python3.12)
# - role: must be the Lambda execution role ARN (from IAM module)
# - timeout: increased to 900s (15 min) so it can run long post-processing tasks if needed
# ------------------------------
resource "aws_lambda_function" "post_ingest" {
  function_name = local.lambda_name
  role          = var.lambda_role_arn
  handler       = "handler.lambda_handler"
  runtime       = var.runtime
  filename      = data.archive_file.lambda_zip.output_path
  timeout       = var.timeout_seconds

  environment {
    variables = {
      PROJECT_NAME  = var.project
      ENV           = var.env
      SNS_TOPIC_ARN = var.sns_topic_arn
    }
  }

  tags = {
    Project = var.project
    Env     = var.env
  }
}

# ------------------------------
# Optional: Grant SNS permission to invoke this Lambda.
# This resource is created only when an SNS topic ARN is supplied.
# It allows the principal 'sns.amazonaws.com' to invoke the function.
# ------------------------------
resource "aws_lambda_permission" "allow_sns_invoke" {
  count = 1

  statement_id  = "AllowExecutionFromSNS"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.post_ingest.function_name
  principal     = "sns.amazonaws.com"
  # Source ARN limits who can invoke (the SNS topic)
  source_arn    = var.sns_topic_arn
}

# ------------------------------
# Outputs
# ------------------------------
output "lambda_function_name" {
  description = "Name of the Lambda function"
  value       = aws_lambda_function.post_ingest.function_name
}

output "lambda_function_arn" {
  description = "ARN of the Lambda function"
  value       = aws_lambda_function.post_ingest.arn
}

output "lambda_role_required" {
  description = "Note: Lambda requires an execution role (passed as var.lambda_role_arn)"
  value       = var.lambda_role_arn
}
