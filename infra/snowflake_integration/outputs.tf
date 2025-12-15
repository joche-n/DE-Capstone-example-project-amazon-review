#############################################
# modules/snowflake_integration/outputs.tf
#############################################

output "snowflake_integration_role_arn" {
  description = "IAM role ARN for Snowflake to assume"
  value       = aws_iam_role.snowflake_integration.arn
}

output "s3_allowed_location" {
  description = "S3 path Snowflake can access"
  value       = "s3://${var.s3_bucket}/${var.s3_prefix}"
}

output "sf_storage_iam_user_arn" {
  description = "Snowflake-generated IAM user ARN (after integration created)"
  value       = try(snowflake_storage_integration.capstone.storage_aws_iam_user_arn, "")
}

output "sf_storage_external_id" {
  description = "Snowflake-generated external ID (after integration created)"
  value       = try(snowflake_storage_integration.capstone.storage_aws_external_id, "")
}