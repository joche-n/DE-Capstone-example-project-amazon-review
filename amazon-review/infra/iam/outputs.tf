############################################
# IAM Outputs
# Expose ARNs for other modules (Glue, Lambda, Step Functions)
############################################

output "glue_role_arn" {
  description = "ARN of the IAM Role for Glue Jobs"
  value       = aws_iam_role.glue_role.arn
}

output "lambda_role_arn" {
  description = "ARN of the IAM Role for Lambda Functions"
  value       = aws_iam_role.lambda_role.arn
}

output "sfn_role_arn" {
  description = "ARN of the IAM Role for Step Functions"
  value       = aws_iam_role.sfn_role.arn
}
