# infra/s3/outputs.tf

output "bucket_name" {
  description = "Name of the data S3 bucket"
  value       = aws_s3_bucket.data.bucket
}

output "bucket_arn" {
  description = "ARN of the data S3 bucket"
  value       = aws_s3_bucket.data.arn
}

output "s3_prefixes" {
  description = "Common prefixes used for this bucket"
  value       = local.prefixes
}