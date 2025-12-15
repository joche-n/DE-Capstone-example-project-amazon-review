output "tfstate_bucket_name" {
  value = aws_s3_bucket.tfstate.bucket
}

output "tfstate_bucket_arn" {
  value = aws_s3_bucket.tfstate.arn
}

output "dynamodb_table_name" {
  value = aws_dynamodb_table.tf_lock.name
}