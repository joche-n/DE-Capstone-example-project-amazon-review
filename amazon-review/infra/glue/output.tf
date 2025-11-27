output "download_job_name" {
  value = aws_glue_job.download.name
}

output "flatten_job_name" {
  value = aws_glue_job.flatten.name
}

output "download_script_s3_path" {
  value = "s3://${var.bucket_name}/${local.download_key}"
}

output "flatten_script_s3_path" {
  value = "s3://${var.bucket_name}/${local.flatten_key}"
}