#############################################
# modules/snowflake_integration/main.tf
#############################################

# NOTE: This module expects aws and snowflake providers to be configured at root.

terraform {
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 0.100.0"
    }
  }
}

# Build IAM policy that permits Snowflake to list & read the prefix
data "aws_iam_policy_document" "s3_read" {
  statement {
    sid       = "AllowListBucket"
    effect    = "Allow"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = ["arn:aws:s3:::${var.s3_bucket}"]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${var.s3_prefix}*"]
    }
  }

  statement {
    sid       = "AllowGetObjects"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = ["arn:aws:s3:::${var.s3_bucket}/${var.s3_prefix}*"]
  }
}

resource "aws_iam_policy" "snowflake_s3_read" {
  name        = "${var.project}-snowflake-s3-read"
  description = "Allow Snowflake to list and read objects under the raw prefix"
  policy      = data.aws_iam_policy_document.s3_read.json
  tags = {
    project = var.project
  }
}

resource "aws_iam_role" "snowflake_integration" {
  name = var.iam_role_name

  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::${var.snowflake_aws_account_id}:root" }
        Action    = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    project = var.project
  }
}

resource "aws_iam_role_policy_attachment" "attach_read" {
  role       = aws_iam_role.snowflake_integration.name
  policy_arn = aws_iam_policy.snowflake_s3_read.arn
}

# Create Snowflake storage integration that references the IAM role
resource "snowflake_storage_integration" "capstone" {
  name                      = var.integration_name
  storage_provider          = "S3"
  enabled                   = true
  storage_aws_role_arn      = aws_iam_role.snowflake_integration.arn
  storage_allowed_locations = ["s3://${var.s3_bucket}/${var.s3_prefix}"]
  comment                   = "Storage integration for ${var.project} raw S3"
}

# Null resource to run a local script that tightens role trust based on Snowflake outputs
resource "null_resource" "tighten_trust" {
  triggers = {
    sf_user_arn = try(snowflake_storage_integration.capstone.storage_aws_iam_user_arn, "")
    sf_external = try(snowflake_storage_integration.capstone.storage_aws_external_id, "")
    role_name   = aws_iam_role.snowflake_integration.name
  }

  depends_on = [
    aws_iam_role.snowflake_integration,
    snowflake_storage_integration.capstone
  ]

  provisioner "local-exec" {
    command     = "${var.update_trust_script} ${aws_iam_role.snowflake_integration.name} \"${try(snowflake_storage_integration.capstone.storage_aws_iam_user_arn, "")}\" \"${try(snowflake_storage_integration.capstone.storage_aws_external_id, "")}\""
    interpreter = ["/bin/bash", "-c"]
  }
}