############################################
# IAM Role for AWS Glue Jobs
# Grants:
# - Read/write access to S3 project bucket
# - CloudWatch Logs access
# - AWS Glue service permissions
############################################

# ---- Trust policy: allow Glue to assume this role ----
data "aws_iam_policy_document" "glue_assume" {
  statement {
    effect = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

# ---- Glue Role ----
resource "aws_iam_role" "glue_role" {
  name               = "${var.project}-glue-role-${var.env}"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
  tags = {
    Project = var.project
    Env     = var.env
  }
}

# ---- Inline S3 access policy ----
resource "aws_iam_policy" "glue_s3_policy" {
  name        = "${var.project}-glue-s3-policy-${var.env}"
  description = "Allow Glue job to access data and scripts in S3 bucket"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid = "AllowListBucketForPrefixes",
        Effect = "Allow",
        Action = ["s3:ListBucket"],
        Resource = [var.s3_bucket_arn],
        Condition = {
          StringLike = {
            "s3:prefix" = [
              "scripts/*",
              "raw/*",
              "temporary/*",
              "flattened/*"
            ]
          }
        }
      },
      {
        Sid = "AllowObjectOps",
        Effect = "Allow",
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:GetObjectVersion",
          "s3:PutObjectAcl"
        ],
        Resource = ["${var.s3_bucket_arn}/*"]
      }
    ]
  })
}

# ---- Attach S3 policy ----
resource "aws_iam_role_policy_attachment" "glue_attach_s3" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.glue_s3_policy.arn
}

# ---- Attach AWS managed Glue service role policy ----
resource "aws_iam_role_policy_attachment" "glue_managed" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# ---- CloudWatch Logs policy ----
resource "aws_iam_policy" "glue_logs" {
  name        = "${var.project}-glue-logs-${var.env}"
  description = "CloudWatch Logs permissions for Glue"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        Resource = ["arn:aws:logs:*:*:log-group:*"]
      }
    ]
  })
}

# ---- Attach CloudWatch Logs policy ----
resource "aws_iam_role_policy_attachment" "glue_attach_logs" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.glue_logs.arn
}