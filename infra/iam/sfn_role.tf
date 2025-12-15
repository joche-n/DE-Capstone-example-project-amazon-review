############################################
# IAM Role for AWS Step Functions
# Grants:
# - Permission to start and poll Glue jobs
# - Invoke Lambda functions
# - Publish SNS messages
# - Write CloudWatch Logs
############################################

# ---- Allow Step Functions service to assume this role ----
data "aws_iam_policy_document" "sfn_assume" {
  statement {
    effect = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

# ---- Step Functions Role ----
resource "aws_iam_role" "sfn_role" {
  name               = "${var.project}-sfn-role-${var.env}"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume.json
  tags = {
    Project = var.project
    Env     = var.env
  }
}

# ---- Lookup account and region dynamically ----
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# ---- Compute Glue Job ARN (handles optional job name) ----
locals {
  glue_job_arn = (
    var.glue_job_name != ""
    ? format(
        "arn:aws:glue:%s:%s:job/%s",
        data.aws_region.current.name,
        data.aws_caller_identity.current.account_id,
        var.glue_job_name
      )
    : "*"
  )
}


# ---- Inline policy for Step Functions ----
resource "aws_iam_policy" "sfn_policy" {
  name        = "${var.project}-sfn-policy-${var.env}"
  description = "Allow Step Functions to orchestrate Glue jobs, Lambda, and SNS"

  # use jsonencode for cleaner multi-line HCL
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        # Step Function can start and poll Glue job runs
        Sid    = "GlueStartAndGet",
        Effect = "Allow",
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns"
        ],
        Resource = local.glue_job_arn
      },
      {
        # Allow invoking Lambda functions
        Sid      = "InvokeLambda",
        Effect   = "Allow",
        Action   = ["lambda:InvokeFunction"],
        Resource = var.lambda_function_arn
      },
      {
        # Allow publishing to SNS topics (for job status notifications)
        Sid      = "PublishSNS",
        Effect   = "Allow",
        Action   = ["sns:Publish"],
        Resource = "*"
      },
      {
        # Allow logging to CloudWatch
        Sid      = "CloudWatchLogs",
        Effect   = "Allow",
        Action   = [
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        Resource = ["arn:aws:logs:*:*:log-group:*"]
      }
      # Optional: uncomment if Step Function must pass the Glue IAM role
      # ,{
      #   "Sid": "AllowPassRole",
      #   "Effect": "Allow",
      #   "Action": ["iam:PassRole"],
      #   "Resource": [aws_iam_role.glue_role.arn]
      # }
    ]
  })
}

# ---- Attach inline policy ----
resource "aws_iam_role_policy_attachment" "sfn_attach_policy" {
  role       = aws_iam_role.sfn_role.name
  policy_arn = aws_iam_policy.sfn_policy.arn
}