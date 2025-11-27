############################################
# IAM Role for AWS Lambda
# Used for post-processing, Glue job polling,
# and SNS notification functions
############################################

# ---- Trust policy: allow Lambda service to assume this role ----
data "aws_iam_policy_document" "lambda_assume" {
  statement {
    effect = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

# ---- Lambda Role ----
resource "aws_iam_role" "lambda_role" {
  name               = "${var.project}-lambda-role-${var.env}"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
  tags = {
    Project = var.project
    Env     = var.env
  }
}

# ---- Basic execution role (CloudWatch logs) ----
resource "aws_iam_role_policy_attachment" "lambda_basic_exec" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# ---- Inline custom policy ----
# Grants SNS, SSM, and Secrets Manager access
resource "aws_iam_policy" "lambda_extra" {
  name        = "${var.project}-lambda-extra-${var.env}"
  description = "Allow Lambda to publish SNS, read SSM params, and SecretsManager"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid = "SSMParameterRead",
        Effect = "Allow",
        Action = [
          "ssm:GetParameter",
          "ssm:GetParameters",
          "ssm:GetParametersByPath"
        ],
        Resource = ["*"] # TODO: restrict to /capstone/* parameters
      },
      {
        Sid = "SNSPublish",
        Effect = "Allow",
        Action = ["sns:Publish"],
        Resource = ["*"] # TODO: restrict to SNS topic ARN
      },
      {
        Sid = "SecretsRead",
        Effect = "Allow",
        Action = ["secretsmanager:GetSecretValue"],
        Resource = ["*"] # TODO: restrict to specific secrets ARN
      }
    ]
  })
}

# ---- Attach extra policy ----
resource "aws_iam_role_policy_attachment" "lambda_extra_attach" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.lambda_extra.arn
}