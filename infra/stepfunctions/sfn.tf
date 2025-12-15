############################################
# Step Functions Orchestration
# - Runs both Glue jobs sequentially
# - Invokes Lambda notification after completion
############################################

locals {
  state_machine_name = "${var.project}-orchestrator-${var.env}"
}

resource "aws_sfn_state_machine" "orchestrator" {
  name     = local.state_machine_name
  role_arn = var.sfn_role_arn

  definition = jsonencode({
    Comment = "Capstone Amazon Review Pipeline (Download → Flatten → Notify)",
    StartAt = "RunDownloadJob",
    States = {
      RunDownloadJob = {
        Type = "Task",
        Resource = "arn:aws:states:::glue:startJobRun.sync",
        Parameters = {
          JobName = var.download_job_name
          Arguments = {
            "--s3-bucket" = var.bucket_name
            "--s3-prefix" = "raw"
          }
        },
        Next = "RunFlattenJob",
        Catch = [
          {
            ErrorEquals = ["States.ALL"],
            Next        = "NotifyFailure"
          }
        ]
      },

      RunFlattenJob = {
        Type = "Task",
        Resource = "arn:aws:states:::glue:startJobRun.sync",
        Parameters = {
          JobName = var.flatten_job_name
          Arguments = {
            "--s3-bucket"            = var.bucket_name
            "--input_prefix"         = "raw"
            "--review_output_prefix" = "flattened/reviews"
            "--meta_output_prefix"   = "flattened/meta"
            "--output_format"        = "parquet"
            "--compression"          = "snappy"
          }
        },
        Next = "NotifySuccess",
        Catch = [
          {
            ErrorEquals = ["States.ALL"],
            Next        = "NotifyFailure"
          }
        ]
      },

      NotifySuccess = {
        Type     = "Task",
        Resource = "arn:aws:states:::lambda:invoke",
        Parameters = {
          FunctionName = var.lambda_function_arn,
          Payload = {
            "Project"     = var.project,
            "Environment" = var.env,
            "GlueJob"     = var.flatten_job_name,
            "Status"      = "SUCCEEDED"
          }
        },
        End = true
      },

      NotifyFailure = {
        Type     = "Task",
        Resource = "arn:aws:states:::lambda:invoke",
        Parameters = {
          FunctionName = var.lambda_function_arn,
          Payload = {
            "Project"     = var.project,
            "Environment" = var.env,
            "GlueJob"     = "Amazon Review Pipeline",
            "Status"      = "FAILED"
          }
        },
        End = true
      }
    }
  })
}