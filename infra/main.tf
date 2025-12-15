# infra/main.tf
# Root wiring of sub-modules — with explicit dependency ordering

locals {
  project = var.project
  env     = var.env
  region  = var.aws_region
}

# -------------------------
# S3 - Data bucket
# -------------------------
module "s3" {
  source      = "./s3"
  project     = local.project
  env         = local.env
  bucket_name = var.data_bucket_name
  # S3 is foundational — no dependencies
}

# -------------------------
# IAM - Roles (Glue, Lambda, Step Functions)
# -------------------------
module "iam" {
  source        = "./iam"
  project       = local.project
  env           = local.env
  s3_bucket_arn = module.s3.bucket_arn
  aws_region    = var.aws_region

  # Ensure IAM runs after S3 is provisioned (role policies reference the bucket ARN)
  depends_on = [
    module.s3
  ]
}

# -------------------------
# Glue - Job & Bootstrap Script
# -------------------------
module "glue" {
  source            = "./glue"
  project           = local.project
  env               = local.env
  bucket_name       = module.s3.bucket_name
  glue_role_arn     = module.iam.glue_role_arn
  download_job_name = "${local.project}-download-job"
  flatten_job_name  = "${local.project}-flatten-job"

  # Glue depends on IAM (for role) and S3 (for script bucket)
  depends_on = [
    module.s3,
    module.iam
  ]
}

# -------------------------
# SNS - Alerts (email subscribers optional)
# -------------------------
module "sns" {
  source       = "./sns"
  project      = local.project
  env          = local.env
  alert_emails = var.alert_emails

  # SNS can be created independently
  depends_on = []
}

# -------------------------
# Lambda - Subscriber (subscribes to SNS)
# -------------------------
module "lambda" {
  source               = "./lambda"
  project              = local.project
  env                  = local.env
  bucket_name          = module.s3.bucket_name
  lambda_role_arn      = module.iam.lambda_role_arn
  sns_topic_arn        = module.sns.sns_topic_arn
  lambda_function_name = "${local.project}-lambda"

  # Lambda depends on IAM (for role) and S3 (for deployment package)
  # SNS doesn't have to exist before Lambda — subscription is handled separately
  depends_on = [
    module.iam,
    module.s3
  ]
}

# -------------------------
# Step Functions - Orchestrator
# -------------------------
module "stepfunctions" {
  source              = "./stepfunctions"
  project             = local.project
  env                 = local.env
  bucket_name         = module.s3.bucket_name
  sfn_role_arn        = module.iam.sfn_role_arn
  lambda_function_arn = module.lambda.lambda_function_arn
  sns_topic_arn       = module.sns.sns_topic_arn

  # Both Glue job names from your glue module
  download_job_name = module.glue.download_job_name
  flatten_job_name  = module.glue.flatten_job_name

  poll_interval_seconds = 120

  depends_on = [
    module.glue,
    module.lambda,
    module.sns,
    module.iam
  ]
}

# -------------------------
# Snowflake - Analytics / Warehouse
# -------------------------
module "snowflake" {
  source = "./snowflake"

  # these must match variables in modules/snowflake/variables.tf
  snowflake_organization_name = var.snowflake_organization_name
  snowflake_account_name      = var.snowflake_account_name
  snowflake_user              = var.snowflake_user
  snowflake_password          = var.snowflake_password
  snowflake_role              = var.snowflake_role
  snowflake_warehouse         = var.snowflake_warehouse

  project_prefix   = upper(local.project)
  database_name    = "${upper(local.project)}_DB"
  schema_name      = "${upper(local.project)}_SCHEMA"
  database_comment = "Capstone : amazon Database"
  schema_comment   = "Capstone : amazon RAW Schema"

  depends_on = [
    module.s3,
    module.iam
  ]
}

# -------------------------
# Snowflake <-> S3 Integration
# -------------------------

module "snowflake_integration" {
  source = "./snowflake_integration"

  # s3 inputs
  s3_bucket = module.s3.bucket_name
  s3_prefix = module.s3.s3_prefixes["flattened"]

  update_trust_script = "${path.root}/snowflake_integration/scripts/update_trust.py"

  depends_on = [
    module.s3,
    module.iam,
    module.snowflake
  ]
}



