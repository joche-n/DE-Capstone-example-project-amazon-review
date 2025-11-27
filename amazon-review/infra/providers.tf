terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 4.0"
    }
    snowflake = {
      source  = "SnowflakeDB/snowflake"
      version = "~> 0.100.0"
    }
  }
}


provider "aws" {
  region = var.aws_region
  # profile = var.aws_profile  # uncomment if you use CLI profile
}

provider "snowflake" {
  organization_name = var.snowflake_organization_name
  account_name      = var.snowflake_account_name
  user              = var.snowflake_user
  password          = var.snowflake_password
  role              = var.snowflake_role
  warehouse         = var.snowflake_warehouse
}
