variable "project_prefix" {
  type        = string
  description = "Prefix used for naming Snowflake database/schema when defaults are used."
  default     = "capstone_amazon"
}

variable "snowflake_organization_name" {
  type        = string
  description = "The Snowflake organization name. The 'snowflake_account' variable is deprecated."
  # It is best practice to provide this value in a .tfvars file or as an environment variable.
}

variable "snowflake_account_name" {
  type        = string
  description = "The Snowflake account name within the organization. The 'snowflake_account' variable is deprecated."
  # It is best practice to provide this value in a .tfvars file or as an environment variable.
}

variable "snowflake_user" {
  type        = string
  description = "Snowflake user used by Terraform. The 'username' parameter is deprecated."
}

variable "snowflake_password" {
  type        = string
  description = "Password for Snowflake user (use GitHub Secrets or env vars)."
  sensitive   = true
}

variable "snowflake_role" {
  type        = string
  description = "Snowflake role to use (e.g. SYSADMIN)."
  default     = "SYSADMIN"
}

variable "snowflake_warehouse" {
  type        = string
  description = "Snowflake Warehouse."
  default     = "COMPUTE_WH"
}

variable "database_name" {
  type        = string
  description = "Optional database name. If empty, will be derived from project_prefix."
  default     = ""
}

variable "database_comment" {
  type        = string
  description = "Comment for Snowflake database."
  default     = "Database for CAPSTONE-amazon project"
}

variable "schema_name" {
  type        = string
  description = "Optional schema name. If empty, will be derived from project_prefix."
  default     = ""
}

variable "schema_comment" {
  type        = string
  description = "Comment for Snowflake schema."
  default     = "Schema for CAPSTONE-amazon project"
}

variable "grant_role" {
  type        = string
  description = "If provided, grants USAGE on database & schema to this account role. Leave empty to skip. The grant is now applied to an account role, not a generic role."
  default     = ""
}
