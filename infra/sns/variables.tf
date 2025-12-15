# infra/sns/variables.tf
#
# Input variables for SNS alert setup.

variable "project" {
  type    = string
  default = "capstone-amazon"
}

variable "env" {
  type    = string
  default = "dev"
}

# List of emails to subscribe for job notifications
variable "alert_emails" {
  description = "List of email addresses to subscribe to the SNS topic"
  type        = list(string)
  default     = []
}
