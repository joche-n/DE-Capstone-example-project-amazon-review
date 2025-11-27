terraform {
  backend "s3" {
    bucket         = "capstone-amazon-state-bucket" # created in bootstrap
    key            = "amazon-infra/dev/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "capstone-amazon-lock-table" # created in bootstrap
    encrypt        = true
  }
}
