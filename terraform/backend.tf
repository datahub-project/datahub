terraform {
  backend "s3" {
    bucket               = "grnds-terraform-workspaces"
    key                  = "dataportal.tfstate"
    region               = "us-east-1"
    encrypt              = true
    dynamodb_table       = "grnds-tf-lock"
    workspace_key_prefix = "dataportal-services"
  }
}
