terraform {
  backend "s3" {
    bucket               = "grnds-terraform-workspaces"
    key                  = "dataportal-kafka.tfstate"
    region               = "us-east-1"
    encrypt              = true
    dynamodb_table       = "grnds-tf-lock"
    workspace_key_prefix = "dataportal-kafka"
  }
}

variable "bootstrap_servers" {
  type = list(string)
}

variable "client_cert_path" {
  description = "The client certificate or path to a file containing the client certificate -- Use for Client authentication to Kafka.	"
  type        = string
}

variable "client_key_path" {
  description = "The private key or path to a file containing the private key that the client certificate was issued for.	"
  type        = string
}

provider "kafka" {
  bootstrap_servers = var.bootstrap_servers
  client_cert       = var.client_cert_path
  client_key        = var.client_key_path
  tls_enabled       = true
}
