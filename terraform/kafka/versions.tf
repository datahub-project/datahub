terraform {
  required_version = ">= 0.13"
  required_providers {
    kafka = {
      source = "Mongey/kafka"
      version = "~> 0.3.3"
    }
  }
}
