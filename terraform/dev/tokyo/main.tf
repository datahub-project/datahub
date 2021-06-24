terraform {
  backend "s3" {
    region = "ap-northeast-1"
    bucket = "terraform-remote-state-smartnews-dev-ap-northeast-1"
    key    = "data-catalogue/datahub.tfstate"
  }
}