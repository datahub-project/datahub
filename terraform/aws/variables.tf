data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

variable "region" {
  default = "us-east-1"
}

locals {
  account_id   = data.aws_caller_identity.current.account_id
  cluster_name = "${terraform.workspace}-eks-cluster"
  root_iam_arn = "arn:aws:iam::${local.account_id}:root"

  security_group = {
    "integration3" = "gr-rack-2018-06-19-164359-sgELBintegration3SG-1UOZPDX4GQC42"   # integration3
    "uat"          = "gr-rack-2015-01-22-1421963843-sgELBuatSG-17WY9POG18O3P"        # uat
    "production"   = "gr-rack-2015-02-05-1423192064-sgELBproductionSG-1LMBP6UGDGW0M" # prod
  }

}

variable "es_domain" {
  default = "dataportal"
}
