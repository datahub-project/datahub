resource "aws_elasticsearch_domain" "elasticsearch" {
  domain_name           = var.es_domain
  elasticsearch_version = "7.9"

  vpc_options {
    subnet_ids         = slice(split(",", data.aws_ssm_parameter.private_subnet_ids.value), 0, 1)
    security_group_ids = [data.aws_security_group.local.id]
  }

  cluster_config {
    instance_type = "r4.large.elasticsearch"
  }

  ebs_options {
    ebs_enabled = true
    volume_size = 10
  }


  access_policies = <<CONFIG
{
  "Version": "2012-10-17",
  "Statement": [
      {
          "Action": "es:*",
          "Principal": "*",
          "Effect": "Allow",
          "Resource": "arn:aws:es:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:domain/${var.es_domain}/*"
      }
  ]
}
  CONFIG

  snapshot_options {
    automated_snapshot_start_hour = 23
  }

  tags = {
    Domain  = var.es_domain
    Service = "dataportal"
  }
}
