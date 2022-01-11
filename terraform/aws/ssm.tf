data "aws_ssm_parameter" "default_eks_assume_role_policy" {
  name = "/policy/${local.cluster_name}-assume-role-policy-document"
}

data "aws_ssm_parameter" "red-key-arn" {
  name = "/kms/red-key-arn"
}
