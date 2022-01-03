resource "aws_iam_role" "dataportal" {
  name                 = "dataportal"
  max_session_duration = 21600 # 6 Hours
  description          = "Role for dataportal service"
  assume_role_policy   = data.aws_ssm_parameter.default_eks_assume_role_policy.value
}
