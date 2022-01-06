resource "aws_iam_role" "dataportal" {
  name                 = "dataportal"
  max_session_duration = 21600 # 6 Hours
  description          = "Role for dataportal service"
  assume_role_policy   = data.aws_ssm_parameter.default_eks_assume_role_policy.value
}

resource "aws_iam_role_policy_attachment" "dataportal_can_query_glue" {
  role       = aws_iam_role.dataportal.name
  policy_arn = aws_iam_policy.dataportal-policy.arn
}

resource "aws_iam_policy" "dataportal-policy" {
  policy = data.aws_iam_policy_document.glue-access.json
}

data "aws_iam_policy_document" "glue-access" {
  statement {
    sid = "GlueAccess"

    actions = [
      "glue:GetRegistry",
      "glue:ListRegistries",
      "glue:CreateSchema",
      "glue:UpdateSchema",
      "glue:GetSchema",
      "glue:ListSchemas",
      "glue:RegisterSchemaVersion",
      "glue:GetSchemaByDefinition",
      "glue:GetSchemaVersion",
      "glue:GetSchemaVersionsDiff",
      "glue:ListSchemaVersions",
      "glue:CheckSchemaVersionValidity",
      "glue:PutSchemaVersionMetadata",
      "glue:QuerySchemaVersionMetadata"
    ]

    resources = [
      "arn:aws:glue:*:${local.account_id}:schema/*",
      "arn:aws:glue:us-west-2:${local.account_id}:registry/demo-shared"
    ]
  }
  statement {
    actions = [
      "glue:GetSchemaVersion"
    ]

    resources = [
      "*"
    ]
  }
}
