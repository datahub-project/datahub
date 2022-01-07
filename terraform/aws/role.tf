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
      "arn:aws:glue:us-east-1:${local.account_id}:registry/dataportal-schema-registry"
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

locals {
  workspace_key_prefix = "dataportal-services"
}

data "aws_s3_bucket" "tf_s3_bucket" {
  bucket = "grnds-terraform-workspaces"
}

data "aws_dynamodb_table" "grnds_tf_lock" {
  name = "grnds-tf-lock"
}


# required for terraforming kafka topics in preDeploy
data "aws_iam_policy_document" "dataportal_read_write_terraform_state" {
  statement {
    sid    = "DataPortalListObjectsGrndsTerraformWorkspaces"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
    ]
    resources = [
      data.aws_s3_bucket.tf_s3_bucket.arn,
    ]
  }
  statement {
    sid    = "DataPortalReadWriteKafkaTerraformState"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
    ]
    resources = [
      # allow updating anything under the workspace key prefix
      "${data.aws_s3_bucket.tf_s3_bucket.arn}/${local.workspace_key_prefix}/*",
      "${data.aws_s3_bucket.tf_s3_bucket.arn}/dataportal-kafka/*",
    ]
  }
}

resource "aws_iam_policy" "dataportal_terraform_kafka_topics" {
  name        = "dataportal-read-write-kafka-terraform-state"
  description = "Grants read/write  access to dataportal kafka terraform state"
  policy      = data.aws_iam_policy_document.dataportal_read_write_terraform_state.json
}

resource "aws_iam_role_policy_attachment" "dataportal_terraform_kafka_topics" {
  role       = aws_iam_role.dataportal.name
  policy_arn = aws_iam_policy.dataportal_terraform_kafka_topics.arn
}
