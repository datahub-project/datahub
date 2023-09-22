### Prerequisities

In order to execute this source, you will need to create access key and secret keys that have DynamoDB read access. You can create these policies and attach to your account or can ask your account admin to attach these policies to your account.

For access key permissions, you can create a policy with permissions below and attach to your account, you can find more details in [Managing access keys for IAM users](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "VisualEditor0",
      "Effect": "Allow",
      "Action": [
        "iam:ListAccessKeys",
        "iam:CreateAccessKey",
        "iam:UpdateAccessKey",
        "iam:DeleteAccessKey"
      ],
      "Resource": "arn:aws:iam::${aws_account_id}:user/${aws:username}"
    }
  ]
}
```

For DynamoDB read access, you can simply attach AWS managed policy `AmazonDynamoDBReadOnlyAccess` to your account, you can find more details in [Attaching a policy to an IAM user group](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_groups_manage_attach-policy.html)
