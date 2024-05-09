### Prerequisities

Notice of breaking change: in the latest version of the DynamoDB connector, `aws_region` is now a required configuration. The connector will no longer loop through all AWS regions; instead, it will only use the region passed into the recipe configuration.

In order to execute this source, you need to attach the `AmazonDynamoDBReadOnlyAccess` policy to a user in your AWS account. Then create an API access key and secret for the user.

For a user to be able to create API access key, it needs the following access key permissions. Your AWS account admin can create a policy with these permissions and attach to the user, you can find more details in [Managing access keys for IAM users](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html)

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
