### Prerequisites

In order to execute this source the user credentials needs the following privileges

- `grant select on DATABASE.* to 'USERNAME'@'%'`
- `grant show view on DATABASE.* to 'USERNAME'@'%'`

`select` is required to see the table structure as well as for profiling.

### AWS RDS IAM Authentication

For AWS RDS MySQL instances, you can use IAM authentication instead of traditional username/password authentication.

**Setup:**

Follow the [AWS RDS IAM Database Authentication](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html) documentation to:

- Enable IAM database authentication on your RDS instance
- Create database users that use IAM authentication
- Configure IAM policies with `rds-db:connect` permissions

**Configuration:**

Set `auth_mode: "IAM"` in your recipe and optionally configure `aws_config` for AWS credentials and region (see example below). If `aws_config` is not specified, boto3 will automatically use the default credential chain from environment variables, AWS config files, or IAM role metadata.
