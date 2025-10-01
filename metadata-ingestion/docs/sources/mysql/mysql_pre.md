### Prerequisites

In order to execute this source the user credentials needs the following privileges

- `grant select on DATABASE.* to 'USERNAME'@'%'`
- `grant show view on DATABASE.* to 'USERNAME'@'%'`

`select` is required to see the table structure as well as for profiling.

### AWS RDS IAM Authentication

For AWS RDS MySQL instances, you can use IAM authentication instead of traditional username/password authentication.

**Prerequisites:**

1. **Enable IAM Database Authentication** on your RDS instance
2. **Create an IAM database user** in MySQL:
   ```sql
   CREATE USER 'iam_user' IDENTIFIED WITH AWSAuthenticationPlugin AS 'RDS';
   GRANT SELECT ON database_name.* TO 'iam_user'@'%';
   GRANT SHOW VIEW ON database_name.* TO 'iam_user'@'%';
   ```
3. **Configure IAM policy** to allow RDS connection:
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": "rds-db:connect",
         "Resource": "arn:aws:rds-db:region:account-id:dbuser:db-resource-id/iam_user"
       }
     ]
   }
   ```
4. **Configure AWS credentials** via AWS CLI, environment variables, or IAM role

**Configuration:**
Set `use_rds_iam: true` and specify `aws_region` in your recipe (see example below).
