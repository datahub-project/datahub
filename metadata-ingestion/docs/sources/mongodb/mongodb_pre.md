## Prerequisites and Permissions

**Important:**
The user account used for MongoDB ingestion must have the `readWrite` role on each database to be ingested. Schema inference and sampling logic executes on system collections (such as `system.profile` and `system.views`), which are not permitted with only `read` or `readAnyDatabase` roles. Without `readWrite`, ingestion will fail with an authorization error.

## Authentication

The `authMechanism` config field maps directly to PyMongo's
[authentication mechanisms](https://pymongo.readthedocs.io/en/stable/examples/authentication.html).
Supported values:

| `authMechanism` | Use Case                                                                                                                                                                                 | Required Fields                            |
| --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------ |
| `DEFAULT`       | SCRAM auth (MongoDB default). Automatically negotiates SCRAM-SHA-256 or SCRAM-SHA-1 based on server version.                                                                             | `username`, `password`                     |
| `SCRAM-SHA-256` | Explicit SCRAM-SHA-256 (MongoDB 4.0+).                                                                                                                                                   | `username`, `password`                     |
| `SCRAM-SHA-1`   | Explicit SCRAM-SHA-1.                                                                                                                                                                    | `username`, `password`                     |
| `MONGODB-AWS`   | AWS IAM authentication for MongoDB Atlas or AWS DocumentDB. Credentials are resolved from the environment (env vars, EC2 instance profile, ECS task role, EKS pod identity) via `boto3`. | See below                                  |
| `MONGODB-X509`  | X.509 certificate authentication.                                                                                                                                                        | TLS options via `connect_uri` or `options` |

### Username/Password (DEFAULT, SCRAM)

The simplest configuration - supply `username` and `password` directly:

```yaml
source:
  type: mongodb
  config:
    connect_uri: "mongodb://host:27017"
    username: "${MONGODB_USER}"
    password: "${MONGODB_PASSWORD}"
    authMechanism: "DEFAULT"
```

### AWS IAM Authentication (MONGODB-AWS)

Set `authMechanism: "MONGODB-AWS"` to authenticate using AWS IAM
credentials. The connector resolves credentials automatically via
`boto3`'s standard credential chain:

1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
   `AWS_SESSION_TOKEN`)
2. Shared credentials/config files (`~/.aws/credentials`)
3. EC2 instance metadata / instance profile
4. ECS container credentials
5. EKS Pod Identity / IRSA (IAM Roles for Service Accounts)

```yaml
source:
  type: mongodb
  config:
    connect_uri: "mongodb+srv://cluster.example.mongodb.net"
    authMechanism: "MONGODB-AWS"
    hostingEnvironment: "ATLAS" # or "AWS_DOCUMENTDB"
```

To supply explicit AWS credentials instead of using the credential chain:

```yaml
source:
  type: mongodb
  config:
    connect_uri: "mongodb://docdb-cluster.region.docdb.amazonaws.com:27017/?tls=true&tlsCAFile=/path/to/rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"
    username: "${AWS_ACCESS_KEY_ID}"
    password: "${AWS_SECRET_ACCESS_KEY}"
    authMechanism: "MONGODB-AWS"
    hostingEnvironment: "AWS_DOCUMENTDB"
```
