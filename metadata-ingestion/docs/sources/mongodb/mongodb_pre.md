### Overview

The `mongodb` module ingests metadata from Mongodb into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

#### Required Permissions

The ingestion user must have the `read` role on each database to be ingested, or
`readAnyDatabase` to ingest all accessible databases:

```js
// Grant read access to a specific database
db.grantRolesToUser("datahub_ingest", [{ role: "read", db: "your_database" }]);
// Or grant read access to all databases
db.grantRolesToUser("datahub_ingest", [
  { role: "readAnyDatabase", db: "admin" },
]);
```

:::tip Note on system collections

MongoDB `system.*` collections (such as `system.profile` and `system.views`) are excluded
from ingestion by default via the `excludeSystemCollections` option (enabled by default).
This means the `read` role is sufficient for all standard ingestion use cases. If you
disable `excludeSystemCollections`, ingestion of `system.profile` requires the `dbAdmin`
role — see [Config Details](#config-details) for more information.

If you are upgrading from a version that ingested system collections by default and have
MongoDB profiling enabled, you may encounter an authorization error during ingestion. As a
workaround, you can explicitly deny system collections in your recipe:

```yaml
source:
  type: mongodb
  config:
    collection_pattern:
      deny:
        - ".*\\.system\\..*"
```

:::

#### Authentication

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

##### Username/Password (DEFAULT, SCRAM)

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

##### AWS IAM Authentication (MONGODB-AWS)

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
