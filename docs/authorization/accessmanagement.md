import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Access Management

<FeatureAvailability/>

## Introduction
DataHub provides the ability to use access management to associate roles with datasets. Users can 
request READ, WRITE, or ADMIN access types directly in the UI at the individual asset level.

## Configuration


By default, the Access Management feature is *disabled*. If you would like to enable it,
you can simply set the `SHOW_ACCESS_MANAGEMENT` environment variable for the `datahub-gms` service container
to `true`. For example in your `docker/datahub-gms/docker.env`, you'd configure

```
SHOW_ACCESS_MANAGEMENT=true
```

## UI Location
Under a dataset, the new tab "Access Management" should appear if configured correctly.

<p align="center">
 <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/roles/accessmanagement.png" />
</p>

## Data Model
Access management introduces a new entity in DataHub's metadata model called a Role.
A Role is comprised of:

* A unique key (URN)
* Properties of the role (name, description, type, request URL)
* A list of users that have been provisioned the role

This role must then be associated with datasets through a new aspect called access.
:::note Important Note
Currently, only Dataset entities support Access Management.
:::

:::caution Do not confuse role with datahubrole. The former is an external role definition while the latter is for the management of privileges within DataHub itself (i.e., the admin role can accept proposed metadata changes).
:::

## Access Management Through the DataHub CLI

### Creating External Roles
You can create external roles using the DataHub CLI.
``` bash
datahub put --urn "urn:li:role:reader" --aspect roleProperties -d - <<-EOF
{
  "name": "Snowflake Reader Role",
  "description": "Description for Snowflake Reader Role",
  "type": "READ",
  "requestUrl": "http://custom-url-for-redirection.com"
}
EOF
```

### Assigning Users to Roles
Optional: Assign pre-existing DataHub users to a role.
``` bash
datahub put --urn "urn:li:role:reader" --aspect actors -d - <<-EOF
{
  "users": [
    {"user": "urn:li:corpuser:datahubuser"}
  ] 
}
EOF
```

### Assigning Roles to Datasets
Associate the role you just created to a dataset.
``` bash
datahub put --urn "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)" --aspect access -d - <<-EOF
{
  "roles": [
    {"urn": "urn:li:role:reader"},
    {"urn": "urn:li:role:writer"}
  ]
}
EOF
```

## Access Management Through the Python API
You can also manage access using the Python SDK

``` python
import datahub.emitter.mce_builder as builder
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import AccessClass, RoleAssociationClass, ChangeTypeClass

dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_table,PROD)"

access_aspect = AccessClass(
   roles=[RoleAssociationClass(urn="urn:li:role:reader")]
)

mcp = MetadataChangeProposalWrapper(
   changeType=ChangeTypeClass.UPSERT,
   entityUrn=dataset_urn,
   aspectName="access",
   aspect=access_aspect
)

emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
emitter.emit(mcp)
```

## What's Next for Access Management
Future enhancements planned for Access Management include:

* Modeling external policies in addition to just roles
* Automatically extracting roles/policies from sources like BigQuery, Snowflake, etc.
