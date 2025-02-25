import FeatureAvailability from '@site/src/components/FeatureAvailability';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Access Management

<FeatureAvailability/>

## Introduction

DataHub's Access Management feature allows you to associate external roles from your source systems with your data assets in DataHub. This creates a unified view of access control across your data ecosystem, helping data consumers:

1. **Discover available access** - Find what roles are already provisioned for them across different data platforms
2. **Request appropriate access** - Easily identify and request to join the appropriate role for the access they need
3. **Simplify governance** - Streamline the access management process by centralizing role information in DataHub

By integrating your external roles into DataHub, teams can reduce access request friction and ensure users have the right level of access to the data they need.

## Configuration

### Self-hosted DataHub

For self-hosted DataHub deployments, the Access Management feature is *disabled* by default. To enable it,
simply set the `SHOW_ACCESS_MANAGEMENT` environment variable for the `datahub-gms` service container
to `true`. For example in your `docker/datahub-gms/docker.env`, you'd configure:

```
SHOW_ACCESS_MANAGEMENT=true
```

### DataHub Cloud

If you're using DataHub Cloud (managed by Acryl), enabling the Access Management feature just requires contacting your Acryl Customer Success representative. They can enable this feature for your environment without any configuration changes on your part.

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

:::caution Do not confuse role with datahubrole
The "role" entity refers to an external role definition that exists in your source systems (like Snowflake or BigQuery), while "datahubrole" is for the management of privileges within DataHub itself (i.e., the admin role can accept proposed metadata changes).
:::

## Managing Access Through DataHub

You can set up Access Management through either the CLI or Python API. Here's how to complete the three main steps:

### Creating External Roles

<Tabs>
<TabItem value="cli" label="CLI">

```bash
datahub put --urn "urn:li:role:reader" --aspect roleProperties -d - <<-EOF
{
  "name": "Snowflake Reader Role",
  "description": "Description for Snowflake Reader Role",
  "type": "READ",
  "requestUrl": "http://custom-url-for-redirection.com"
}
EOF
```

</TabItem>
<TabItem value="python" label="Python">

```python
import datahub.emitter.mce_builder as builder
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import RolePropertiesClass, ChangeTypeClass

# Create a role properties aspect
role_properties = RolePropertiesClass(
    name="Snowflake Reader Role",
    description="Description for Snowflake Reader Role",
    type="READ",
    requestUrl="http://custom-url-for-redirection.com"
)

# Create a metadata change proposal
mcp = MetadataChangeProposalWrapper(
    changeType=ChangeTypeClass.UPSERT,
    entityUrn="urn:li:role:reader",
    aspectName="roleProperties",
    aspect=role_properties
)

# Emit the metadata
emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
emitter.emit(mcp)
```

</TabItem>
</Tabs>

### Assigning Users to Roles (Optional)

<Tabs>
<TabItem value="cli" label="CLI">

```bash
datahub put --urn "urn:li:role:reader" --aspect actors -d - <<-EOF
{
  "users": [
    {"user": "urn:li:corpuser:datahubuser"}
  ] 
}
EOF
```

</TabItem>
<TabItem value="python" label="Python">

```python
from datahub.metadata.schema_classes import ActorsClass, ActorClass

# Create an actors aspect
actors = ActorsClass(
    users=[
        ActorClass(user="urn:li:corpuser:datahubuser")
    ]
)

# Create a metadata change proposal
mcp = MetadataChangeProposalWrapper(
    changeType=ChangeTypeClass.UPSERT,
    entityUrn="urn:li:role:reader",
    aspectName="actors",
    aspect=actors
)

# Emit the metadata
emitter.emit(mcp)
```

</TabItem>
</Tabs>

### Assigning Roles to Datasets

<Tabs>
<TabItem value="cli" label="CLI">

```bash
datahub put --urn "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)" --aspect access -d - <<-EOF
{
  "roles": [
    {"urn": "urn:li:role:reader"},
    {"urn": "urn:li:role:writer"}
  ]
}
EOF
```

</TabItem>
<TabItem value="python" label="Python">

```python
from datahub.metadata.schema_classes import AccessClass, RoleAssociationClass

dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)"

# Create an access aspect with multiple roles
access_aspect = AccessClass(
   roles=[
       RoleAssociationClass(urn="urn:li:role:reader"),
       RoleAssociationClass(urn="urn:li:role:writer")
   ]
)

# Create a metadata change proposal
mcp = MetadataChangeProposalWrapper(
   changeType=ChangeTypeClass.UPSERT,
   entityUrn=dataset_urn,
   aspectName="access",
   aspect=access_aspect
)

# Emit the metadata
emitter.emit(mcp)
```

</TabItem>
</Tabs>

## Use Cases

Here are some common scenarios where integrating external roles into DataHub is valuable:

1. **Unified Access View** - Data engineers can see all users with access to sensitive data across multiple platforms from a single interface
2. **Self-Service Access Requests** - Analysts can discover what roles they need to access specific datasets and request them directly from DataHub
3. **Access Auditing** - Compliance teams can review who has access to which datasets through which roles
4. **Onboarding Acceleration** - New team members can quickly discover what access they need for their role

## Demo and Examples

To see Access Management in action, check out our [DataHub Townhall demo](https://youtu.be/mXsn33tALCA?t=1333) where we showcase how to use this feature in a real-world scenario.

## What's Next for Access Management

Future enhancements planned for Access Management include:

* Modeling external policies in addition to just roles
* Automatically extracting roles/policies from sources like BigQuery, Snowflake, etc.
* Extending support to more entity types beyond datasets
* Advanced access request workflows with approvals