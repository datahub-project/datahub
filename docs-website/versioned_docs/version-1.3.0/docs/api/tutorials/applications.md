---
title: Applications (Experimental - Beta Feature)
slug: /api/tutorials/applications
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/api/tutorials/applications.md
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Applications (Experimental - Beta Feature)

## Why Would You Use Applications?

Applications are groupings of assets based on a particular purpose, similar to domains and data products.
For more information on what an Application is, and how it differs from other concepts, refer to [About DataHub Applications](/docs/features/feature-guides/applications.md).

### Goal Of This Guide

This guide will show you how to

- Create an application.
- Read the application attached to a dataset.
- Add a dataset to an application
- Remove the application from a dataset.

## Prerequisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data.
For detailed steps, please refer to [Datahub Quickstart Guide](/docs/quickstart.md).

## Create Application

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```json
mutation createApplication {
  createApplication(
    input: {
      properties: {
        name: "My New Application"
        description: "An optional description"
      }
    }
  )
}
```

If you see the following response, the operation was successful:

```json
{
  "data": {
    "createApplication": "<application_urn>"
  },
  "extensions": {}
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
# Inlined from /metadata-ingestion/examples/library/create_application.py
#!/usr/bin/env python3

from datahub.emitter.mce_builder import make_dataset_urn, make_user_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    ApplicationPropertiesClass,
    ApplicationsClass,
    DomainsClass,
    GlobalTagsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    TagAssociationClass,
)


# Utility function for creating application URNs (not yet in SDK)
def make_application_urn(application_id: str) -> str:
    """Create a DataHub application URN."""
    return f"urn:li:application:{application_id}"


def create_banking_application():
    """Complete example of creating an application and associating entities."""

    # Initialize emitter
    emitter = DatahubRestEmitter(gms_server="http://localhost:8080", token="")

    try:
        # Create application
        application_urn = make_application_urn("banking-app")

        # 1. Application Properties
        application_properties = ApplicationPropertiesClass(
            name="Banking Application",
            description="Core banking application handling customer accounts and transactions.",
        )

        emitter.emit(
            MetadataChangeProposalWrapper(
                entityUrn=application_urn, aspect=application_properties
            )
        )

        # 2. Ownership
        ownership = OwnershipClass(
            owners=[
                OwnerClass(
                    owner=make_user_urn("john.smith@company.com"),
                    type=OwnershipTypeClass.TECHNICAL_OWNER,
                ),
                OwnerClass(
                    owner=make_user_urn("jane.doe@company.com"),
                    type=OwnershipTypeClass.BUSINESS_OWNER,
                ),
            ]
        )
        emitter.emit(
            MetadataChangeProposalWrapper(entityUrn=application_urn, aspect=ownership)
        )

        # 3. Tags and Domain
        tags = GlobalTagsClass(tags=[TagAssociationClass(tag="urn:li:tag:Production")])
        emitter.emit(
            MetadataChangeProposalWrapper(entityUrn=application_urn, aspect=tags)
        )

        domains = DomainsClass(domains=["urn:li:domain:finance-domain"])
        emitter.emit(
            MetadataChangeProposalWrapper(entityUrn=application_urn, aspect=domains)
        )

        # 4. Associate datasets
        datasets = ["customer_accounts", "transaction_history", "account_balances"]

        for dataset_name in datasets:
            dataset_urn = make_dataset_urn("hive", dataset_name, "PROD")
            applications_aspect = ApplicationsClass(applications=[application_urn])

            emitter.emit(
                MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn, aspect=applications_aspect
                )
            )

        print(f"Successfully created application: {application_urn}")
        print(f"Associated {len(datasets)} datasets with the application")

    finally:
        emitter.close()


if __name__ == "__main__":
    create_banking_application()

```

</TabItem>
</Tabs>

### Expected Outcomes of Creating Applications

You can now see the applications under `Applications` sidebar section.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/master//imgs/applications/ManageApplicationsScreen.png"/>
</p>

## Read Applications

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```json
query {
  dataset(urn: "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)") {
    application {
      application {
        urn
        properties {
          name
          description
        }
      }
    }
  }
}
```

If you see the following response, the operation was successful:

```python
{
  "data": {
    "dataset": {
      "application": {
        "application": {
          "urn": "urn:li:application:71b3bf7b-2e3f-4686-bfe1-93172c8c4e10",
          "properties": {
            "name": "Cancellation Processing"
          }
        }
      }
    }
  },
  "extensions": {}
}
```

</TabItem>
</Tabs>

## Add Application

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```json
mutation batchSetApplication {
  batchSetApplication(
    input: {
      resourceUrns: [
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,banking.public.customer,PROD)"
      ]
      applicationUrn: "urn:li:application:new-customer-signup"
    }
  )
}
```

If you see the following response, the operation was successful:

```json
{
  "data": {
    "batchSetApplication": true
  },
  "extensions": {}
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
# Inlined from /metadata-ingestion/examples/library/add_application.py
#!/usr/bin/env python3
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import ApplicationsClass


# Utility function for creating application URNs (not yet in SDK)
def make_application_urn(application_id: str) -> str:
    """Create a DataHub application URN."""
    return f"urn:li:application:{application_id}"


def add_application_aspect():
    emitter = DatahubRestEmitter(gms_server="http://localhost:8080", token="")
    dataset_urn = make_dataset_urn("snowflake", "database.schema.table", "PROD")

    application_urn = make_application_urn("my_application")
    applications_aspect = ApplicationsClass(applications=[application_urn])

    emitter.emit(
        MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=applications_aspect)
    )

    print(f"Successfully added application: {application_urn}")


if __name__ == "__main__":
    add_application_aspect()

```

</TabItem>
</Tabs>

### Expected Outcomes of Adding Application

You can now see the application has been added to the dataset.

## Remove Applications

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```json
mutation batchSetApplication {
  batchSetApplication(
    input: {
      resourceUrns: [
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,banking.public.customer,PROD)"
      ],
      applicationUrn: null
    }
  )
}
```

Expected Response:

```python
{
  "data": {
    "batchSetApplication": true
  },
  "extensions": {}
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
# Inlined from /metadata-ingestion/examples/library/remove_application.py
#!/usr/bin/env python3

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import ApplicationsClass


def remove_application_aspect():
    emitter = DatahubRestEmitter(gms_server="http://localhost:8080", token="")
    dataset_urn = make_dataset_urn("snowflake", "database.schema.table", "PROD")

    applications_aspect = ApplicationsClass(applications=[])

    emitter.emit(
        MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=applications_aspect)
    )

    print("Successfully removed application")


if __name__ == "__main__":
    remove_application_aspect()

```

</TabItem>
</Tabs>
