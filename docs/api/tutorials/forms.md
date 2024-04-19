import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Forms

## Why Would You Use Forms?

Forms are a way for end-users to fill out all mandatory attributes associated with a data asset. The form will be dynamically generated based on the definitions provided by administrators and stewards and matching rules.

Learn more about forms in the [Forms Feature Guide](../../../docs/features/feature-guides/forms.md).


### Goal Of This Guide
This guide will show you how to create and read forms.

## Prerequisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data.
For detailed information, please refer to [Datahub Quickstart Guide](/docs/quickstart.md).


<Tabs>
<TabItem value="CLI" label="CLI">

Install the relevant CLI version. Forms are available as of CLI version `0.13.1`. The corresponding SaaS release version is `v0.2.16.5`
Connect to your instance via [init](https://datahubproject.io/docs/cli/#init):

1. Run `datahub init` to update the instance you want to load into
2. Set the server to your sandbox instance, `https://{your-instance-address}/gms`
3. Set the token to your access token


</TabItem>
</Tabs>


## Create Structured Properties


<Tabs>
<TabItem value="CLI" label="CLI">

Create a yaml file representing the forms you’d like to load. 
For example, below file represents a form `123456``. You can see the full example [here](https://github.com/datahub-project/datahub/blob/example-yaml-sp/metadata-ingestion/examples/forms/forms.yaml).
        
```yaml
- id: 123456
  # urn: "urn:li:form:123456"  # optional if id is provided
  type: VERIFICATION
  name: "Metadata Initiative 2023"
  description: "How we want to ensure the most important data assets in our organization have all of the most important and expected pieces of metadata filled out"
  prompts:
    - id: "123"
      title: "Retention Time"
      description: "Apply Retention Time structured property to form"
      type: STRUCTURED_PROPERTY
      structured_property_id: io.acryl.privacy.retentionTime
      required: True # optional, will default to True
    - id: "92847"
      title: "Replication SLA"
      description: "Apply Replication SLA structured property to form"
      type: STRUCTURED_PROPERTY
      structured_property_urn: urn:li:structuredProperty:io.acryl.dataManagement.replicationSLA
      required: True
    - title: "compliance_officer"
      description: "Compliance officer"
      type: STRUCTURED_PROPERTY
      structured_property_id: compliance_officer
      required: True
    - id: "76543"
      title: "Replication SLA"
      description: "Apply Replication SLA structured property to form"
      type: FIELDS_STRUCTURED_PROPERTY
      structured_property_urn: urn:li:structuredProperty:io.acryl.dataManagement.replicationSLA
      required: False
  entities: # Either pass a list of urns or a group of filters. This example shows a list of urns
    urns:
      - urn:li:dataset:(urn:li:dataPlatform:hive,user.clicks,PROD)
      - urn:li:dataset:(urn:li:dataPlatform:snowflake,user.clicks,PROD)
      - urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)
```

You can apply forms to either a list of entity urns, or a list of filters. For a list of entity urns, use this structure:
    
```
entities:
urns:
  - urn:li:dataset:...
```
    
For a list of filters, use this structure:
    
```
entities:
filters:
  types:
    - dataset  # you can use entity type name or urn
  platforms:
    - snowflake  # you can use platform name or urn
  domains:
    - urn:li:domain:finance  # you must use domain urn
  containers:
    - urn:li:container:my_container  # you must use container urn
```

Note that you can filter to entity types, platforms, domains, and/or containers.

Use the CLI to create your properties:

```commandline
datahub forms upsert -f {forms_yaml}
```

If successful, you should see `Created form urn:li:form:...`

</TabItem>
</Tabs>

## Read Property Definition


<Tabs>
<TabItem value="CLI" label="CLI">

You can see the properties you created by running the following command:

```commandline
datahub forms get --urn {urn}
```
For Example, you can run `datahub forms get --urn urn:li:form:123456`.

If successful, you should see metadata about your form returned.

</TabItem>
</Tabs>
