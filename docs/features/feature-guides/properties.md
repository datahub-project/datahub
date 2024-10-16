import FeatureAvailability from '@site/src/components/FeatureAvailability';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# About DataHub Properties
<FeatureAvailability/>

DataHub Custom Properties and Structured Properties are powerful tools to collect meaningful metadata for Assets that might not perfectly fit into other Aspects within DataHub, such as Glossary Terms, Tags, etc. Both types can be found in an Asset's Properties tab:

<p align="center">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/properties/custom_and_structured_properties.png"/>
</p>

This guide will explain the differences and use cases of each property type.

## What are Custom Properties and Structured Properties?
Here are the differences between the two property types at a glance:

| Custom Properties | Structured Properties |
| --- | --- |
| Map of key-value pairs stored as strings | Validated namespaces and data types |
| Added to assets during ingestion and via API | Defined via YAML; created and added to assets via CLI |
| No support for UI-based Edits | Support for UI-based edits |

**Custom Properties** are key-value pairs of strings that capture additional information about assets that is not readily available in standard metadata fields. Custom Properties can be added to assets automatically during ingestion or programmatically via API and *cannot* be edited via the UI.
<p align="center">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/properties/custom_properties_highlight.png"/>
</p>
<p align="center"><em>Example of Custom Properties assigned to a Dataset</em></p>

**Structured Properties** are an extension of Custom Properties, providing a structured and validated way to attach metadata to DataHub Assets. Available as of v0.13.1, Structured Properties have a pre-defined type (Date, Integer, URN, String, etc.). They can be configured to only accept a specific set of allowed values, making it easier to ensure high levels of data quality and consistency. Structured Properties are defined via YAML, added to assets via CLI, and can be edited via the UI.
<p align="center">
  <img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/properties/structured_properties_highlight.png"/>
</p>
<p align="center"><em>Example of Structured Properties assigned to a Dataset</em></p>

## Use Cases for Custom Properties and Structured Properties
**Custom Properties** are useful for capturing raw metadata from source systems during ingestion or programmatically via API. Some examples include:

- GitHub file location of code which generated a dataset
- Data encoding type
- Account ID, cluster size, and region where a dataset is stored

**Structured Properties** are useful for setting and enforcing standards of metadata collection, particularly in support of compliance and governance initiatives. Values can be added programmatically via API, then manually via the DataHub UI as necessary. Some examples include:

- Deprecation Date
  - Type: Date, Single Select
  - Validation: Must be formatted as 'YYYY-MM-DD'
- Data Retention Period
  - Type: String, Single Select
  - Validation: Adheres to allowed values "30 Days", "90 Days", "365 Days", or "Indefinite"
- Consulted Compliance Officer, chosen from a list of DataHub users
  - Type: DataHub User, Multi-Select
  - Validation: Must be valid DataHub User URN

By using Structured Properties, compliance and governance officers can ensure consistency in data collection across assets.

## Creating, Assigning, and Editing Structured Properties

Structured Properties are defined via YAML, then created and assigned to DataHub Assets via the DataHub CLI. 

Here's how we would define the above examples in YAML:

<Tabs>
<TabItem value="deprecationDate" label="Deprecation Date" default>

```yaml
- id: deprecation_date
  qualified_name: deprecation_date
  type: date # Supported types: date, string, number, urn, rich_text
  cardinality: SINGLE # Supported options: SINGLE, MULTIPLE
  display_name: Deprecation Date
  description: "Scheduled date when resource will be deprecated in the source system"
  entity_types: # Define which types of DataHub Assets the Property can be assigned to
    - dataset
```

</TabItem>
<TabItem value="dataRetentionPeriod" label="Data Retention Period">

```yaml
- id: retention_period
  qualified_name: retention_period
  type: string # Supported types: date, string, number, urn, rich_text
  cardinality: SINGLE # Supported options: SINGLE, MULTIPLE
  display_name: Data Retention Period
  description: "Predetermined storage duration before being deleted or archived 
                based on legal, regulatory, or organizational requirements"
  entity_types: # Define which types of DataHub Assets the Property can be assigned to
    - dataset
  allowed_values:
    - value: "30 Days"
      description: "Use this for datasets that are ephemeral and contain PII"
    - value: "90 Days"
      description: "Use this for datasets that drive monthly reporting but contain PII"
    - value: "365 Days"
      description: "Use this for non-sensitive data that can be retained for longer"
    - value: "Indefinite"
      description: "Use this for non-sensitive data that can be retained indefinitely"
```

</TabItem>
<TabItem value="consultedComplianceOfficer" label="Consulted Compliance Officer(s)">

```yaml
- id: compliance_officer
  qualified_name: compliance_officer
  type: urn # Supported types: date, string, number, urn, rich_text
  cardinality: MULTIPLE # Supported options: SINGLE, MULTIPLE
  display_name: Consulted Compliance Officer(s)
  description: "Member(s) of the Compliance Team consulted/informed during audit"
  type_qualifier: # Define the type of Asset URNs to allow
    - corpuser
    - corpGroup
  entity_types: # Define which types of DataHub Assets the Property can be assigned to
    - dataset
```

</TabItem>
</Tabs>

:::note
To learn more about creating and assigning Structured Properties via CLI, please see the [Create Structured Properties](/docs/api/tutorials/structured-properties.md) tutorial.
:::

Once a Structured Property is assigned to an Asset, Users with the `Edit Properties` Metadata Privilege will be able to change Structured Property values via the DataHub UI.
<p align="center">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/properties/edit_structured_properties_modal.png"/>
</p>
<p align="center"><em>Example of editing the value of a Structured Property via the UI</em></p>

### Videos

**Deep Dive: UI-Editable Properties**

<p align="center">
<iframe width="560" height="315" src="https://www.youtube.com/embed/06zaQyKxJYk?si=H_YiwQty25m2xzaP" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>
</p> 


### API

Please see the following API guides related to Custom and Structured Properties:

- [Custom Properties API Guide](/docs/api/tutorials/structured-properties.md)
- [Structured Properties API Guide](/docs/api/tutorials/structured-properties.md)


## FAQ and Troubleshooting

**Why can't I edit the value of a Structured Property from the DataHub UI?**
1. Your version of DataHub does not support UI-based edits of Structured Properties. Confirm you are running DataHub v0.13.1 or later.
2. You are attempting to edit a Custom Property, not a Structured Property. Confirm you are trying to edit a Structured Property, which will have an "Edit" button visible. Please note that Custom Properties are not eligible for UI-based edits to minimize overwrites during recurring ingestion.
3. You do not have the necessary privileges. Confirm with your Admin that you have the `Edit Properties` Metadata Privilege.

### Related Features

- [Documentation Forms](/docs/features/feature-guides/documentation-forms.md)