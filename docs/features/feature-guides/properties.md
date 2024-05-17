import FeatureAvailability from '@site/src/components/FeatureAvailability';

# About DataHub Properties
<FeatureAvailability/>

DataHub Custom Properties and Strutured Properties are a powerful tools way to collect meaningful metadata for Assets that might not perflectly fit into other Aspects within DataHub, such as Glossary Terms, Tags, etc. Both types can be found in an Asset's Properties tab:

<p align="center">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/properties/custom_and_structured_properties.png"/>
</p>

This guide will explain the differences and use cases of each property type.

### What are Custom Properties and Structured Properties?
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

### Use Cases for Custom Properties and Structured Properties
**Custom Propterties** are useful for capturing raw metadata from source systems during ingestion or programmatically via API. Some examples include:

- GitHub file location of code which generated a dataset
- Data encoding type
- Account ID, cluster size, and region where a dataset is stored

**Structured Properties** are useful for setting and enforcing standards of metadata collection, particularly in support of complaince and governance initiatives. Values can be added programmatically via API, then manually via the DataHub UI as necessary. Some examples include:

- Deprecation Date
	- Type: Date
	- Validation: Must be formatted as 'YYYY-MM-DD'
- Data Retention Period
	- Type: String
	- Validation: Adheres to allowed values "30 Days", "90 Days", "365 Days", or "Indefinite"
- Consulted Complaince Officer, chosen from a list of DataHub users
	- Type: DataHub User
	- Validation: Must be valid DataHub User URN

By using Structured Properties, complaince and governance offericers can ensure consistency in data collection accross assets.

## Creating, Assigning, and Editing Structured Properties

Structured Properties can only be created and assigned to DataHub Assets via the DataHub CLI; please note that access tokens will require the `Edit All` Metadata Privilege.

:::Note
You must create a Structured Property before attaching it to a DataHub Asset.
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
2. You are attempting to edit a Custom Property, not a Structured Property. Confirm you are trying to edit a Structured Property, which will have an "Edit" button visible. Please note that Custom Properties are not eligible for UI-based edits to minimize overwirtes during recurring ingestion.
3. You do not have have the necessary privileges. Confirm with your Admin that you have the `Edit Properties` Metadata Privilege.

### Related Features

- [DataHub Forms](/docs/features/feature-guides/forms.md)