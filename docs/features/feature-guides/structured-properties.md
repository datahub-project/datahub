import FeatureAvailability from '@site/src/components/FeatureAvailability';

# About DataHub Structured Properties

<FeatureAvailability/>

Structured Properties are an extension of DataHub's basic key-value properties, providing a structured and validated way to attach metadata to DataHub Assets, such as Datasets, Dashboards, etc. Structured Properties have a pre-defined type (Date, Integer, URN, String, etc.) and can be configured to only accept a specific set of allowed values, making it easier to ensure high levels of data quality and consistency. Additionally, end-users can easily edit/update the values of a Structured Property via the UI.

Available as of v0.13.1, Structured Properties are useful for setting standards of metadata collection, particularly in complaince and/or governance workflows. Here's an example:

Let's say your Compliance Team wants to ensure all Datasets are annotated with a valid retention period, a targeted deletion date, and capture which Complinace Officers are responsible ensuring these policies are met.

One approach would be to annotate all Datasets with basic key-value properties, but that leaves it up to the annotator to determine how to format that data. For example, a 30-day retention window could be captured as "Thirty Days", "30 Days", "30d", "1 Month", etc. with the same semantic meaning, but would be a mess to reconcile after the fact.

Instead, Structured Properties empower the Compliance Officer to set clear requirements of how that data must be captured, for example:

**Data Retention Period** - Predetermined storage duration before being deleted or archived based on legal, regulatory, or organizational requirements.
* Type: Single-select String
* Allowed Values: 30 Days, 90 Days, 12 Months, 24 Months, Indefinite

By setting the type (String), the cardinality (Single-select), and specifiying allowed values, you ensure that your DataHub users can only apply one of the approved values, ensuring consistency in data entry.

**Target Deletion Date** - Scheduled date when resource will be permanently deleted from the source system.
* Type: Single-select Date

By setting the type (Date) and cardinality (Single-select), you ensure that users will only submit one value with proper date formatting.

**Assigned Compliance Officer(s)** - Member(s) of the Compliance Team consulted/informed during audit.
* Type: URN, Multiple-select
* Allowed Types: corpuser, corpGroup

By setting the type (URN), cardinality (Multiple-select), and Allowed Types (User, Group) you ensure that users will choose one or more known DataHub user/groups, minimizing spelling errors, use of nicknames, etc.

## Creating and Assigning Structured Properties

Currently, Structured Properties can only be created and assigned to DataHub Assets via the DataHub CLI; please note that access tokens will require the `Edit All` Metadata Privilege.

:::Note
You must create a Structured Property before you can attach it to a DataHub Asset.
To learn more about creating and assinging Structured Properties via CLI, please see the [Create Structured Properties](/docs/api/tutorials/structured-properties.md) tutorial.
:::

Users with the `Edit Properties` Metadata Privilege will be able to make changes to Structured Property values via the DataHub UI.


## Viewing and Editing Structured Properties

After a Structured Property has been assigned to an Asset, users can view those values on the `Properties` Tab:

<!-- PLACEHOLDER/TO-DO:

	- NEED SCREENSHOT OF TAB WITH BASIC & STRUCTURED PROPERTIES
	- SCREENSHOT OF EDIT MODAL
-->

Users with the `Edit Properties` privilege will have the option to edit 

### Videos

**Deep Dive: UI-Editable Properties**

<p align="center">
<iframe width="560" height="315" src="https://www.youtube.com/embed/06zaQyKxJYk?si=H_YiwQty25m2xzaP" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>
</p> 


### API

You can create a structured property by DataHub CLI.
Follow the below tutorial to create a structured property.

- [Create Structured Properties](/docs/api/tutorials/structured-properties.md)


## FAQ and Troubleshooting

**What is the difference between Properties and Structured Properties in DataHub?**

DataHub Properties are key-value pairs of strings which capture additional information about assets that is not readily available in standard metadata fields. Properties can also be used in advanced search to filter and sort to improve end-users’ search experience.

Structured Properties are an extension of DataHub's basic key-value properties, providing a structured and validated way to attach metadata to DataHub Assets. Structured Properties have a pre-defined type (Date, Integer, URN, String, etc.) and can be configured to only accept a specific set of allowed values, making it easier to ensure high levels of data quality and consistency.

| Properties | Structured Properties |
| --- | --- |
| Extremely flexible | Validated namespace and type |
| Added/edited during ingestion and/or via API | Defined via YAML and created/assigned via CLI |
| No support for UI-based Edits | Support for UI-based edits |


**Why can't I edit the value of a Structured Property from the DataHub UI?**

1. Your version of DataHub does not support UI-based edits of Structured Properties. Confirm you are running DataHub v0.13.1 or later.
2. You are attempting to edit a Basic Propterty, not a Structured Property. Confirm you are attempting to edit a Structured Property, which will have an "Edit" button visible. Please note that basic DataHub Properties are not eligible for UI-based edits to minimize overwirtes during recurring ingestion.
3. You do not have have the necessary privileges. Confirm with your Admin that you have the `Edit Properties` Metadata Privilege.


### Related Features

- [DataHub Forms](/docs/features/feature-guides/forms.md)