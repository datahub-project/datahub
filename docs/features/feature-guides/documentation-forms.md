import FeatureAvailability from '@site/src/components/FeatureAvailability';

# About DataHub Documentation Forms
<FeatureAvailability/>

DataHub Documentation Forms streamline the process of setting documentation requirements and delegating annotation responsibilities to the relevant data asset owners, stewards, and subject matter experts.

Forms are highly configurable, making it easy to ask the right questions of the right people, for a specific set of assets.  

## What are Documentation Forms?

You can think of Documentation Forms as a survey for your data assets: a set of questions that must be answered in order for an asset to be considered properly documented.

Verification Forms are an extension of Documentation Forms, requiring a final verification, or sign-off, on all responses before the asset can be considered Verified. This is useful for compliance and/or governance annotation initiatives where you want assignees to provide a final acknowledgement that the information provided is correct.

## Creating and Assigning Documentation Forms

Documentation Forms are defined via YAML with the following details:

- Name and Description to help end-users understand the scope and use case
- Form Type, either Documentation or Verification
	- Verification Forms require a final signoff, i.e. Verification, of all required questions before the Form can be considered complete
- Form Questions (aka "prompts") for end-users to complete
	- Questions can be assigned at the asset-level and/or the field-level
	- Asset-level questions can be configured to be required; by default, all questions are optional
- Assigned Assets, defined by:
	- A set of specific asset URNs, OR
	- Assets related to a set of filters, such as Type (Datasets, Dashboards, etc.), Platform (Snowflake, Looker, etc.), Domain (Product, Marketing, etc.), or Container (Schema, Folder, etc.)
- Optional: Form Assignees
	- Optionally assign specific DataHub users/groups to complete the Form for all relevant assets
	- If omitted, any Owner of an Asset can complete Forms assigned to that Asset

Here's an example of defining a Documentation Form via YAML:
```yaml
- id: 123456
  # urn: "urn:li:form:123456"  # optional if id is provided
  type: VERIFICATION # Supported Types: DOCUMENTATION, VERIFICATION
  name: "Metadata Initiative 2024"
  description: "How we want to ensure the most important data assets in our organization have all of the most important and expected pieces of metadata filled out"
  prompts: # Questions for Form assignees to complete
    - id: "123"
      title: "Data Retention Time"
      description: "Apply Retention Time structured property to form"
      type: STRUCTURED_PROPERTY
      structured_property_id: io.acryl.privacy.retentionTime
      required: True # optional; default value is False
  entities: # Either pass a list of urns or a group of filters. This example shows a list of urns
    urns:
      - urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)
  # optionally assign the form to a specific set of users and/or groups
  # when omitted, form will be assigned to Asset owners
  actors: 
    users:
      - urn:li:corpuser:jane@email.com  # note: these should be URNs
      - urn:li:corpuser:john@email.com
    groups:
      - urn:li:corpGroup:team@email.com  # note: these should be URNs

```

:::note
Documentation Forms currently only support defining Structured Properties as Form Questions
:::

<!-- ## Completing Documentation Forms -->

<!-- Plain-language instructions of how to use the feature

Provide a step-by-step guide to use feature, including relevant screenshots and/or GIFs

* Where/how do you access it?
* What best practices exist?
* What are common code snippets?
 -->

## Additional Resources

### Videos

**Asset Verification in DataHub Cloud**

<p align="center">
	<iframe width="560" height="315" src="https://www.loom.com/embed/dd834d3cb8f041fca001cea19b2b4071?sid=7073dcd4-407c-41ec-b41d-c99f26dd6a2f" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>
</p> 

## FAQ and Troubleshooting

**What is the difference between Documentation and Verification Forms?**

Both form types are a way to configure a set of optional and/or required questions for DataHub users to complete. When using Verification Forms, users will be presented with a final verification step once all required questions have been completed; you can think of this as a final acknowledgement of the accuracy of information submitted.

**Who is able to complete Forms in DataHub?**

By default, any owner of an Asset will be able to respond to questions assigned via a Form.

When assigning a Form to an Asset, you can optionally assign specific DataHub users/groups to fill them out.

**Can I assign multiple Forms to a single asset?**

You sure can! Please keep in mind that an Asset will only be considered Documented or Verified if all required questions are completed on all assiged Forms.

### API Tutorials

- [API Guides on Documentation Form](../../../docs/api/tutorials/forms.md)

:::note
You must create a Structured Property before including it in a Documentation Form.
To learn more about creating Structured Properties via CLI, please see the [Create Structured Properties](/docs/api/tutorials/structured-properties.md) tutorial.
:::

### Related Features

- [DataHub Properties](/docs/features/feature-guides/properties.md)