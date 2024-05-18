import FeatureAvailability from '@site/src/components/FeatureAvailability';

# About DataHub Documentation Forms
<FeatureAvailability/>

DataHub Documentation Forms streamline the process of setting documentation requirements and delegating annotation responsibilities to the relevant data asset owners, stewards, and subject matter experts.

Forms are highly configurable, making it easy to ask the right questions of the right people, for a specific set of assets.  

## What are Documentation Forms?

You can think of Documentation Forms as a survey for your data assets: a set of questions that must be answered in order for an asset to be considered properly documented.


## Creating and Assigning Documentation Forms

Documentation Forms are defined via YAML with the following details:

- Name and Description to help end-users understand the scope and use case
- Form Type, either Documentation or Verification
	- Verification Forms require a final signoff, i.e. Verification, of all responses before the Form can be be considered complete
- Form Questions for end-users to complete
	- Questions can be applied at the asset-level and/or the field-level
	- Questions can be configured to be optional or required
- Target assets, defined by either:
	- A set of specific asset URNs
	- Assets related to a set of filters, such as Type, Platform, Domain, or Container
- Form Assignees
	- By defualt, asset owners will be assigned to the Documentation Form
	- Optionally assign specific DataHub users/groups to the form

Here's an example of defining a Documentation Form via YAML:
```yaml
- id: 123456
  # urn: "urn:li:form:123456"  # optional if id is provided
  type: VERIFICATION # Supported Types: DOCUMENTATION, VERIFICATION
  name: "Metadata Initiative 2023"
  description: "How we want to ensure the most important data assets in our organization have all of the most important and expected pieces of metadata filled out"
  prompts: # Questions for Form assignees to complete
    - id: "123"
      title: "Retention Time"
      description: "Apply Retention Time structured property to form"
      type: STRUCTURED_PROPERTY
      structured_property_id: io.acryl.privacy.retentionTime
      required: True # optional, will default to True
  entities: # Either pass a list of urns or a group of filters. This example shows a list of urns
    urns:
      - urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)
  # optionally assign the form to a specific set of users and/or groups
  # when omitted, form will be assigned to Asset owners
  actors: 
    users:
      - urn:li:corpuser:jane@email.com  # note: these should be urns
      - urn:li:corpuser:john@email.com
    groups:
      - urn:li:corpGroup:team@email.com  # note: these should be urns

```

:::Note
Documentation Forms currently only support defining Structured Properties as Form Questions
:::

## Completing Documentation Forms

<!-- Plain-language instructions of how to use the feature

Provide a step-by-step guide to use feature, including relevant screenshots and/or GIFs

* Where/how do you access it?
* What best practices exist?
* What are common code snippets?
 -->

## Additional Resources

### Videos

**Asset Verification in Acryl Cloud**

<p align="center">
	<iframe width="560" height="315" src="https://www.loom.com/embed/dd834d3cb8f041fca001cea19b2b4071?sid=7073dcd4-407c-41ec-b41d-c99f26dd6a2f" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>
</p> 

### API Tutorials

- [Create a Documentation Form](../../../docs/api/tutorials/forms.md)

:::Note
You must create a Structured Property before including it in a Documentation Form.
To learn more about creating Structured Properties via CLI, please see the [Create Structured Properties](/docs/api/tutorials/structured-properties.md) tutorial.
:::

### Related Features

- [DataHub Properties](/docs/features/feature-guides/properties.md)