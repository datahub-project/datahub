import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Business Attributes

<FeatureAvailability ossOnly />

>**Note:** This is <b>BETA</b> feature

## What are Business Attributes
A Business Attribute, as its name implies, is an attribute with a business focus. It embodies the traits or properties of an entity within a business framework. This attribute is a crucial piece of data for a business, utilised to define or control the entity throughout the organisation. If a business process or concept is depicted as a comprehensive logical model, then each Business Attribute can be considered as an individual component within that model. While business names and descriptions are generally managed through glossary terms, Business Attributes encompass additional characteristics such as data quality rules/assertions, data privacy markers, data usage protocols, standard tags, and supplementary documentation, alongside Names and Descriptions.

For instance, "United States - Social Security Number" comes with a Name and definition. However, it also includes an abbreviation, a Personal Identifiable Information (PII) classification tag, a set of data rules, and possibly some additional references.

## Benefits of Business Attributes
The principle of "Define Once; Use in Many Places" applies to Business Attributes. Information Stewards can establish these attributes once with all their associated characteristics in an enterprise environment. Subsequently, individual applications or data owners can link their dataset attributes with these Business Attributes. This process allows the complete metadata structure built for a Business Attribute to be inherited. Application owners can also use these attributes to check if their applications align with the organisation-wide standard descriptions and data policies. This approach aids in centralised management for enhanced control and enforcement of metadata standards.

This standardised metadata can be employed to facilitate data quality, data governance, and data discovery use cases within the organisation.

A collection of 'related' Business Attributes can create a logical business model.

With Business Attributes users have the ability to search associated datasets using business description/tags/glossary attached to business attribute
## How can you use Business Attributes
Business Attributes can be utilised in any of the following scenario:
Attributes that are frequently used across multiple domains, data products, projects, and applications.
Attributes requiring standardisation and inheritance of their characteristics, including name and descriptions, to be propagated.
Attributes that need centralised management for improved control and standard enforcement.

A Business Attribute could be used to accelerate and standardise business definition management at entity / fields a field across various datasets. This ensures consistent application of the characteristics across all datasets using the Business Attribute. Any change in the them requires a change at only one place (i.e., business attributes) and change can then be inherited across all the application & datasets in the organisation

Taking the example of "United States- Social Security Number", if an application or data owner  has multiple instances of the social security number within their datasets, they can link all these dataset attributes with a Business Attribute to inherit all the aforementioned characteristics. Additionally, users can search for associated datasets using the business description, tags, or glossary linked to the Business Attribute.

## Business Attributes Setup, Prerequisites, and Permissions
What you need to create/update and associate business attributes to dataset schema field

* **Manage Business Attributes** platform privilege to create/update/delete business attributes.

## Using Business Attributes
As of now Business Attributes can only be created through UI

### Creating a Business Attribute (UI)
To create a Business Attribute, first navigate to the Business Attributes tab on the home page.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/88472958703d5e9236f71bb457c1acd481d123af/imgs/business_attributes/businessattribute-tab.png"/>
</p>

Then click on '+ Create Business Attribute'.
This will open a new modal where you can configure the settings for your business attribute. Inside the form, you can choose a name for Business Attribute. Most often, this will align with the logical purpose of the Business Attribute, 
for example 'Social Security Number'. You can also add documentation for your Business Attribute to help other users easily discover it. This can be changed later. 

We can also add datatype for Business Attribute. It has String as a default value.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/88472958703d5e9236f71bb457c1acd481d123af/imgs/business_attributes/businessattribute-create.png"/>
</p>

Once you've chosen a name and a description, click 'Create' to create the new Business Attribute.

Then we can attach tags and glossary terms to it to make it more discoverable.

### Assigning Business Attribute to a Dataset Schema Field (UI)
You can associate the business attribute to a dataset schema field using the Dataset's schema page as the starting point. As per design, there is one to one mapping between business attribute and dataset schema field.

On a Dataset's schema page, click the 'Add Attribute' to add business attribute to the dataset schema field.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/88472958703d5e9236f71bb457c1acd481d123af/imgs/business_attributes/businessattribute-associate-datasetschemafield.png"/>
</p>

After association, dataset schema field gets its description, tags and glossary inherited from Business attribute. 
Description inherited from business attribute is greyed out to differentiate between original description of schema field. Similarly, tags and glossary terms inherited can't be removed directly.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/88472958703d5e9236f71bb457c1acd481d123af/imgs/business_attributes/dataset-inherits-businessattribute-properties.png"/>
</p>

### Enable Business Attributes Feature
By default, business attribute is disabled. To enable Business Attributes feature, export environmental variable 
(may be done via `extraEnvs` for GMS deployment):
```shell
BUSINESS_ATTRIBUTE_ENTITY_ENABLED=true
```

### What updates are planned for the Business Attributes feature?

- Ingestion of Business attributes through recipe file (YAML)
- AutoTagging of Business attributes to child datasets through lineage

### Related Features
* [Glossary Terms](./glossary/business-glossary.md)
* [Tags](./tags.md)