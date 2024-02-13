# Business Attributes


## What are Business Attributes
A Business attribute is a centrally managed logical field that represents a unique schema field entity. This common construct is global in nature, i.e. it is not bound to a project or application implementation. Instead, its identity exists in representing the same field across various datasets owned by various different projects and applications. Projects or applications use the Business attribute to model a column in a dataset and inherit information about it such as definitions, data type, data quality rules/assertions, tags, glossary terms etc from the global definition.

## Benefits of Business Attributes
Data architects can use the concept of the business attribute to validate whether applications are conformant with the applicable metadata defined for the business attribute. By abstracting common business metadata into a logical model, different personas with appropriate business knowledge can define pertinent details, like rich definition, business use for the attribute, classification (i.e. PII, sensitive, shareable etc.), specific data rules that govern the attribute, connection to glossary terms.

With Business Attributes users have the ability to search associated datasets using business description/tags/glossary attached to business attribute
## How can you use Business Attributes
Business attributes can be used to define a common set of metadata for a logical field that is used across multiple datasets. This metadata can be used to drive data quality, data governance, and data discovery. For example, a business attribute can be used to define a common set of data quality rules that are applicable to a logical field across multiple datasets. This can be used to ensure that the same data quality rules are applied consistently across all datasets that use the logical field.

## Business Attributes Setup, Prerequisites, and Permissions
What you need to create/update and associate business attributes to dataset schema field

* **Manage Business Attributes** platform privilege to create/update/delete business attributes.
* **Edit Dataset Column Business Attribute** metadata privilege to associate business attributes to dataset schema field.

## Using Business Attributes
As of now Business Attributes can only be created through UI

### Creating a Business Attribute (UI)
To create a Business Attribute, first navigate to the Business Attributes tab on the home page.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/e100603e3fbae1f9b14d14c9f4d86744e7310d18/imgs/business_attributes/businessattribute-tab.png"/>

Then click on '+ Create Business Attribute'.
This will open a new modal where you can configure the settings for your business attribute. Inside the form, you can choose a name for your Business Attribute. Most often, this will align with the logical purpose of the Business Attribute, 
for example 'Customer ID'. You can also add documentation for your Business Attribute to help other users easily discover it. This can be changed later. 

We can also add Datatype for Business Attribute. It has String as a default value.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/e100603e3fbae1f9b14d14c9f4d86744e7310d18/imgs/business_attributes/businessattribute-create.png"/>

Once you've chosen a name and a description, click 'Create' to create the new Business Attribute.

Then we can attach tags and glossary terms to it to make it more discoverable.

### Assigning Business Attribute to a Dataset Schema Field (UI)
You can associate the business attribute to a dataset schema field using the Dataset's schema page as the starting point. As per design, there is one to one mapping between business attribute and dataset schema field.

On a Dataset's schema page, click the 'Add Attribute' to add business attribute to the dataset schema field.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/e100603e3fbae1f9b14d14c9f4d86744e7310d18/imgs/business_attributes/businessattribute-associate-datasetschemafield.png"/>


After association, dataset schema field gets its description, tags and glossary inherited from Business attribute. 
Description inherited from business attribute is greyed out to differentiate between original description of schema field. 

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/e100603e3fbae1f9b14d14c9f4d86744e7310d18/imgs/business_attributes/dataset-inherits-businessattribute-properties.png"/>

### What updates are planned for the Business Attributes feature?

- Ingestion of Business attributes through recipe file (YAML)
- AutoTagging of Business attributes to child datasets through lineage

### Related Features
* [Glossary Terms](./glossary/business-glossary.md)
* [Tags](./tags.md)