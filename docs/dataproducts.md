---
title: Data Products
sidebar_label: Data Products
slug: /dataproducts
custom_edit_url: 'https://github.com/datahub-project/datahub/blob/master/docs/dataproducts.md'
---
import FeatureAvailability from '@site/src/components/FeatureAvailability';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Data Products

<FeatureAvailability/>

Starting in version `0.10.3` (open-source) and `0.2.8` (Acryl), DataHub supports the concept of **Data Products**. A Data Product is a collection of Data Assets within DataHub that belongs to a specific Domain and can be consumed by other teams or stakeholders within an organization. Data Assets can include Tables, Topics, Views, Pipelines, Charts, Dashboards, and any other entity that is supported by DataHub.

A Data Product is a key concept in the data mesh architecture. In a data mesh, a Data Product is an autonomous and self-contained unit of data that is owned and managed by a particular domain team. Each Data Product is responsible for defining, publishing, and maintaining its own data assets, as well as ensuring that the data is of high quality and meets the needs of its intended consumers.

The benefits of Data Products include the creation of a curated set of logical entities that help with data discovery and governance. By grouping related Data Assets together into a Data Product, data teams can create a cohesive and consistent view of data within a specific domain, which makes it easier for stakeholders to discover and understand the available data. This approach also supports data governance efforts by enabling data teams to manage and control access to Data Products as a whole, ensuring that the data is used appropriately and in compliance with any relevant regulations or policies.

Data Products can be published to the DataHub catalog for other teams to discover and consume within their own Domain. By making data products available through DataHub, data teams can streamline the process of sharing data and reduce the time required to make data-driven decisions within their specific Domain.

## Data Products Setup, Prerequisites, and Permissions

What you need to create and add data products:

* **Manage Data Product** platform privilege to add data products at the entity level

You can create this privileges by creating a new [Metadata Policy](./authorization/policies.md).


## Using Data Products

Data Products can be created using the UI or via a YAML file that is managed using software engineering (GitOps) practices.

### Creating a Data Product (UI)

To create a Data Product, first navigate to the Domain that will contain this Data Product. 

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/a84499c124c9123d6831a0e6ad8dd8caf70203a0/imgs/data_products/dataproducts-tab.png"/>
</p>

Then navigate to the Data Products tab on the Domain's home page, and click '+ New Data Product'. 
This will open a new modal where you can configure the settings for your data product. Inside the form, you can choose a name for your Data Product. Most often, this will align with the logical purpose of the Data Product, for example
'Customer Orders' or 'Revenue Attribution'. You can also add documentation for your product to help other users easily discover it. Don't worry, this can be changed later.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/a84499c124c9123d6831a0e6ad8dd8caf70203a0/imgs/data_products/dataproducts-create.png"/>
</p>

Once you've chosen a name and a description, click 'Create' to create the new Data Product. Once you've created the Data Product, you can click on it to continue on to the next step, adding assets to it.

### Assigning an Asset to a Data Product (UI)

You can assign an asset to a Data Product either using the Data Product page as the starting point or the Asset's page as the starting point.
On a Data Product page, click the 'Add Assets' button on the top right corner to add assets to the Data Product.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/a84499c124c9123d6831a0e6ad8dd8caf70203a0/imgs/data_products/dataproducts-add-assets.png"/>
</p>

On an Asset's profile page, use the right sidebar to locate the Data Product section. Click 'Set Data Product', and then search for the Data Product you'd like to add this asset to. When you're done, click 'Add'.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/a84499c124c9123d6831a0e6ad8dd8caf70203a0/imgs/data_products/dataproducts-set.png"/>
</p>

To remove an asset from a Data Product, click the 'x' icon on the Data Product label. 

> Notice: Adding or removing an asset from a Data Product requires the `Edit Data Product` Metadata Privilege, which can be granted
> by a [Policy](authorization/policies.md).

### Creating a Data Product (YAML + git)
DataHub ships with a YAML-based Data Product spec for defining and managing Data Products as code.

Here is an example of a Data Product named "Pet of the Week" which belongs to the **Marketing** domain and contains three data assets. The **Spec** tab describes the JSON Schema spec for a DataHub data product file.

<Tabs>
<TabItem value="sample" label="Example" default>

```yaml
{{ inline /metadata-ingestion/examples/data_product/dataproduct.yaml show_path_as_comment }}
```

:::note

When bare domain names like `Marketing` is used, `datahub` will first check if a domain like `urn:li:domain:Marketing` is provisioned, failing that; it will check for a provisioned domain that has the same name. If we are unable to resolve bare domain names to provisioned domains, then yaml-based ingestion will refuse to proceeed until the domain is provisioned on DataHub.

:::

You can also provide fully-qualified domain names (e.g. `urn:li:domain:dcadded3-2b70-4679-8b28-02ac9abc92eb`) to ensure that no ingestion-time domain resolution is needed.

</TabItem>
<TabItem value="schema" label="Spec">

```json
{{ inline /docs/generated/specs/schemas/dataproduct_schema.json }}

```

</TabItem>
</Tabs>


To sync this yaml file to DataHub, use the `datahub` cli via the `dataproduct` group of commands.
```shell
datahub dataproduct upsert -f user_dataproduct.yaml
```

### Keeping the YAML file sync-ed with changes in UI

The `datahub` cli allows you to keep this YAML file synced with changes happening in the UI. All you have to do is run the `datahub dataproduct diff` command.

Here is an example invocation that checks if there is any diff and updates the file in place:
```shell
datahub dataproduct diff -f user_dataproduct.yaml --update
```

This allows you to manage your data product definition in git while still allowing for edits in the UI. Business Users and Developers can both collaborate on the definition of a data product with ease using this workflow.


### Advanced cli commands for managing Data Products

There are many more advanced cli commands for managing Data Products as code. Take a look at the [Data Products section](./cli.md#dataproduct) on the CLI reference guide for more details.


### What updates are planned for the Data Products feature?

The following features are next on the roadmap for Data Products
- Support for marking data assets in a Data Product as private versus shareable for other teams to consume
- Support for declaring lineage manually to upstream and downstream data products
- Support for declaring logical schema for Data Products
- Support for associating data contracts with Data Products
- Support for semantic versioning of the Data Product entity


### Related Features

* [Domains](./domains.md)
* [Glossary Terms](./glossary/business-glossary.md)
* [Tags](./tags.md)


*Need more help? Join the conversation in [Slack](http://slack.datahubproject.io)!*
