import FeatureAvailability from '@site/src/components/FeatureAvailability';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Data Products

<FeatureAvailability/>

**🤝 Version compatibility**
> Open Source DataHub: **0.10.3** | Acryl: **0.2.8**

## What are Data Products?
Data Products are an innovative way to organize and manage your Data Assets, such as Tables, Topics, Views, Pipelines, Charts, Dashboards, etc., within DataHub. These Data Products belong to a specific Domain and can be easily accessed by various teams or stakeholders within your organization.

## Why Data Products?
A key concept in data mesh architecture, Data Products are independent units of data managed by a specific domain team. They are responsible for defining, publishing, and maintaining their data assets while ensuring high-quality data that meets the needs of its consumers. 

## Benefits of Data Products
Data Products help in curating a coherent set of logical entities, simplifying data discovery and governance. By grouping related Data Assets into a Data Product, it allows stakeholders to discover and understand available data easily, supporting data governance efforts by managing and controlling access to Data Products. 

## How Can You Use Data Products?
Data Products can be easily published to the DataHub catalog, allowing other teams to discover and consume them. By doing this, data teams can streamline the process of sharing data, making data-driven decisions faster and more efficient.

## Data Products Setup, Prerequisites, and Permissions

What you need to create and add data products:

* **Manage Data Product** metadata privilege for Domains to create/delete Data Products at the entity level. If a user has this privilege for a given Domain, they will be able to create and delete Data Products underneath it.
* **Edit Data Product** metadata privilege to add or remove the Data Product for a given entity.

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

This applies to other fields as well, such as owners, ownership types, tags, and terms.

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

There are many more advanced cli commands for managing Data Products as code. Take a look at the [Data Products section](./cli.md#dataproduct-data-product-entity) on the CLI reference guide for more details.


### What updates are planned for the Data Products feature?

The following features are next on the roadmap for Data Products
- Support for marking data assets in a Data Product as private versus shareable for other teams to consume
- Support for declaring data lineage manually to upstream and downstream data products
- Support for declaring logical schema for Data Products
- Support for associating data contracts with Data Products
- Support for semantic versioning of the Data Product entity


### Related Features

* [Domains](./domains.md)
* [Glossary Terms](./glossary/business-glossary.md)
* [Tags](./tags.md)



