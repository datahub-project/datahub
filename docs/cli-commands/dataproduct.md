# datahub dataproduct

**🤝 Version Compatibility:** `acryl-datahub>=0.10.2.4`

The dataproduct group of commands allows you to manage the lifecycle of a DataProduct entity on DataHub.
See the [Data Products](../dataproducts.md) page for more details on what a Data Product is and how DataHub represents it.

```shell
datahub dataproduct --help
Commands:
  upsert*          Upsert attributes to a Data Product in DataHub
  update           Create or Update a Data Product in DataHub.
  add_asset        Add an asset to a Data Product
  add_owner        Add an owner to a Data Product
  delete           Delete a Data Product in DataHub.
  diff             Diff a Data Product file with its twin in DataHub
  get              Get a Data Product from DataHub
  remove_asset     Add an asset to a Data Product
  remove_owner     Remove an owner from a Data Product
  set_description  Set description for a Data Product in DataHub
```

Here we detail the sub-commands available under the dataproduct group of commands:

## upsert

Use this to upsert a data product yaml file into DataHub. This will create the data product if it doesn't exist already. Remember, this will upsert all the fields that are specified in the yaml file and will not touch the fields that are not specified. For example, if you do not specify the `description` field in the yaml file, then `upsert` will not modify the description field on the Data Product entity in DataHub. To keep this file sync-ed with the metadata on DataHub use the [diff](#diff) command. The format of the yaml file is available [here](../dataproducts.md#creating-a-data-product-yaml--git).

```shell
# Usage
> datahub dataproduct upsert -f data_product.yaml

```

## update

Use this to fully replace a data product's metadata in DataHub from a yaml file. This will create the data product if it doesn't exist already. Remember, this will update all the fields including ones that are not specified in the yaml file. For example, if you do not specify the `description` field in the yaml file, then `update` will set the description field on the Data Product entity in DataHub to empty. To keep this file sync-ed with the metadata on DataHub use the [diff](#diff) command. The format of the yaml file is available [here](../dataproducts.md#creating-a-data-product-yaml--git).

```shell
# Usage
> datahub dataproduct upsert -f data_product.yaml

```

:::note

❗**Pro-Tip: upsert versus update**

Wondering which command is right for you? Use `upsert` if there are certain elements of metadata that you don't want to manage using the yaml file (e.g. owners, assets or description). Use `update` if you want to manage the entire data product's metadata using the yaml file.

:::

## diff

Use this to keep a data product yaml file updated from its server-side version in DataHub.

```shell
# Usage
> datahub dataproduct diff -f data_product.yaml --update
```

## get

Use this to get a data product entity from DataHub and optionally write it to a yaml file

```shell
# Usage
> datahub dataproduct get --urn urn:li:dataProduct:pet_of_the_week --to-file pet_of_the_week_dataproduct.yaml
{
  "id": "urn:li:dataProduct:pet_of_the_week",
  "domain": "urn:li:domain:dcadded3-2b70-4679-8b28-02ac9abc92eb",
  "assets": [
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.pet_details,PROD)",
    "urn:li:dashboard:(looker,dashboards.19)",
    "urn:li:dataFlow:(airflow,snowflake_load,prod)"
  ],
  "display_name": "Pet of the Week Campaign",
  "owners": [
    {
      "id": "urn:li:corpuser:jdoe",
      "type": "BUSINESS_OWNER"
    }
  ],
  "description": "This campaign includes Pet of the Week data.",
  "tags": [
    "urn:li:tag:adoption"
  ],
  "terms": [
    "urn:li:glossaryTerm:ClientsAndAccounts.AccountBalance"
  ],
  "properties": {
    "lifecycle": "production",
    "sla": "7am every day"
  }
}
Data Product yaml written to pet_of_the_week_dataproduct.yaml
```

## add_asset

Use this to add a data asset to a Data Product.

```shell
# Usage
> datahub dataproduct add_asset --urn "urn:li:dataProduct:pet_of_the_week"  --asset "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)"
```

## remove_asset

Use this to remove a data asset from a Data Product.

```shell
# Usage
> datahub dataproduct remove_asset --urn "urn:li:dataProduct:pet_of_the_week"  --asset "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)"
```

## add_owner

Use this to add an owner to a Data Product.

```shell
# Usage
> datahub dataproduct add_owner --urn "urn:li:dataProduct:pet_of_the_week"  --owner "jdoe@longtail.com" --owner-type BUSINESS_OWNER
```

## remove_owner

Use this to remove an owner from a Data Product.

```shell
# Usage
> datahub dataproduct remove_owner --urn "urn:li:dataProduct:pet_of_the_week"  --owner "urn:li:corpUser:jdoe@longtail.com"
```

## set_description

Use this to attach rich documentation for a Data Product in DataHub.

```shell
> datahub dataproduct set_description --urn "urn:li:dataProduct:pet_of_the_week" --description "This is the pet dataset"
# For uploading rich documentation from a markdown file, use the --md-file option
# > datahub dataproduct set_description --urn "urn:li:dataProduct:pet_of_the_week" --md-file ./pet_of_the_week.md
```

## delete

Use this to delete a Data Product from DataHub. Default to `--soft` which preserves metadata, use `--hard` to erase all metadata associated with this Data Product.

```shell
> datahub dataproduct delete --urn "urn:li:dataProduct:pet_of_the_week"
# For Hard Delete see below:
# > datahub dataproduct delete --urn "urn:li:dataProduct:pet_of_the_week" --hard
```
