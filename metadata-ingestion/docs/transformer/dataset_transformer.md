---
title: "Dataset"
---
# Dataset Transformers 
The below table shows transformer which can transform aspects of entity [Dataset](../../../docs/generated/metamodel/entities/dataset.md).

| Dataset Aspect      | Transformer                                                                                                                                                                                                       |                                                                                               
|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `status`            | - [Mark Dataset status](#mark-dataset-status)                                                                                                                                                                     |
| `ownership`         | - [Simple Add Dataset ownership](#simple-add-dataset-ownership)<br/> - [Pattern Add Dataset ownership](#pattern-add-dataset-ownership)<br/> - [Simple Remove Dataset Ownership](#simple-remove-dataset-ownership)<br/> - [Extract Ownership from Tags](#extract-ownership-from-tags)<br/> - [Clean suffix prefix from Ownership](#clean-suffix-prefix-from-ownership) |
| `globalTags`        | - [Simple Add Dataset globalTags ](#simple-add-dataset-globaltags)<br/> - [Pattern Add Dataset globalTags](#pattern-add-dataset-globaltags)<br/> - [Add Dataset globalTags](#add-dataset-globaltags)              |
| `browsePaths`       | - [Set Dataset browsePath](#set-dataset-browsepath)                                                                                                                                                               |
| `glossaryTerms`     | - [Simple Add Dataset glossaryTerms ](#simple-add-dataset-glossaryterms)<br/> - [Pattern Add Dataset glossaryTerms](#pattern-add-dataset-glossaryterms)<br/> - [Tags to Term Mapping](#tags-to-term-mapping)                                                           |
| `schemaMetadata`    | - [Pattern Add Dataset Schema Field glossaryTerms](#pattern-add-dataset-schema-field-glossaryterms)<br/> - [Pattern Add Dataset Schema Field globalTags](#pattern-add-dataset-schema-field-globaltags)            |
| `datasetProperties` | - [Simple Add Dataset datasetProperties](#simple-add-dataset-datasetproperties)<br/> - [Add Dataset datasetProperties](#add-dataset-datasetproperties)                                                            |
| `domains`           | - [Simple Add Dataset domains](#simple-add-dataset-domains)<br/> - [Pattern Add Dataset domains](#pattern-add-dataset-domains)<br/> - [Domain Mapping Based on Tags](#domain-mapping-based-on-tags)                                                                                    |
| `dataProduct`       | - [Simple Add Dataset dataProduct ](#simple-add-dataset-dataproduct)<br/> - [Pattern Add Dataset dataProduct](#pattern-add-dataset-dataproduct)<br/> - [Add Dataset dataProduct](#add-dataset-dataproduct)  

## Extract Ownership from Tags
### Config Details
| Field                       | Required | Type    | Default       | Description                                 |
|-----------------------------|----------|---------|---------------|---------------------------------------------|
| `tag_pattern`  |          | str     |               | Regex to use for tags to match against. Supports Regex to match a pattern which is used to remove content. Rest of string is considered owner ID for creating owner URN. |
| `is_user`      |          | bool    | `true`   | Whether should be consider a user or not. If `false` then considered a group. |
| `tag_character_mapping` |          | dict[str, str]  |     | A mapping of tag character to datahub owner character. If provided, `tag_pattern` config should be matched against converted tag as per mapping|
| `email_domain` |          | str    |    | If set then this is appended to create owner URN. |
| `extract_owner_type_from_tag_pattern` |          | str    |  `false`   | Whether to extract an owner type from provided tag pattern first group. If `true`, no need to provide owner_type and owner_type_urn config. For example: if provided tag pattern is `(.*)_owner_email:` and actual tag is `developer_owner_email`, then extracted owner type will be `developer`.|
| `owner_type` |          | str    |  `TECHNICAL_OWNER`   | Ownership type. |
| `owner_type_urn` |          | str    |  `None`   | Set to a custom ownership type's URN if using custom ownership. |

Let’s suppose we’d like to add a dataset ownerships based on part of dataset tags. To do so, we can use the `extract_ownership_from_tags` transformer that’s included in the ingestion framework.

The config, which we’d append to our ingestion recipe YAML, would look like this:

```yaml
transformers:
  - type: "extract_ownership_from_tags"
    config:
      tag_pattern: "owner_email:"
```

So if we have input dataset tag like
- `urn:li:tag:owner_email:abc@email.com`
- `urn:li:tag:owner_email:xyz@email.com`

The portion of the tag after the matched tag pattern will be converted into an owner. Hence users `abc@email.com` and `xyz@email.com` will be added as owners.

### Examples

- Add owners, however owner should be considered as group and also email domain not provided in tag string. For example: from tag urn `urn:li:tag:owner:abc` extracted owner urn should be `urn:li:corpGroup:abc@email.com` then config would look like this:
    ```yaml
    transformers:
      - type: "extract_ownership_from_tags"
        config:
          tag_pattern: "owner:"
          is_user: false
          email_domain: "email.com"
    ```
- Add owners, however owner type and owner type urn wanted to provide externally. For example: from tag urn `urn:li:tag:owner_email:abc@email.com` owner type should be `CUSTOM` and owner type urn as `"urn:li:ownershipType:data_product"` then config would look like this:
    ```yaml
    transformers:
      - type: "extract_ownership_from_tags"
        config:
          tag_pattern: "owner_email:"
          owner_type: "CUSTOM"
          owner_type_urn: "urn:li:ownershipType:data_product"
    ```
- Add owners, however some tag characters needs to replace with some other characters before extracting owner. For example: from tag urn `urn:li:tag:owner__email:abc--xyz-email_com` extracted owner urn should be `urn:li:corpGroup:abc.xyz@email.com` then config would look like this:
    ```yaml
    transformers:
      - type: "extract_ownership_from_tags"
        config:
          tag_pattern: "owner_email:"
          tag_character_mapping:
            "_": "."
            "-": "@"
            "--": "-"
            "__": "_"
    ```
- Add owners, however owner type also need to extracted from tag pattern. For example: from tag urn `urn:li:tag:data_producer_owner_email:abc@email.com` extracted owner type should be `data_producer` then config would look like this:
    ```yaml
    transformers:
      - type: "extract_ownership_from_tags"
        config:
          tag_pattern: "(.*)_owner_email:"
          extract_owner_type_from_tag_pattern: true
    ```

## Clean suffix prefix from Ownership
### Config Details
| Field                       | Required | Type    | Default       | Description                                 |
|-----------------------------|----------|---------|---------------|---------------------------------------------|
| `pattern_for_cleanup`                 | ✅         | list[string]    |    | List of suffix/prefix to remove from the Owner URN(s) |


Matches against a Onwer URN and remove the matching part from the Owner URN

```yaml
transformers:
  - type: "pattern_cleanup_ownership"
    config:
      pattern_for_cleanup:
        - "ABCDEF"
        - (?<=_)(\w+)
```

## Mark Dataset Status
### Config Details
| Field                       | Required | Type    | Default       | Description                                 |
|-----------------------------|----------|---------|---------------|---------------------------------------------|
| `removed`                | ✅        | boolean |               | Flag to control visbility of dataset on UI. |

If you would like to stop a dataset from appearing in the UI, then you need to mark the status of the dataset as removed. 

You can use this transformer in your source recipe to mark status as removed.

```yaml
transformers:
  - type: "mark_dataset_status"
    config:
      removed: true
```
## Simple Add Dataset ownership 
### Config Details
| Field              | Required | Type         | Default     | Description                                                                                                |
|--------------------|----------|--------------|-------------|------------------------------------------------------------------------------------------------------------|
| `owner_urns`       | ✅        | list[string] |             | List of owner urns.                                                                                        |
| `ownership_type`   |          | string       | "DATAOWNER" | ownership type of the owners (either as enum or ownership type urn)                                        |
| `replace_existing` |          | boolean      | `false`     | Whether to remove ownership from entity sent by ingestion source.                                          |
| `semantics`        |          | enum         | `OVERWRITE` | Whether to OVERWRITE or PATCH the entity present on DataHub GMS.                                           |
| `on_conflict`      |          | enum         | `DO_UPDATE` | Whether to make changes if domains already exist. If set to DO_NOTHING, `semantics` setting is irrelevant. |

For transformer behaviour on `replace_existing` and `semantics`, please refer section [Relationship Between replace_existing And semantics](#relationship-between-replace_existing-and-semantics).

<br/>
Let’s suppose we’d like to append a series of users who we know to own a dataset but aren't detected during normal ingestion. To do so, we can use the `simple_add_dataset_ownership` transformer that’s included in the ingestion framework.

The config, which we’d append to our ingestion recipe YAML, would look like this:

Below configuration will add listed owner_urns in ownership aspect

```yaml
transformers:
  - type: "simple_add_dataset_ownership"
    config:
      owner_urns:
        - "urn:li:corpuser:username1"
        - "urn:li:corpuser:username2"
        - "urn:li:corpGroup:groupname"
      ownership_type: "PRODUCER"
```


`simple_add_dataset_ownership` can be configured in below different way 

- Add owners, however replace existing owners sent by ingestion source
    ```yaml
    transformers:
      - type: "simple_add_dataset_ownership"
        config:
          replace_existing: true  # false is default behaviour
          owner_urns:
            - "urn:li:corpuser:username1"
            - "urn:li:corpuser:username2"
            - "urn:li:corpGroup:groupname"
          ownership_type: "urn:li:ownershipType:__system__producer"
    ```
- Add owners, however overwrite the owners available for the dataset on DataHub GMS
    ```yaml
    transformers:
      - type: "simple_add_dataset_ownership"
        config:
          semantics: OVERWRITE  # OVERWRITE is default behaviour 
          owner_urns:
            - "urn:li:corpuser:username1"
            - "urn:li:corpuser:username2"
            - "urn:li:corpGroup:groupname"
          ownership_type: "urn:li:ownershipType:__system__producer" 
    ```
- Add owners, however keep the owners available for the dataset on DataHub GMS
    ```yaml
    transformers:
      - type: "simple_add_dataset_ownership"
        config:
          semantics: PATCH
          owner_urns:
            - "urn:li:corpuser:username1"
            - "urn:li:corpuser:username2"
            - "urn:li:corpGroup:groupname"
          ownership_type: "PRODUCER"
    ```

## Pattern Add Dataset ownership 
### Config Details
| Field              | Required | Type                 | Default     | Description                                                                                                                  |
|--------------------|----------|----------------------|-------------|------------------------------------------------------------------------------------------------------------------------------|
| `owner_pattern`    | ✅        | map[regx, list[urn]] |             | entity urn with regular expression and list of owners urn apply to matching entity urn.                                      |
| `ownership_type`   |          | string               | "DATAOWNER" | ownership type of the owners (either as enum or ownership type urn)                                                          |
| `replace_existing` |          | boolean              | `false`     | Whether to remove owners from entity sent by ingestion source.                                                               |
| `semantics`        |          | enum                 | `OVERWRITE` | Whether to OVERWRITE or PATCH the entity present on DataHub GMS.                                                             |
| `is_container`     |          | bool                 | `false`     | Whether to also consider a container or not. If true, then ownership will be attached to both the dataset and its container. |
| `on_conflict`      |          | enum                 | `DO_UPDATE` | Whether to make changes if domains already exist. If set to DO_NOTHING, `semantics` setting is irrelevant.                   |

let’s suppose we’d like to append a series of users who we know to own a different dataset from a data source but aren't detected during normal ingestion. To do so, we can use the `pattern_add_dataset_ownership` module that’s included in the ingestion framework.  This will match the pattern to `urn` of the dataset and assign the respective owners.

If the is_container field is set to true, the module will not only attach the ownerships to the matching datasets but will also find and attach containers associated with those datasets. This means that both the datasets and their containers will be associated with the specified owners.

The config, which we’d append to our ingestion recipe YAML, would look like this:

  ```yaml
  transformers:
    - type: "pattern_add_dataset_ownership"
      config:
        owner_pattern:
          rules:
            ".*example1.*": ["urn:li:corpuser:username1"]
            ".*example2.*": ["urn:li:corpuser:username2"]
        ownership_type: "DEVELOPER"
  ```

`pattern_add_dataset_ownership` can be configured in below different way 

- Add owner, however replace existing owner sent by ingestion source
    ```yaml
    transformers:
      - type: "pattern_add_dataset_ownership"
        config:
          replace_existing: true  # false is default behaviour
          owner_pattern:
            rules:
              ".*example1.*": ["urn:li:corpuser:username1"]
              ".*example2.*": ["urn:li:corpuser:username2"]
          ownership_type: "urn:li:ownershipType:__system__producer"
    ```
- Add owner, however overwrite the owners available for the dataset on DataHub GMS
    ```yaml
    transformers:
      - type: "pattern_add_dataset_ownership"
        config:
          semantics: OVERWRITE  # OVERWRITE is default behaviour 
          owner_pattern:
            rules:
              ".*example1.*": ["urn:li:corpuser:username1"]
              ".*example2.*": ["urn:li:corpuser:username2"]
          ownership_type: "urn:li:ownershipType:__system__producer" 
    ```
- Add owner, however keep the owners available for the dataset on DataHub GMS
    ```yaml
    transformers:
      - type: "pattern_add_dataset_ownership"
        config:
          semantics: PATCH
          owner_pattern:
            rules:
              ".*example1.*": ["urn:li:corpuser:username1"]
              ".*example2.*": ["urn:li:corpuser:username2"]
          ownership_type: "PRODUCER"
    ```
- Add owner to dataset and its containers
  ```yaml
  transformers:
    - type: "pattern_add_dataset_ownership"
      config:
        is_container: true
        replace_existing: true  # false is default behaviour
        semantics: PATCH / OVERWRITE # Based on user 
        owner_pattern:
          rules:
            ".*example1.*": ["urn:li:corpuser:username1"]
            ".*example2.*": ["urn:li:corpuser:username2"]
        ownership_type: "PRODUCER"
  ```
⚠️ Warning:
When working with two datasets in the same container but with different owners, all owners will be added for that dataset containers.

For example:
```yaml
transformers:
  - type: "pattern_add_dataset_ownership"
    config:
      is_container: true
      owner_pattern:
        rules:
          ".*example1.*": ["urn:li:corpuser:username1"]
          ".*example2.*": ["urn:li:corpuser:username2"]
```
If example1 and example2 are in the same container, then both urns urn:li:corpuser:username1 and urn:li:corpuser:username2 will be added for respective dataset containers.

## Simple Remove Dataset ownership
If we wanted to clear existing owners sent by ingestion source we can use the `simple_remove_dataset_ownership` transformer which removes all owners sent by the ingestion source.

```yaml
transformers:
  - type: "simple_remove_dataset_ownership"
    config: {}
```

The main use case of `simple_remove_dataset_ownership` is to remove incorrect owners present in the source. You can use it along with the [Simple Add Dataset ownership](#simple-add-dataset-ownership) to remove wrong owners and add the correct ones.

Note that whatever owners you send via `simple_remove_dataset_ownership` will overwrite the owners present in the UI.
## Extract Dataset globalTags
### Config Details
| Field                       | Required | Type         | Default       | Description                                                      |
|-----------------------------|----------|--------------|---------------|------------------------------------------------------------------|
| `extract_tags_from`         | ✅       | string       |  `urn`             | Which field to extract tag from. Currently only `urn` is supported.  |
| `extract_tags_regex`        | ✅       | string       |  `.*`             | Regex to use to extract tag.|
| `replace_existing`          |          | boolean      | `false`       | Whether to remove globalTags from entity sent by ingestion source.   |
| `semantics`                 |          | enum         | `OVERWRITE`   | Whether to OVERWRITE or PATCH the entity present on DataHub GMS. |

Let’s suppose we’d like to add a dataset tags based on part of urn. To do so, we can use the `extract_dataset_tags` transformer that’s included in the ingestion framework.

The config, which we’d append to our ingestion recipe YAML, would look like this:

  ```yaml
  transformers:
    - type: "extract_dataset_tags"
      config:
        extract_tags_from: "urn"
        extract_tags_regex: ".([^._]*)_"
  ```

So if we have input URNs like 
- `urn:li:dataset:(urn:li:dataPlatform:kafka,clusterid.USA-ops-team_table1,PROD)`
- `urn:li:dataset:(urn:li:dataPlatform:kafka,clusterid.Canada-marketing_table1,PROD)`

a tag called `USA-ops-team` and `Canada-marketing` will be added to them respectively. This is helpful in case you are using prefixes in your datasets to segregate different things. Now you can turn that segregation into a tag on your dataset in DataHub for further use.


## Simple Add Dataset globalTags
### Config Details
| Field                       | Required | Type         | Default       | Description                                                      |
|-----------------------------|----------|--------------|---------------|------------------------------------------------------------------|
| `tag_urns`                  | ✅        | list[string] |               | List of globalTags urn.                                          |
| `replace_existing`          |          | boolean      | `false`       | Whether to remove globalTags from entity sent by ingestion source.   |
| `semantics`                 |          | enum         | `OVERWRITE`   | Whether to OVERWRITE or PATCH the entity present on DataHub GMS. |

Let’s suppose we’d like to add a set of dataset tags. To do so, we can use the `simple_add_dataset_tags` transformer that’s included in the ingestion framework.

The config, which we’d append to our ingestion recipe YAML, would look like this:

  ```yaml
  transformers:
    - type: "simple_add_dataset_tags"
      config:
        tag_urns:
          - "urn:li:tag:NeedsDocumentation"
          - "urn:li:tag:Legacy"
  ```

`simple_add_dataset_tags` can be configured in below different way 

- Add tags, however replace existing tags sent by ingestion source
    ```yaml
    transformers:
      - type: "simple_add_dataset_tags"
        config:
          replace_existing: true  # false is default behaviour
          tag_urns:
            - "urn:li:tag:NeedsDocumentation"
            - "urn:li:tag:Legacy"
    ```
- Add tags, however overwrite the tags available for the dataset on DataHub GMS
    ```yaml
    transformers:
      - type: "simple_add_dataset_tags"
        config:
          semantics: OVERWRITE  # OVERWRITE is default behaviour 
          tag_urns:
            - "urn:li:tag:NeedsDocumentation"
            - "urn:li:tag:Legacy"
    ```
- Add tags, however keep the tags available for the dataset on DataHub GMS
    ```yaml
    transformers:
      - type: "simple_add_dataset_tags"
        config:
          semantics: PATCH
          tag_urns:
            - "urn:li:tag:NeedsDocumentation"
            - "urn:li:tag:Legacy"
    ```
## Pattern Add Dataset globalTags
### Config Details
| Field                       | Required | Type                 | Default     | Description                                                                           |
|-----------------------------|----------|----------------------|-------------|---------------------------------------------------------------------------------------|
| `tag_pattern`               | ✅        | map[regx, list[urn]] |             | Entity urn with regular expression and list of tags urn apply to matching entity urn. |
| `replace_existing`          |          | boolean              | `false`     | Whether to remove globalTags from entity sent by ingestion source.                        |
| `semantics`                 |          | enum                 | `OVERWRITE` | Whether to OVERWRITE or PATCH the entity present on DataHub GMS.                      |

Let’s suppose we’d like to append a series of tags to specific datasets. To do so, we can use the `pattern_add_dataset_tags` module that’s included in the ingestion framework.  This will match the regex pattern to `urn` of the dataset and assign the respective tags urns given in the array.

The config, which we’d append to our ingestion recipe YAML, would look like this:

  ```yaml
  transformers:
    - type: "pattern_add_dataset_tags"
      config:
        tag_pattern:
          rules:
            ".*example1.*": ["urn:li:tag:NeedsDocumentation", "urn:li:tag:Legacy"]
            ".*example2.*": ["urn:li:tag:NeedsDocumentation"]
  ```

`pattern_add_dataset_tags` can be configured in below different way 

- Add tags, however replace existing tags sent by ingestion source
    ```yaml
    transformers:
      - type: "pattern_add_dataset_tags"
        config:
          replace_existing: true  # false is default behaviour
          tag_pattern:
            rules:
              ".*example1.*": ["urn:li:tag:NeedsDocumentation", "urn:li:tag:Legacy"]
              ".*example2.*": ["urn:li:tag:NeedsDocumentation"]
    ```
- Add tags, however overwrite the tags available for the dataset on DataHub GMS
    ```yaml
    transformers:
      - type: "pattern_add_dataset_tags"
        config:
          semantics: OVERWRITE  # OVERWRITE is default behaviour 
          tag_pattern:
            rules:
              ".*example1.*": ["urn:li:tag:NeedsDocumentation", "urn:li:tag:Legacy"]
              ".*example2.*": ["urn:li:tag:NeedsDocumentation"]
    ```
- Add tags, however keep the tags available for the dataset on DataHub GMS
    ```yaml
    transformers:
      - type: "pattern_add_dataset_tags"
        config:
          semantics: PATCH
          tag_pattern:
            rules:
              ".*example1.*": ["urn:li:tag:NeedsDocumentation", "urn:li:tag:Legacy"]
              ".*example2.*": ["urn:li:tag:NeedsDocumentation"]
    ```
## Add Dataset globalTags
### Config Details
| Field                       | Required | Type                                       | Default       | Description                                                                |
|-----------------------------|----------|--------------------------------------------|---------------|----------------------------------------------------------------------------|
| `get_tags_to_add`           | ✅        | callable[[str], list[TagAssociationClass]] |               | A function which takes entity urn as input and return TagAssociationClass. |
| `replace_existing`          |          | boolean                                    | `false`       | Whether to remove globalTags from entity sent by ingestion source.             |
| `semantics`                 |          | enum                                       | `OVERWRITE`   | Whether to OVERWRITE or PATCH the entity present on DataHub GMS.           |

If you'd like to add more complex logic for assigning tags, you can use the more generic add_dataset_tags transformer, which calls a user-provided function to determine the tags for each dataset.

```yaml
transformers:
  - type: "add_dataset_tags"
    config:
      get_tags_to_add: "<your_module>.<your_function>"
```

Then define your function to return a list of TagAssociationClass tags, for example:

```python
import logging

import datahub.emitter.mce_builder as builder
from datahub.metadata.schema_classes import (
    TagAssociationClass
)

def custom_tags(entity_urn: str) -> List[TagAssociationClass]:
    """Compute the tags to associate to a given dataset."""

    tag_strings = []

    ### Add custom logic here
    tag_strings.append('custom1')
    tag_strings.append('custom2')
    
    tag_strings = [builder.make_tag_urn(tag=n) for n in tag_strings]
    tags = [TagAssociationClass(tag=tag) for tag in tag_strings]
    
    logging.info(f"Tagging dataset {entity_urn} with {tag_strings}.")
    return tags
```
Finally, you can install and use your custom transformer as [shown here](#installing-the-package).

`add_dataset_tags` can be configured in below different way 

- Add tags, however replace existing tags sent by ingestion source
    ```yaml
    transformers:
      - type: "add_dataset_tags"
        config:
          replace_existing: true  # false is default behaviour
          get_tags_to_add: "<your_module>.<your_function>"
    ```
- Add tags, however overwrite the tags available for the dataset on DataHub GMS
    ```yaml
    transformers:
      - type: "add_dataset_tags"
        config:
          semantics: OVERWRITE  # OVERWRITE is default behaviour 
          get_tags_to_add: "<your_module>.<your_function>"
    ```
- Add tags, however keep the tags available for the dataset on DataHub GMS
    ```yaml
    transformers:
      - type: "add_dataset_tags"
        config:
          semantics: PATCH
          get_tags_to_add: "<your_module>.<your_function>"
    ```
## Set Dataset browsePath
### Config Details
| Field                       | Required | Type         | Default      | Description                                                      |
|-----------------------------|----------|--------------|--------------|------------------------------------------------------------------|
| `path_templates`            | ✅        | list[string] |              | List of path templates.                                          |
| `replace_existing`          |          | boolean      | `false`      | Whether to remove browsePath from entity sent by ingestion source.   |
| `semantics`                 |          | enum         | `OVERWRITE`  | Whether to OVERWRITE or PATCH the entity present on DataHub GMS. |

If you would like to add to browse paths of dataset can use this transformer. There are 3 optional variables that you can use to get information from the dataset `urn`:
- ENV: env passed (default: prod)
- PLATFORM: `mysql`, `postgres` or different platform supported by datahub
- DATASET_PARTS: slash separated parts of dataset name. e.g. `database_name/schema_name/[table_name]` for postgres

e.g. this can be used to create browse paths like `/prod/postgres/superset/public/logs` for table `superset.public.logs` in a `postgres` database
```yaml
transformers:
  - type: "set_dataset_browse_path"
    config:
      path_templates:
        - /ENV/PLATFORM/DATASET_PARTS 
```

If you don't want the environment but wanted to add something static in the browse path like the database instance name you can use this.
```yaml
transformers:
  - type: "set_dataset_browse_path"
    config:
      path_templates:
        - /PLATFORM/marketing_db/DATASET_PARTS 
```
It will create browse path like `/mysql/marketing_db/sales/orders` for a table `sales.orders` in `mysql` database instance.

You can use this to add multiple browse paths. Different people might know the same data assets by different names.
```yaml
transformers:
  - type: "set_dataset_browse_path"
    config:
      path_templates:
        - /PLATFORM/marketing_db/DATASET_PARTS
        - /data_warehouse/DATASET_PARTS
```
This will add 2 browse paths like `/mysql/marketing_db/sales/orders` and `/data_warehouse/sales/orders` for a table `sales.orders` in `mysql` database instance.

Default behaviour of the transform is to add new browse paths, you can optionally set `replace_existing: True` so 
the transform becomes a _set_ operation instead of an _append_.
```yaml
transformers:
  - type: "set_dataset_browse_path"
    config:
      replace_existing: True
      path_templates:
        - /ENV/PLATFORM/DATASET_PARTS
```
In this case, the resulting dataset will have only 1 browse path, the one from the transform.

`set_dataset_browse_path` can be configured in below different way 

- Add browsePath, however replace existing browsePath sent by ingestion source
    ```yaml
    transformers:
      - type: "set_dataset_browse_path"
        config:
          replace_existing: true  # false is default behaviour
          path_templates:
            - /PLATFORM/marketing_db/DATASET_PARTS 
    ```
- Add browsePath, however overwrite the browsePath available for the dataset on DataHub GMS
    ```yaml
    transformers:
      - type: "set_dataset_browse_path"
        config:
          semantics: OVERWRITE  # OVERWRITE is default behaviour 
          path_templates:
            - /PLATFORM/marketing_db/DATASET_PARTS 
    ```
- Add browsePath, however keep the browsePath available for the dataset on DataHub GMS
    ```yaml
    transformers:
      - type: "set_dataset_browse_path"
        config:
          semantics: PATCH
          path_templates:
            - /PLATFORM/marketing_db/DATASET_PARTS 
    ```

## Simple Add Dataset glossaryTerms
### Config Details
| Field                       | Required | Type         | Default       | Description                                                      |
|-----------------------------|----------|--------------|---------------|------------------------------------------------------------------|
| `term_urns`                  | ✅        | list[string] |               | List of glossaryTerms urn.                                          |
| `replace_existing`          |          | boolean      | `false`       | Whether to remove glossaryTerms from entity sent by ingestion source.   |
| `semantics`                 |          | enum         | `OVERWRITE`   | Whether to OVERWRITE or PATCH the entity present on DataHub GMS. |

We can use a similar convention to associate [Glossary Terms](../../../docs/generated/ingestion/sources/business-glossary.md) to datasets. 
We can use the `simple_add_dataset_terms` transformer that’s included in the ingestion framework.

The config, which we’d append to our ingestion recipe YAML, would look like this:

  ```yaml
  transformers:
    - type: "simple_add_dataset_terms"
      config:
        term_urns:
          - "urn:li:glossaryTerm:Email"
          - "urn:li:glossaryTerm:Address"
  ```

`simple_add_dataset_terms` can be configured in below different way 

- Add terms, however replace existing terms sent by ingestion source
    ```yaml
    transformers:
      - type: "simple_add_dataset_terms"
        config:
          replace_existing: true  # false is default behaviour
          term_urns:
            - "urn:li:glossaryTerm:Email"
            - "urn:li:glossaryTerm:Address"
    ```
- Add terms, however overwrite the terms available for the dataset on DataHub GMS
    ```yaml
    transformers:
      - type: "simple_add_dataset_terms"
        config:
          semantics: OVERWRITE  # OVERWRITE is default behaviour 
          term_urns:
            - "urn:li:glossaryTerm:Email"
            - "urn:li:glossaryTerm:Address"
    ```
- Add terms, however keep the terms available for the dataset on DataHub GMS
    ```yaml
    transformers:
      - type: "simple_add_dataset_terms"
        config:
          semantics: PATCH
          term_urns:
            - "urn:li:glossaryTerm:Email"
            - "urn:li:glossaryTerm:Address"
    ```

## Pattern Add Dataset glossaryTerms
### Config Details
| Field                       | Required | Type                 | Default      | Description                                                                                     |
|-----------------------------|--------|----------------------|--------------|-------------------------------------------------------------------------------------------------|
| `term_pattern`              | ✅      | map[regx, list[urn]] |              |  entity urn with regular expression and list of glossaryTerms urn apply to matching entity urn. |
| `replace_existing`          |        | boolean              | `false`      | Whether to remove glossaryTerms from entity sent by ingestion source.                                  |
| `semantics`                 |        | enum                 | `OVERWRITE`  | Whether to OVERWRITE or PATCH the entity present on DataHub GMS.                                |

We can add glossary terms to datasets based on a regex filter.

  ```yaml
  transformers:
    - type: "pattern_add_dataset_terms"
      config:
        term_pattern:
          rules:
            ".*example1.*": ["urn:li:glossaryTerm:Email", "urn:li:glossaryTerm:Address"]
            ".*example2.*": ["urn:li:glossaryTerm:PostalCode"]
  ```

`pattern_add_dataset_terms` can be configured in below different way 

- Add terms, however replace existing terms sent by ingestion source
    ```yaml
    transformers:
      - type: "pattern_add_dataset_terms"
        config:
          replace_existing: true  # false is default behaviour
          term_pattern:
            rules:
              ".*example1.*": ["urn:li:glossaryTerm:Email", "urn:li:glossaryTerm:Address"]
              ".*example2.*": ["urn:li:glossaryTerm:PostalCode"]

    ```
- Add terms, however overwrite the terms available for the dataset on DataHub GMS
    ```yaml
    transformers:
      - type: "pattern_add_dataset_terms"
        config:
          semantics: OVERWRITE  # OVERWRITE is default behaviour 
          term_pattern:
            rules:
              ".*example1.*": ["urn:li:glossaryTerm:Email", "urn:li:glossaryTerm:Address"]
              ".*example2.*": ["urn:li:glossaryTerm:PostalCode"]
    ```
- Add terms, however keep the terms available for the dataset on DataHub GMS
    ```yaml
    transformers:
      - type: "pattern_add_dataset_terms"
        config:
          semantics: PATCH
          term_pattern:
            rules:
              ".*example1.*": ["urn:li:glossaryTerm:Email", "urn:li:glossaryTerm:Address"]
              ".*example2.*": ["urn:li:glossaryTerm:PostalCode"]
    ```

## Tags to Term Mapping
### Config Details

| Field         | Required | Type               | Default     | Description                                                                                           |
|---------------|----------|--------------------|-------------|-------------------------------------------------------------------------------------------------------|
| `tags`        | ✅       | List[str]          |             | List of tag names based on which terms will be created and associated with the dataset.               |
| `semantics`   |          | enum               | "OVERWRITE" | Determines whether to OVERWRITE or PATCH the terms associated with the dataset on DataHub GMS.        |

<br/>

The `tags_to_term` transformer is designed to map specific tags to glossary terms within DataHub. It takes a configuration of tags that should be translated into corresponding glossary terms. This transformer can apply these mappings to any tags found either at the column level of a dataset or at the dataset top level.

When specifying tags in the configuration, use the tag's simple name rather than the full tag URN.

For example, instead of using the tag URN `urn:li:tag:snowflakedb.snowflakeschema.tag_name:tag_value`, you should specify just the tag name `tag_name` in the mapping configuration.

```yaml
transformers:
  - type: "tags_to_term"
    config:
      semantics: OVERWRITE  # OVERWRITE is the default behavior
      tags:
        - "tag_name"
```

The `tags_to_term` transformer can be configured in the following ways:

- Add terms based on tags, however overwrite the terms available for the dataset on DataHub GMS
```yaml
    transformers:
      - type: "tags_to_term"
        config:
          semantics: OVERWRITE  # OVERWRITE is default behaviour
          tags:
            - "example1"
            - "example2"
            - "example3"
  ```
- Add terms based on tags, however keep the terms available for the dataset on DataHub GMS
```yaml
    transformers:
      - type: "tags_to_term"
        config:
          semantics: PATCH
          tags:
            - "example1"
            - "example2"
            - "example3"
  ```

## Pattern Add Dataset Schema Field glossaryTerms
### Config Details
| Field                       | Required | Type                 | Default     | Description                                                                                    |
|-----------------------------|---------|----------------------|-------------|------------------------------------------------------------------------------------------------|
| `term_pattern`              | ✅       | map[regx, list[urn]] |             | entity urn with regular expression and list of glossaryTerms urn apply to matching entity urn. |
| `replace_existing`          |         | boolean              | `false`     | Whether to remove glossaryTerms from entity sent by ingestion source.                                 |
| `semantics`                 |         | enum                 | `OVERWRITE` | Whether to OVERWRITE or PATCH the entity present on DataHub GMS.                               |

We can add glossary terms to schema fields based on a regex filter.

Note that only terms from the first matching pattern will be applied.

  ```yaml
  transformers:
    - type: "pattern_add_dataset_schema_terms"
      config:
        term_pattern:
          rules:
            ".*email.*": ["urn:li:glossaryTerm:Email"]
            ".*name.*": ["urn:li:glossaryTerm:Name"]
  ```

`pattern_add_dataset_schema_terms` can be configured in below different way 

- Add terms, however replace existing terms sent by ingestion source
    ```yaml
    transformers:
      - type: "pattern_add_dataset_schema_terms"
        config:
          replace_existing: true  # false is default behaviour
          term_pattern:
            rules:
              ".*email.*": ["urn:li:glossaryTerm:Email"]
              ".*name.*": ["urn:li:glossaryTerm:Name"]
    ```
- Add terms, however overwrite the terms available for the dataset on DataHub GMS
    ```yaml
    transformers:
      - type: "pattern_add_dataset_schema_terms"
        config:
          semantics: OVERWRITE  # OVERWRITE is default behaviour 
          term_pattern:
            rules:
              ".*email.*": ["urn:li:glossaryTerm:Email"]
              ".*name.*": ["urn:li:glossaryTerm:Name"]
    ```
- Add terms, however keep the terms available for the dataset on DataHub GMS
    ```yaml
    transformers:
      - type: "pattern_add_dataset_schema_terms"
        config:
          semantics: PATCH
          term_pattern:
            rules:
              ".*email.*": ["urn:li:glossaryTerm:Email"]
              ".*name.*": ["urn:li:glossaryTerm:Name"]
    ```
## Pattern Add Dataset Schema Field globalTags
### Config Details
| Field                       | Required | Type                 | Default     | Description                                                                           |
|-----------------------------|----------|----------------------|-------------|---------------------------------------------------------------------------------------|
| `tag_pattern`               | ✅        | map[regx, list[urn]] |             | entity urn with regular expression and list of tags urn apply to matching entity urn. |
| `replace_existing`          |          | boolean              | `false`     | Whether to remove globalTags from entity sent by ingestion source.                        |
| `semantics`                 |          | enum                 | `OVERWRITE` | Whether to OVERWRITE or PATCH the entity present on DataHub GMS.                      |


We can also append a series of tags to specific schema fields. To do so, we can use the `pattern_add_dataset_schema_tags` transformer. This will match the regex pattern to each schema field path and assign the respective tags urns given in the array.

Note that the tags from the first matching pattern will be applied, not all matching patterns.

The config would look like this:

  ```yaml
  transformers:
    - type: "pattern_add_dataset_schema_tags"
      config:
        tag_pattern:
          rules:
            ".*email.*": ["urn:li:tag:Email"]
            ".*name.*": ["urn:li:tag:Name"]
  ```

`pattern_add_dataset_schema_tags` can be configured in below different way 

- Add tags, however replace existing tag sent by ingestion source
    ```yaml
    transformers:
      - type: "pattern_add_dataset_schema_tags"
        config:
          replace_existing: true  # false is default behaviour
          tag_pattern:
            rules:
              ".*example1.*": ["urn:li:tag:NeedsDocumentation", "urn:li:tag:Legacy"]
              ".*example2.*": ["urn:li:tag:NeedsDocumentation"]
    ```
- Add tags, however overwrite the tags available for the dataset on DataHub GMS
    ```yaml
    transformers:
      - type: "pattern_add_dataset_schema_tags"
        config:
          semantics: OVERWRITE  # OVERWRITE is default behaviour 
          tag_pattern:
            rules:
              ".*example1.*": ["urn:li:tag:NeedsDocumentation", "urn:li:tag:Legacy"]
              ".*example2.*": ["urn:li:tag:NeedsDocumentation"]
    ```
- Add tags, however keep the tags available for the dataset on DataHub GMS
    ```yaml
    transformers:
      - type: "pattern_add_dataset_schema_tags"
        config:
          semantics: PATCH
          tag_pattern:
            rules:
              ".*example1.*": ["urn:li:tag:NeedsDocumentation", "urn:li:tag:Legacy"]
              ".*example2.*": ["urn:li:tag:NeedsDocumentation"]
    ```
## Simple Add Dataset datasetProperties
### Config Details
| Field              | Required | Type           | Default     | Description                                                      |
|--------------------|---------|----------------|-------------|------------------------------------------------------------------|
| `properties`       | ✅       | dict[str, str] |             | Map of key value pair.                                           |
| `replace_existing` |         | boolean        | `false`     | Whether to remove datasetProperties from entity sent by ingestion source.   |
| `semantics`        |         | enum           | `OVERWRITE` | Whether to OVERWRITE or PATCH the entity present on DataHub GMS. |

`simple_add_dataset_properties` transformer assigns the properties to dataset entity from the configuration.
`properties` field is a dictionary of string values. Note in case of any key collision, the value in the config will
overwrite the previous value.

  ```yaml
  transformers:
    - type: "simple_add_dataset_properties"
      config:
        properties:
          prop1: value1
          prop2: value2
  ```

`simple_add_dataset_properties` can be configured in below different way 

- Add dataset-properties, however replace existing dataset-properties sent by ingestion source
    ```yaml
    transformers:
      - type: "simple_add_dataset_properties"
        config:
          replace_existing: true  # false is default behaviour
          properties:
            prop1: value1
            prop2: value2
    ```
- Add dataset-properties, however overwrite the dataset-properties available for the dataset on DataHub GMS
    ```yaml
    transformers:
      - type: "simple_add_dataset_properties"
        config:
          semantics: OVERWRITE  # OVERWRITE is default behaviour 
          properties:
            prop1: value1
            prop2: value2
    ```
- Add dataset-properties, however keep the dataset-properties available for the dataset on DataHub GMS
    ```yaml
    transformers:
      - type: "simple_add_dataset_properties"
        config:
          semantics: PATCH
          properties:
            prop1: value1
            prop2: value2
    ```

## Add Dataset datasetProperties
### Config Details
| Field                          | Required | Type                                       | Default     | Description                                                      |
|--------------------------------|----------|--------------------------------------------|-------------|------------------------------------------------------------------|
| `add_properties_resolver_class`| ✅        | Type[AddDatasetPropertiesResolverBase] |             | A class extends from `AddDatasetPropertiesResolverBase`          |
| `replace_existing`             |          | boolean                                    | `false`     | Whether to remove datasetProperties from entity sent by ingestion source.   |
| `semantics`                    |          | enum                                       | `OVERWRITE` | Whether to OVERWRITE or PATCH the entity present on DataHub GMS. |

If you'd like to add more complex logic for assigning properties, you can use the `add_dataset_properties` transformer, which calls a user-provided class (that extends from `AddDatasetPropertiesResolverBase` class) to determine the properties for each dataset.

The config, which we’d append to our ingestion recipe YAML, would look like this:

  ```yaml
  transformers:
    - type: "add_dataset_properties"
      config:
        add_properties_resolver_class: "<your_module>.<your_class>"
  ```

Then define your class to return a list of custom properties, for example:
  
  ```python
  import logging
  from typing import Dict
  from datahub.ingestion.transformer.add_dataset_properties import AddDatasetPropertiesResolverBase

  class MyPropertiesResolver(AddDatasetPropertiesResolverBase):
      def get_properties_to_add(self, entity_urn: str) -> Dict[str, str]:
          ### Add custom logic here        
          properties= {'my_custom_property': 'property value'}
          logging.info(f"Adding properties: {properties} to dataset: {entity_urn}.")
          return properties
  ```

`add_dataset_properties` can be configured in below different way 

- Add dataset-properties, however replace existing dataset-properties sent by ingestion source
    ```yaml
    transformers:
      - type: "add_dataset_properties"
        config:
          replace_existing: true  # false is default behaviour
          add_properties_resolver_class: "<your_module>.<your_class>"

    ```
- Add dataset-properties, however overwrite the dataset-properties available for the dataset on DataHub GMS
    ```yaml
    transformers:
      - type: "add_dataset_properties"
        config:
          semantics: OVERWRITE  # OVERWRITE is default behaviour 
          add_properties_resolver_class: "<your_module>.<your_class>"

    ```
- Add dataset-properties, however keep the dataset-properties available for the dataset on DataHub GMS
    ```yaml
    transformers:
      - type: "add_dataset_properties"
        config:
          semantics: PATCH
          add_properties_resolver_class: "<your_module>.<your_class>"
    ```

## Replace ExternalUrl Dataset
### Config Details
| Field                       | Required | Type    | Default       | Description                                 |
|-----------------------------|----------|---------|---------------|---------------------------------------------|
| `input_pattern`                 | ✅         | string    |    | String or pattern to replace |
| `replacement`                 | ✅         | string    |    | Replacement string |


Matches the full/partial string in the externalUrl of the dataset properties and replace that with the replacement string

```yaml
transformers:
  - type: "replace_external_url"
    config:
      input_pattern: '\b\w*hub\b'
      replacement: "sub"
```

## Replace ExternalUrl Container
### Config Details
| Field                       | Required | Type    | Default       | Description                                 |
|-----------------------------|----------|---------|---------------|---------------------------------------------|
| `input_pattern`                 | ✅         | string    |    | String or pattern to replace |
| `replacement`                 | ✅         | string    |    | Replacement string |


Matches the full/partial string in the externalUrl of the container properties and replace that with the replacement string

```yaml
transformers:
  - type: "replace_external_url_container"
    config:
      input_pattern: '\b\w*hub\b'
      replacement: "sub"
```

## Clean User URN in DatasetUsageStatistics Aspect
### Config Details
| Field                       | Required | Type    | Default       | Description                                 |
|-----------------------------|----------|---------|---------------|---------------------------------------------|
| `pattern_for_cleanup`                 | ✅         | list[string]    |    | List of suffix/prefix to remove from the Owner URN(s) |


Matches against a User URN in DatasetUsageStatistics aspect and remove the matching part from it
```yaml
transformers:
  - type: "pattern_cleanup_dataset_usage_user"
    config:
      pattern_for_cleanup:
        - "ABCDEF"
        - (?<=_)(\w+)
```


## Simple Add Dataset domains 
### Config Details
| Field              | Required | Type                   | Default       | Description                                                      |
|--------------------|----------|------------------------|---------------|------------------------------------------------------------------|
| `domains`          | ✅        | list[union[urn, str]]  |               | List of simple domain name or domain urns.                       |
| `replace_existing` |          | boolean                | `false`       | Whether to remove domains from entity sent by ingestion source.   |
| `semantics`        |          | enum                   | `OVERWRITE`   | Whether to OVERWRITE or PATCH the entity present on DataHub GMS. |

For transformer behaviour on `replace_existing` and `semantics`, please refer section [Relationship Between replace_existing And semantics](#relationship-between-replace_existing-and-semantics).

<br/>

let’s suppose we’d like to add a series of domain to dataset, in this case you can use `simple_add_dataset_domain` transformer.

The config, which we’d append to our ingestion recipe YAML, would look like this:

Here we can set domains to either urn (i.e. urn:li:domain:engineering) or simple domain name (i.e. engineering) in both of the cases domain should be provisioned on DataHub GMS
```yaml
transformers:
  - type: "simple_add_dataset_domain"
    config:
      semantics: OVERWRITE
      domains:
        - urn:li:domain:engineering
```


`simple_add_dataset_domain` can be configured in below different way 

- Add domains, however replace existing domains sent by ingestion source
    ```yaml
    transformers:
      - type: "simple_add_dataset_domain"
        config:
          replace_existing: true  # false is default behaviour
          domains:
            - "urn:li:domain:engineering"
            - "urn:li:domain:hr"
   ```
- Add domains, however overwrite the domains available for the dataset on DataHub GMS
    ```yaml
    transformers:
      - type: "simple_add_dataset_domain"
        config:
          semantics: OVERWRITE  # OVERWRITE is default behaviour 
          domains:
            - "urn:li:domain:engineering"
            - "urn:li:domain:hr"
    ```
- Add domains, however keep the domains available for the dataset on DataHub GMS
    ```yaml
    transformers:
      - type: "simple_add_dataset_domain"
        config:
          semantics: PATCH
          domains:
            - "urn:li:domain:engineering"
            - "urn:li:domain:hr"
    ```

## Pattern Add Dataset domains 
### Config Details
| Field                      | Required  | Type                            | Default         | Description                                                                                                                |
|----------------------------|-----------|---------------------------------|-----------------|----------------------------------------------------------------------------------------------------------------------------|
| `domain_pattern`           | ✅         | map[regx, list[union[urn, str]] |                 | dataset urn with regular expression and list of simple domain name or domain urn need to be apply on matching dataset urn. |
| `replace_existing`         |           | boolean                         | `false`         | Whether to remove domains from entity sent by ingestion source.                                                             |
| `semantics`                |           | enum                            | `OVERWRITE`     | Whether to OVERWRITE or PATCH the entity present on DataHub GMS.                                                           |
| `is_container`     |          | bool    | `false`    | Whether to also consider a container or not. If true, then domains will be attached to both the dataset and its container. |

Let’s suppose we’d like to append a series of domain to specific datasets. To do so, we can use the pattern_add_dataset_domain transformer that’s included in the ingestion framework. 
This will match the regex pattern to urn of the dataset and assign the respective domain urns given in the array.

If the is_container field is set to true, the module will not only attach the domains to the matching datasets but will also find and attach containers associated with those datasets. This means that both the datasets and their containers will be associated with the specified owners.

The config, which we’d append to our ingestion recipe YAML, would look like this:
Here we can set domain list to either urn (i.e. urn:li:domain:hr) or simple domain name (i.e. hr) 
in both of the cases domain should be provisioned on DataHub GMS

  ```yaml
  transformers:
    - type: "pattern_add_dataset_domain"
      config:
        semantics: OVERWRITE
        domain_pattern:
          rules:
            'urn:li:dataset:\(urn:li:dataPlatform:postgres,postgres\.public\.n.*': ["hr"]
            'urn:li:dataset:\(urn:li:dataPlatform:postgres,postgres\.public\.t.*': ["urn:li:domain:finance"]
  ```

`pattern_add_dataset_domain` can be configured in below different way 

- Add domains, however replace existing domains sent by ingestion source
    ```yaml
    transformers:
      - type: "pattern_add_dataset_domain"
        config:
          replace_existing: true  # false is default behaviour
          domain_pattern:
            rules:
              'urn:li:dataset:\(urn:li:dataPlatform:postgres,postgres\.public\.n.*': ["hr"]
              'urn:li:dataset:\(urn:li:dataPlatform:postgres,postgres\.public\.t.*': ["urn:li:domain:finance"] 
    ```
- Add domains, however overwrite the domains available for the dataset on DataHub GMS
    ```yaml
    transformers:
      - type: "pattern_add_dataset_domain"
        config:
          semantics: OVERWRITE  # OVERWRITE is default behaviour 
          domain_pattern:
            rules:
              'urn:li:dataset:\(urn:li:dataPlatform:postgres,postgres\.public\.n.*': ["hr"]
              'urn:li:dataset:\(urn:li:dataPlatform:postgres,postgres\.public\.t.*': ["urn:li:domain:finance"] 
    ```
- Add domains, however keep the domains available for the dataset on DataHub GMS
    ```yaml
    transformers:
      - type: "pattern_add_dataset_domain"
        config:
          semantics: PATCH
          domain_pattern:
            rules:
              'urn:li:dataset:\(urn:li:dataPlatform:postgres,postgres\.public\.n.*': ["hr"]
              'urn:li:dataset:\(urn:li:dataPlatform:postgres,postgres\.public\.t.*': ["urn:li:domain:finance"] 
    ```

- Add domains to dataset and its containers
  ```yaml
  transformers:
    - type: "pattern_add_dataset_domain"
      config:
        is_container: true
        semantics: PATCH / OVERWRITE # Based on user
        domain_pattern:
          rules:
            'urn:li:dataset:\(urn:li:dataPlatform:postgres,postgres\.public\.n.*': ["hr"]
            'urn:li:dataset:\(urn:li:dataPlatform:postgres,postgres\.public\.t.*': ["urn:li:domain:finance"]
  ```
⚠️ Warning:
When working with two datasets in the same container but with different domains, all domains will be added for that dataset containers.

For example:
```yaml
transformers:
  - type: "pattern_add_dataset_domain"
    config:
      is_container: true
      domain_pattern:
        rules:
          ".*example1.*": ["hr"]
          ".*example2.*": ["urn:li:domain:finance"]
```
If example1 and example2 are in the same container, then both domains hr and finance will be added for respective dataset containers.


## Domain Mapping Based on Tags
### Config Details

| Field           | Required | Type                    | Default     | Description                                                                                             |
|-----------------|----------|-------------------------|-------------|---------------------------------------------------------------------------------------------------------|
| `domain_mapping`| ✅       | Dict[str, str]     |             | Dataset Entity tag as key and domain urn or name as value to map with dataset as asset.           |
| `semantics`     |          | enum                  | "OVERWRITE" | Whether to OVERWRITE or PATCH the entity present on DataHub GMS.|

<br/>

let’s suppose we’d like to add domain to dataset based on tag, in this case you can use `domain_mapping_based_on_tags` transformer.

The config, which we’d append to our ingestion recipe YAML, would look like this:

Here we can set domains to either urn (i.e. urn:li:domain:engineering) or simple domain name (i.e. engineering) in both of the cases domain should be provisioned on DataHub GMS

When specifying tags within the domain mapping, use the tag's simple name rather than the full tag URN.

For example, instead of using the tag URN urn:li:tag:NeedsDocumentation, you should specify just the simple tag name NeedsDocumentation in the domain mapping configuration

```yaml
transformers:
  - type: "domain_mapping_based_on_tags"
    config:
      domain_mapping:
        'NeedsDocumentation': "urn:li:domain:documentation"
```


`domain_mapping_based_on_tags` can be configured in below different way

- Add domains based on tags, however overwrite the domains available for the dataset on DataHub GMS
    ```yaml
    transformers:
      - type: "domain_mapping_based_on_tags"
        config:
          semantics: OVERWRITE  # OVERWRITE is default behaviour
          domain_mapping:
            'example1': "urn:li:domain:engineering"
            'example2': "urn:li:domain:hr"
    ```
- Add domains based on tags, however keep the domains available for the dataset on DataHub GMS
    ```yaml
    transformers:
      - type: "domain_mapping_based_on_tags"
        config:
          semantics: PATCH
          domain_mapping:
            'example1': "urn:li:domain:engineering"
            'example2': "urn:li:domain:hr"
    ```

## Simple Add Dataset dataProduct
### Config Details
| Field                         | Required | Type            | Default       | Description                                                                            |
|-------------------------------|----------|-----------------|---------------|----------------------------------------------------------------------------------------|
| `dataset_to_data_product_urns`| ✅       | Dict[str, str]  |               | Dataset Entity urn as key and dataproduct urn as value to create with dataset as asset.|

Let’s suppose we’d like to add a set of dataproduct with specific datasets as its assets. To do so, we can use the `simple_add_dataset_dataproduct` transformer that’s included in the ingestion framework.

The config, which we’d append to our ingestion recipe YAML, would look like this:

  ```yaml
  transformers:
    - type: "simple_add_dataset_dataproduct"
      config:
        dataset_to_data_product_urns:
          "urn:li:dataset:(urn:li:dataPlatform:bigquery,example1,PROD)": "urn:li:dataProduct:first"
          "urn:li:dataset:(urn:li:dataPlatform:bigquery,example2,PROD)": "urn:li:dataProduct:second"
  ```

## Pattern Add Dataset dataProduct
### Config Details
| Field                                 | Required | Type                 | Default     | Description                                                                                 |
|---------------------------------------|----------|----------------------|-------------|---------------------------------------------------------------------------------------------|
| `dataset_to_data_product_urns_pattern`| ✅       | map[regx, urn]       |             | Dataset Entity urn with regular expression and dataproduct urn apply to matching entity urn.|
| `is_container`      |          | bool    | `false`   | Whether to also consider a container or not. If true, the data product will be attached to both the dataset and its container. |


Let’s suppose we’d like to append a series of data products with specific datasets or their containers as assets. To do so, we can use the pattern_add_dataset_dataproduct module that’s included in the ingestion framework. This module matches a regex pattern to the urn of the dataset and creates a data product entity with the given urn, associating the matched datasets as its assets.

If the is_container field is set to true, the module will not only attach the data product to the matching datasets but will also find and attach the containers associated with those datasets. This means that both the datasets and their containers will be associated with the specified data product.

The config, which we’d append to our ingestion recipe YAML, would look like this:

- Add Product to dataset
  ```yaml
  transformers:
    - type: "pattern_add_dataset_dataproduct"
      config:
        dataset_to_data_product_urns_pattern:
          rules:
            ".*example1.*": "urn:li:dataProduct:first"
            ".*example2.*": "urn:li:dataProduct:second"
  ```
- Add Product to dataset container
  ```yaml
  transformers:
    - type: "pattern_add_dataset_dataproduct"
      config:
        is_container: true
        dataset_to_data_product_urns_pattern:
          rules:
            ".*example1.*": "urn:li:dataProduct:first"
            ".*example2.*": "urn:li:dataProduct:second"
  ```
⚠️ Warning:
When working with two datasets in the same container but with different data products, only one data product can be attached to the container.

For example:
```yaml
transformers:
  - type: "pattern_add_dataset_dataproduct"
    config:
      is_container: true
      dataset_to_data_product_urns_pattern:
        rules:
          ".*example1.*": "urn:li:dataProduct:first"
          ".*example2.*": "urn:li:dataProduct:second"
```
If example1 and example2 are in the same container, only urn:li:dataProduct:first will be added. However, if they are in separate containers, the system works as expected and assigns the correct data product URNs.

## Add Dataset dataProduct
### Config Details
| Field                       | Required | Type                              | Default       | Description                                                                              |
|-----------------------------|----------|-----------------------------------|---------------|------------------------------------------------------------------------------------------|
| `get_data_product_to_add`   | ✅        | callable[[str], Optional[str]]   |               | A function which takes dataset entity urn as input and return dataproduct urn to create. |

If you'd like to add more complex logic for creating dataproducts, you can use the more generic add_dataset_dataproduct transformer, which calls a user-provided function to determine the dataproduct to create with specified datasets as its asset.

```yaml
transformers:
  - type: "add_dataset_dataproduct"
    config:
      get_data_product_to_add: "<your_module>.<your_function>"
```

Then define your function to return a dataproduct entity urn, for example:

```python
import datahub.emitter.mce_builder as builder

def custom_dataproducts(entity_urn: str) -> Optional[str]:
    """Compute the dataproduct urn to a given dataset urn."""

    dataset_to_data_product_map = {
        builder.make_dataset_urn("bigquery", "example1"): "urn:li:dataProduct:first"
    }
    return dataset_to_data_product_map.get(dataset_urn)
```
Finally, you can install and use your custom transformer as [shown here](#installing-the-package).

## Relationship Between replace_existing and semantics
The transformer behaviour mentioned here is in context of `simple_add_dataset_ownership`, however it is applicable for all dataset transformers which are supporting `replace_existing`
and `semantics` configuration attributes, for example `simple_add_dataset_tags` will add or remove tags as per behaviour mentioned in this section.

`replace_existing`  controls whether to remove owners from currently executing ingestion pipeline.

`semantics` controls whether to overwrite or patch owners present on DataHub GMS server. These owners might be added from DataHub Portal.

if `replace_existing` is set to `true` and `semantics` is set to `OVERWRITE` then transformer takes below steps
1. As `replace_existing` is set to `true`, remove the owners from input entity (i.e. dataset)
2. Add owners mentioned in ingestion recipe to input entity  
3. As `semantics` is set to `OVERWRITE` no need to fetch owners present on DataHub GMS server for the input entity
4. Return input entity 

if `replace_existing` is set to `true` and `semantics` is set to `PATCH` then transformer takes below steps
1. `replace_existing` is set to `true`, first remove the owners from input entity (i.e. dataset)
2. Add owners mentioned in ingestion recipe to input entity  
3. As `semantics` is set to `PATCH` fetch owners for the input entity from DataHub GMS Server
4. Add owners fetched from DataHub GMS Server to input entity
5. Return input entity 

if `replace_existing` is set to `false` and `semantics` is set to `OVERWRITE` then transformer takes below steps
1. As `replace_existing` is set to `false`, keep the owners present in input entity as is
2. Add owners mentioned in ingestion recipe to input entity  
3. As `semantics` is set to `OVERWRITE` no need to fetch owners from DataHub GMS Server for the input entity
4. Return input entity 

if `replace_existing` is set to `false` and `semantics` is set to `PATCH` then transformer takes below steps
1. `replace_existing` is set to `false`, keep the owners present in input entity as is
2. Add owners mentioned in ingestion recipe to input entity  
3. As `semantics` is set to `PATCH` fetch owners for the input entity from DataHub GMS Server
4. Add owners fetched from DataHub GMS Server to input entity
5. Return input entity 



## Writing a custom transformer from scratch

In the above couple of examples, we use classes that have already been implemented in the ingestion framework. However, it’s common for more advanced cases to pop up where custom code is required, for instance if you'd like to utilize conditional logic or rewrite properties. In such cases, we can add our own modules and define the arguments it takes as a custom transformer.

As an example, suppose we want to append a set of ownership fields to our metadata that are dependent upon an external source – for instance, an API endpoint or file – rather than a preset list like above. In this case, we can set a JSON file as an argument to our custom config, and our transformer will read this file and append the included ownership elements to all metadata events.

Our JSON file might look like the following:

```json
[
  "urn:li:corpuser:athos",
  "urn:li:corpuser:porthos",
  "urn:li:corpuser:aramis",
  "urn:li:corpGroup:the_three_musketeers"
]
```

### Defining a config

To get started, we’ll initiate an `AddCustomOwnershipConfig` class that inherits from [`datahub.configuration.common.ConfigModel`](../../src/datahub/configuration/common.py). The sole parameter will be an `owners_json` which expects a path to a JSON file containing a list of owner URNs. This will go in a file called `custom_transform_example.py`.

```python
from datahub.configuration.common import ConfigModel

class AddCustomOwnershipConfig(ConfigModel):
    owners_json: str
```

### Defining the transformer

Next, we’ll define the transformer itself, which must inherit from [`datahub.ingestion.api.transform.Transformer`](../../src/datahub/ingestion/api/transform.py). The framework provides a helper class called [`datahub.ingestion.transformer.base_transformer.BaseTransformer`](../../src/datahub/ingestion/transformer/base_transformer.py) that makes it super-simple to write transformers. 
First, let's get all our imports in:

```python
# append these to the start of custom_transform_example.py
import json
from typing import List, Optional

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.add_dataset_ownership import Semantics
from datahub.ingestion.transformer.base_transformer import (
    BaseTransformer,
    SingleAspectTransformer,
)
from datahub.metadata.schema_classes import (
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)

```

Next, let's define the base scaffolding for the class:

```python
# append this to the end of custom_transform_example.py

class AddCustomOwnership(BaseTransformer, SingleAspectTransformer):
    """Transformer that adds owners to datasets according to a callback function."""

    # context param to generate run metadata such as a run ID
    ctx: PipelineContext
    # as defined in the previous block
    config: AddCustomOwnershipConfig

    def __init__(self, config: AddCustomOwnershipConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config

        with open(self.config.owners_json, "r") as f:
            raw_owner_urns = json.load(f)

        self.owners = [
            OwnerClass(owner=owner, type=OwnershipTypeClass.DATAOWNER)
            for owner in raw_owner_urns
        ]
```

A transformer must have two functions: a `create()` function for initialization and a `transform()` function for executing the transformation. Transformers that extend `BaseTransformer` and `SingleAspectTransformer` can avoid having to implement the more complex `transform` function and just implement the `transform_aspect` function.

Let's begin by adding a `create()` method for parsing our configuration dictionary:

```python
# add this as a function of AddCustomOwnership

@classmethod
def create(cls, config_dict: dict, ctx: PipelineContext) -> "AddCustomOwnership":
    config = AddCustomOwnershipConfig.parse_obj(config_dict)
    return cls(config, ctx)
```

Next we need to tell the helper classes which entity types and aspect we are interested in transforming. In this case, we want to only process `dataset` entities and transform the `ownership` aspect.

```python
def entity_types(self) -> List[str]:
    return ["dataset"]

def aspect_name(self) -> str:
    return "ownership"
```

Finally we need to implement the `transform_aspect()` method that does the work of adding our custom ownership classes. This method will be called be the framework with an optional aspect value filled out if the upstream source produced a value for this aspect. The framework takes care of pre-processing both MCE-s and MCP-s so that the `transform_aspect()` function is only called one per entity. Our job is merely to inspect the incoming aspect (or absence) and produce a transformed value for this aspect. Returning `None` from this method will effectively suppress this aspect from being emitted.

```python
# add this as a function of AddCustomOwnership

def transform_aspect(  # type: ignore
    self, entity_urn: str, aspect_name: str, aspect: Optional[OwnershipClass]
) -> Optional[OwnershipClass]:

    owners_to_add = self.owners
    assert aspect is None or isinstance(aspect, OwnershipClass)

    if owners_to_add:
        ownership = (
            aspect
            if aspect
            else OwnershipClass(
                owners=[],
            )
        )
        ownership.owners.extend(owners_to_add)

    return ownership
```

### More Sophistication: Making calls to DataHub during Transformation

In some advanced cases, you might want to check with DataHub before performing a transformation. A good example for this might be retrieving the current set of owners of a dataset before providing the new set of owners during an ingestion process. To allow transformers to always be able to query the graph, the framework provides them access to the graph through the context object `ctx`. Connectivity to the graph is automatically instantiated anytime the pipeline uses a REST sink. In case you are using the Kafka sink, you can additionally provide access to the graph by configuring it in your pipeline. 

Here is an example of a recipe that uses Kafka as the sink, but provides access to the graph by explicitly configuring the `datahub_api`. 

```yaml
source:
  type: mysql
  config: 
     # ..source configs
     
sink: 
  type: datahub-kafka
  config:
     connection:
        bootstrap: localhost:9092
	schema_registry_url: "http://localhost:8081"

datahub_api:
  server: http://localhost:8080
  # standard configs accepted by datahub rest client ... 
```

#### Advanced Use-Case: Patching Owners

With the above capability, we can now build more powerful transformers that can check with the server-side state before issuing changes in metadata. 
e.g. Here is how the AddDatasetOwnership transformer can now support PATCH semantics by ensuring that it never deletes any owners that are stored on the server. 

```python
def transform_one(self, mce: MetadataChangeEventClass) -> MetadataChangeEventClass:
    if not isinstance(mce.proposedSnapshot, DatasetSnapshotClass):
        return mce
    owners_to_add = self.config.get_owners_to_add(mce.proposedSnapshot)
    if owners_to_add:
        ownership = builder.get_or_add_aspect(
            mce,
            OwnershipClass(
                owners=[],
            ),
        )
        ownership.owners.extend(owners_to_add)

        if self.config.semantics == Semantics.PATCH:
            assert self.ctx.graph
            patch_ownership = AddDatasetOwnership.get_ownership_to_set(
                self.ctx.graph, mce.proposedSnapshot.urn, ownership
            )
            builder.set_aspect(
                mce, aspect=patch_ownership, aspect_type=OwnershipClass
            )
    return mce
```

### Installing the package

Now that we've defined the transformer, we need to make it visible to DataHub. The easiest way to do this is to just place it in the same directory as your recipe, in which case the module name is the same as the file – in this case, `custom_transform_example`.

<details>
  <summary>Advanced: Installing as a package and enable discoverability</summary>
Alternatively, create a `setup.py` in the same directory as our transform script to make it visible globally. After installing this package (e.g. with `python setup.py` or `pip install -e .`), our module will be installed and importable as `custom_transform_example`.

```python
from setuptools import find_packages, setup

setup(
    name="custom_transform_example",
    version="1.0",
    packages=find_packages(),
    # if you don't already have DataHub installed, add it under install_requires
    # install_requires=["acryl-datahub"],
    entry_points={
        "datahub.ingestion.transformer.plugins": [
            "custom_transform_example_alias = custom_transform_example:AddCustomOwnership",
        ],
    },
)
```

Additionally, declare the transformer under the `entry_points` variable of the setup script. This enables the transformer to be
listed when running `datahub check plugins`, and sets up the transformer's shortened alias for use in recipes.

</details>

### Running the transform

```yaml
transformers:
  - type: "custom_transform_example_alias"
    config:
      owners_json: "<path_to_owners_json>" # the JSON file mentioned at the start
```

After running `datahub ingest -c <path_to_recipe>`, our MCEs will now have the following owners appended:

```json
"owners": [
    {
        "owner": "urn:li:corpuser:athos",
        "type": "DATAOWNER",
        "source": null
    },
    {
        "owner": "urn:li:corpuser:porthos",
        "type": "DATAOWNER",
        "source": null
    },
    {
        "owner": "urn:li:corpuser:aramis",
        "type": "DATAOWNER",
        "source": null
    },
    {
        "owner": "urn:li:corpGroup:the_three_musketeers",
        "type": "DATAOWNER",
        "source": null
    },
	// ...and any additional owners
],
```

All the files for this tutorial may be found [here](../../examples/transforms/).
