# Transformers

## What’s a transformer?

Oftentimes we want to modify metadata before it reaches the ingestion sink – for instance, we might want to add custom tags, ownership, properties, or patch some fields. A transformer allows us to do exactly these things.

Moreover, a transformer allows one to have fine-grained control over the metadata that’s ingested without having to modify the ingestion framework's code yourself. Instead, you can write your own module that can transform metadata events however you like. To include a transformer into a recipe, all that's needed is the name of the transformer as well as any configuration that the transformer needs.

## Provided transformers

Aside from the option of writing your own transformer (see below), we provide some simple transformers for the use cases of adding: dataset tags, dataset glossary terms, dataset properties and ownership information.

### Adding a set of tags

Let’s suppose we’d like to add a set of dataset tags. To do so, we can use the `simple_add_dataset_tags` module that’s included in the ingestion framework.

The config, which we’d append to our ingestion recipe YAML, would look like this:

```yaml
transformers:
  - type: "simple_add_dataset_tags"
    config:
      tag_urns:
        - "urn:li:tag:NeedsDocumentation"
        - "urn:li:tag:Legacy"
```

### Adding tags by dataset urn pattern

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

### Adding tags by schema field pattern

We can also append a series of tags to specific schema fields. To do so, we can use the `pattern_add_dataset_schema_tags` module. This will match the regex pattern to each schema field path and assign the respective tags urns given in the array.

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

### Add your own custom Transformer

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
    DatasetSnapshotClass,
    TagAssociationClass
)

def custom_tags(current: DatasetSnapshotClass) -> List[TagAssociationClass]:
    """ Returns tags to associate to a dataset depending on custom logic

    This function receives a DatasetSnapshotClass, performs custom logic and returns
    a list of TagAssociationClass-wrapped tags.

    Args:
        current (DatasetSnapshotClass): Single DatasetSnapshotClass object

    Returns:
        List of TagAssociationClass objects.
    """

    tag_strings = []

    ### Add custom logic here
    tag_strings.append('custom1')
    tag_strings.append('custom2')
    
    tag_strings = [builder.make_tag_urn(tag=n) for n in tag_strings]
    tags = [TagAssociationClass(tag=tag) for tag in tag_strings]
    
    logging.info(f"Tagging dataset {current.urn} with {tag_strings}.")
    return tags
```
Finally, you can install and use your custom transformer as [shown here](#installing-the-package).

### Adding a set of glossary terms

We can use a similar convention to associate [Glossary Terms](../docs/generated/ingestion/sources/business-glossary.md) to datasets. We can use the `simple_add_dataset_terms` module that’s included in the ingestion framework.

The config, which we’d append to our ingestion recipe YAML, would look like this:

```yaml
transformers:
  - type: "simple_add_dataset_terms"
    config:
      term_urns:
        - "urn:li:glossaryTerm:Email"
        - "urn:li:glossaryTerm:Address"
```

### Adding glossary terms by dataset urn pattern

Similar to the above example with tags, we can add glossary terms to datasets based on a regex filter.

```yaml
transformers:
  - type: "pattern_add_dataset_terms"
    config:
      term_pattern:
        rules:
          ".*example1.*": ["urn:li:glossaryTerm:Email", "urn:li:glossaryTerm:Address"]
          ".*example2.*": ["urn:li:glossaryTerm:PostalCode"]
```

### Adding glossary terms by schema field pattern

Similar to the above example with tags applied to schema fields, we can add glossary terms to schema fields based on a regex filter.
Again, note that only terms from the first matching pattern will be applied.

```yaml
transformers:
  - type: "pattern_add_dataset_schema_terms"
    config:
      term_pattern:
        rules:
          ".*email.*": ["urn:li:glossaryTerm:Email"]
          ".*name.*": ["urn:li:glossaryTerm:Name"]
```

### Change owners

If we wanted to clear existing owners sent by ingestion source we can use the `simple_remove_dataset_ownership` module which removes all owners sent by the ingestion source.

```yaml
transformers:
  - type: "simple_remove_dataset_ownership"
    config: {}
```

The main use case of `simple_remove_dataset_ownership` is to remove incorrect owners present in the source. You can use it along with the next `simple_add_dataset_ownership` to remove wrong owners and add the correct ones.

Let’s suppose we’d like to append a series of users who we know to own a dataset but aren't detected during normal ingestion. To do so, we can use the `simple_add_dataset_ownership` module that’s included in the ingestion framework.

The config, which we’d append to our ingestion recipe YAML, would look like this:

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

Note `ownership_type` is an optional field with `DATAOWNER` as default value.

### Setting ownership by dataset urn pattern

Again, let’s suppose we’d like to append a series of users who we know to own a different dataset from a data source but aren't detected during normal ingestion. To do so, we can use the `pattern_add_dataset_ownership` module that’s included in the ingestion framework.  This will match the pattern to `urn` of the dataset and assign the respective owners.

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

Note `ownership_type` is an optional field with `DATAOWNER` as default value.

If you'd like to add more complex logic for assigning ownership, you can use the more generic `add_dataset_ownership` transformer, which calls a user-provided function to determine the ownership of each dataset.

```yaml
transformers:
  - type: "add_dataset_ownership"
    config:
      get_owners_to_add: "<your_module>.<your_function>"
```

Note that whatever owners you send via this will overwrite the owners present in the UI.
### Add domain to dataset 
let’s suppose we’d like to add a series of domain to dataset, in this case you can use `simple_add_dataset_domain` transformer.

The config, which we’d append to our ingestion recipe YAML, would look like this:

Here we can set `domains` to either urn (i.e. `urn:li:domain:engineering`) or simple domain name (i.e. `engineering`) 
in both of the cases domain should be provisioned on DataHub GMS

```yaml 
transformers:
  - type: "simple_add_dataset_domain"
    config:
      semantics: OVERWRITE
      domains:
        - urn:li:domain:engineering
```
It will add domain to all datasets, above yaml configuration will overwrite the existing domain of the datasets on DataHub GMS, 
if you want to preserve domain stored on DataHub GMS then set semantics to `PATCH` as shown in below configuration 

```yaml 
transformers:
  - type: "simple_add_dataset_domain"
    config:
      semantics: PATCH
      domains:
        - urn:li:domain:engineering
```

### Adding domain by dataset urn pattern
Let’s suppose we’d like to append a series of domain to specific datasets. To do so, we can use the `pattern_add_dataset_domain` transformer that’s included in the ingestion framework.  This will match the regex pattern to `urn` of the dataset and assign the respective domain urns given in the array.
    
The config, which we’d append to our ingestion recipe YAML, would look like this:

Here we can set domain list to either urn (i.e. `urn:li:domain:hr`) or simple domain name (i.e. `hr`) 
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

We can set semantics to `PATCH` to preserve the domain of the dataset stored on DataHub GMS.

### Mark dataset status

If you would like to stop a dataset from appearing in the UI, then you need to mark the status of the dataset as removed. You can use this transformer after filtering for the specific datasets that you want to mark as removed.

```yaml
transformers:
  - type: "mark_dataset_status"
    config:
      removed: true
```

### Add dataset browse paths

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

Note that whatever browse paths you send via this will overwrite the browse paths present in the UI.


### Adding a set of properties

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
from datahub.metadata.schema_classes import DatasetSnapshotClass

class MyPropertiesResolver(AddDatasetPropertiesResolverBase):
    def get_properties_to_add(self, current: DatasetSnapshotClass) -> Dict[str, str]:
        ### Add custom logic here        
        properties= {'my_custom_property': 'property value'}
        logging.info(f"Adding properties: {properties} to dataset: {current.urn}.")
        return properties
```

There also exists `simple_add_dataset_properties` transformer for directly assigning properties from the configuration.
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

To get started, we’ll initiate an `AddCustomOwnershipConfig` class that inherits from [`datahub.configuration.common.ConfigModel`](./src/datahub/configuration/common.py). The sole parameter will be an `owners_json` which expects a path to a JSON file containing a list of owner URNs. This will go in a file called `custom_transform_example.py`.

```python
from datahub.configuration.common import ConfigModel

class AddCustomOwnershipConfig(ConfigModel):
    owners_json: str
```

### Defining the transformer

Next, we’ll define the transformer itself, which must inherit from [`datahub.ingestion.api.transform.Transformer`](./src/datahub/ingestion/api/transform.py). The framework provides a helper class called [`datahub.ingestion.transformer.base_transformer.BaseTransformer`](./src/datahub/ingestion/transformer/base_transformer.py) that makes it super-simple to write transformers. 
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
  <summary>Advanced: installing as a package</summary>
Alternatively, create a `setup.py` in the same directory as our transform script to make it visible globally. After installing this package (e.g. with `python setup.py` or `pip install -e .`), our module will be installed and importable as `custom_transform_example`.

```python
from setuptools import find_packages, setup

setup(
    name="custom_transform_example",
    version="1.0",
    packages=find_packages(),
    # if you don't already have DataHub installed, add it under install_requires
	# install_requires=["acryl-datahub"]
)
```

</details>

### Running the transform

```yaml
transformers:
  - type: "custom_transform_example.AddCustomOwnership"
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

All the files for this tutorial may be found [here](./examples/transforms/).
