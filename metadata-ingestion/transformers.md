# Using transformers

## What’s a transformer?

Sometimes we want to modify metadata before it reaches the ingestion sink – for instance, we might want to add custom tags, ownership, or patch some fields. A transformer allows us to do exactly these things.

Moreover, a transformer allows one to have fine-grained control over the metadata that’s ingested without having to modify the source code of the ingestion framework itself. Instead, you can write your own independent module.

## Provided transformers

We provide two simple transformers for the use cases of adding dataset tags and ownership information.

### Adding a set of tags

Let’s suppose we’d like to add a set of dataset tags. To do so, we can use the `simple_add_dataset_tags` module that’s provided by the ingestion framework.

The config (which we’d append to our ingestion recipe YAML) would look like this:

```yaml
transformers:
  - type: "simple_add_dataset_tags"
    config:
      tag_urns:
        - "urn:li:tag:NeedsDocumentation"
        - "urn:li:tag:Legacy"
```

### Setting ownership

Let’s suppose we’d like to add a set of dataset tags. To do so, we can use the `simple_add_dataset_tags` module that’s provided by the ingestion framework.

The config (which we’d append to our ingestion recipe YAML) would look like this:

```yaml
transformers:
  - type: "simple_add_dataset_ownership"
    config:
      owner_urns:
        - "urn:li:corpuser:username1"
        - "urn:li:corpuser:username2"
        - "urn:li:corpGroup:groupname"
```

## Writing a transformer from scratch

In the above couple of examples, we use classes that have already been implemented in the ingestion framework. However, it’s common for more advanced cases to pop up where custom code is required. In such cases, we can add our own class and define what arguments it takes.

Suppose we want to append a set of ownership fields to our metadata that are dependent upon an external source – for instance, an API endpoint or file – rather than a preset list like above.

### Defining a config

To get started, we’ll initiate an `AddCustomOwnershipConfig` class that inherits from `datahub.configuration.common.ConfigModel`. The sole parameter will be an `owners_file` which expects a path to a JSON file containing a list of owner URNs. This will go in a file called `custom_transform_example.py`.

```python
from datahub.configuration.common import ConfigModel

class AddCustomOwnershipConfig(ConfigModel):
    owners_json: str
```

### Defining the transformer

Next, we’ll define the transformer itself, which will inherit from `datahub.ingestion.api.transform.Transformer`. First, let's get all our imports in:

```python
# append these to the start of custom_transform_example.py

import json
from typing import Iterable

# for constructing URNs
import datahub.emitter.mce_builder as builder
# for typing the config model
from datahub.configuration.common import ConfigModel
# for typing context and records
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
# base transformer class
from datahub.ingestion.api.transform import Transformer
# MCE-related classes
from datahub.metadata.schema_classes import (
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)
```

First, let's define the base scaffolding for the class:

```python
# append this to the end of custom_transform_example.py

class AddCustomOwnership(Transformer):
    """Transformer that adds owners to datasets according to a callback function."""

    # context param to generate run metadata such as a run ID
    ctx: PipelineContext
    # as defined in the previous block
    config: AddCustomOwnershipConfig

    def __init__(self, config: AddCustomOwnershipConfig, ctx: PipelineContext):
        self.ctx = ctx
        self.config = config

        self.owners = [
            OwnerClass(owner=owner, type=OwnershipTypeClass.DATAOWNER)
            for owner in json.loads(config.owner_file)
        ]
```

A transformer should have two functions: a `create()` function for initialization and a `transform()` function for executing the transformation.

Let's begin by adding a `create()` method for taking our configuration dictionary and parsing it:

```python
# add this as a function of AddCustomOwnership

@classmethod
def create(cls, config_dict: dict, ctx: PipelineContext) -> "AddCustomOwnership":
    config = AddCustomOwnershipConfig.parse_obj(config_dict)
    return cls(config, ctx)
```

Now we've got to add a `transform()` method that does the work of adding our custom ownership classes. This method will take as input an MCE and output the transformed MCE. Let's offload the processing of each MCE to another `transform_one()` class.

```python
# add this as a function of AddCustomOwnership

def transform(
    self, record_envelopes: Iterable[RecordEnvelope]
) -> Iterable[RecordEnvelope]:

    # loop over envelopes
    for envelope in record_envelopes:

        # if envelope is an MCE, add the ownership classes
        if isinstance(envelope.record, MetadataChangeEventClass):
            envelope.record = self.transform_one(envelope.record)
        yield envelope
```

The `transform_one()` method will take a single MCE and add the owners that we loaded from the JSON.

```python
# add this as a function of AddCustomOwnership

def transform_one(self, mce: MetadataChangeEventClass) -> MetadataChangeEventClass:
    if not isinstance(mce.proposedSnapshot, DatasetSnapshotClass):
        return mce

    owners_to_add = self.owners

    if owners_to_add:
        ownership = builder.get_or_add_aspect(
            mce,
            OwnershipClass(
                owners=[],
            ),
        )
        ownership.owners.extend(owners_to_add)

    return mce
```

### Installing the package

Now that we've defined the transformer, we can set up the package for installation. To do so, create a `setup.py` in the same directory:

```python
from setuptools import find_packages, setup

setup(
    name="custom_transform_example",
    version="1.0",
    packages=find_packages(),
    # if you don't already have DataHub installed, add it under install_requires
		# install_requires=["datahub"]
)
```

### Running the transform

After installing this (e.g. with `python setup.py` or `pip install -e .`), our module will be importable.

```yaml
transformers:
  - type: "custom_transform_example.AddCustomOwnership"
    config:
      owners_json: "<path_to_owners_json>"
```

We provide a simple JSON if you'd like to test it:

```json
[
  "urn:li:corpuser:athos",
  "urn:li:corpuser:porthos",
  "urn:li:corpuser:aramis",
  "urn:li:corpGroup:the_three_musketeers"
]
```

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
