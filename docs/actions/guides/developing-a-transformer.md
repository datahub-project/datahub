# Developing a Transformer

In this guide, we will outline each step to developing a custom Transformer for the DataHub Actions Framework.

## Overview

Developing a DataHub Actions Transformer is a matter of extending the `Transformer` base class in Python, installing your
Transformer to make it visible to the framework, and then configuring the framework to use the new Transformer.


## Step 1: Defining a Transformer

To implement an Transformer, we'll need to extend the `Transformer` base class and override the following functions:

- `create()` - This function is invoked to instantiate the action, with a free-form configuration dictionary
  extracted from the Actions configuration file as input.
- `transform()` - This function is invoked when an Event is received. It should contain the core logic of the Transformer.
  and will return the transformed Event, or `None` if the Event should be filtered.

Let's start by defining a new implementation of Transformer called `CustomTransformer`. We'll keep it simple-- this Transformer will
print the configuration that is provided when it is created, and print any Events that it receives.

```python
# custom_transformer.py
from datahub_actions.transform.transformer import Transformer
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext
from typing import Optional

class CustomTransformer(Transformer):
    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Transformer":
        # Simply print the config_dict.
        print(config_dict)
        return cls(config_dict, ctx)

    def __init__(self, ctx: PipelineContext):
        self.ctx = ctx

    def transform(self, event: EventEnvelope) -> Optional[EventEnvelope]:
        # Simply print the received event.
        print(event)
        # And return the original event (no-op)
        return event
```


## Step 2: Installing the Transformer

Now that we've defined the Transformer, we need to make it visible to the framework by making
it available in the Python runtime environment.

The easiest way to do this is to just place it in the same directory as your configuration file, in which case the module name is the same as the file
name - in this case it will be `custom_transformer`.

### Advanced: Installing as a Package

Alternatively, create a `setup.py` file in the same directory as the new Transformer to convert it into a package that pip can understand.

```
from setuptools import find_packages, setup

setup(
    name="custom_transformer_example",
    version="1.0",
    packages=find_packages(),
    # if you don't already have DataHub Actions installed, add it under install_requires
    # install_requires=["acryl-datahub-actions"]
)
```

Next, install the package

```shell
pip install -e .
```

inside the module. (alt.`python setup.py`).

Once we have done this, our class will be referencable via `custom_transformer_example.custom_transformer:CustomTransformer`.


## Step 3: Running the Action

Now that we've defined our Transformer, we can create an Action configuration file that refers to the new Transformer.
We will need to provide the fully-qualified Python module & class name when doing so.

*Example Configuration*

```yaml
# custom_transformer_action.yaml
name: "custom_transformer_test"
source:
  type: "kafka"
  config:
    connection:
      bootstrap: ${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}
      schema_registry_url: ${SCHEMA_REGISTRY_URL:-http://localhost:8081}
transform:
  - type: "custom_transformer_example.custom_transformer:CustomTransformer"
    config:
      # Some sample configuration which should be printed on create.
      config1: value1
action:
  # Simply reuse the default hello_world action
  type: "hello_world"
```

Next, run the `datahub actions` command as usual:

```shell
datahub actions -c custom_transformer_action.yaml
```

If all is well, your Transformer should now be receiving & printing Events.


### (Optional) Step 4: Contributing the Transformer

If your Transformer is generally applicable, you can raise a PR to include it in the core Transformer library
provided by DataHub. All Transformers will live under the `datahub_actions/plugin/transform` directory inside the
[datahub-actions](https://github.com/acryldata/datahub-actions) repository.

Once you've added your new Transformer there, make sure that you make it discoverable by updating the `entry_points` section
of the `setup.py` file. This allows you to assign a globally unique name for you Transformer, so that people can use
it without defining the full module path.

#### Prerequisites:

Prerequisites to consideration for inclusion in the core Transformer library include

- **Testing** Define unit tests for your Transformer
- **Deduplication** Confirm that no existing Transformer serves the same purpose, or can be easily extended to serve the same purpose
