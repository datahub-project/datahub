# Developing an Action

In this guide, we will outline each step to developing a Action for the DataHub Actions Framework.

## Overview

Developing a DataHub Action is a matter of extending the `Action` base class in Python, installing your
Action to make it visible to the framework, and then configuring the framework to use the new Action.


## Step 1: Defining an Action

To implement an Action, we'll need to extend the `Action` base class and override the following functions:

- `create()` - This function is invoked to instantiate the action, with a free-form configuration dictionary
  extracted from the Actions configuration file as input.
- `act()` - This function is invoked when an Action is received. It should contain the core logic of the Action.
- `close()` - This function is invoked when the framework has issued a shutdown of the pipeline. It should be used
  to cleanup any processes happening inside the Action. 

Let's start by defining a new implementation of Action called `CustomAction`. We'll keep it simple-- this Action will
print the configuration that is provided when it is created, and print any Events that it receives.

```python
# custom_action.py
from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext

class CustomAction(Action):
    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        # Simply print the config_dict.
        print(config_dict)
        return cls(ctx)

    def __init__(self, ctx: PipelineContext):
        self.ctx = ctx

    def act(self, event: EventEnvelope) -> None:
        # Do something super important.
        # For now, just print. :) 
        print(event)

    def close(self) -> None:
        pass
```


## Step 2: Installing the Action

Now that we've defined the Action, we need to make it visible to the framework by making it 
available in the Python runtime environment. 

The easiest way to do this is to just place it in the same directory as your configuration file, in which case the module name is the same as the file 
name - in this case it will be `custom_action`.

### Advanced: Installing as a Package

Alternatively, create a `setup.py` file in the same directory as the new Action to convert it into a package that pip can understand.

```
from setuptools import find_packages, setup

setup(
    name="custom_action_example",
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

Once we have done this, our class will be referencable via `custom_action_example.custom_action:CustomAction`.


## Step 3: Running the Action

Now that we've defined our Action, we can create an Action configuration file that refers to the new Action.
We will need to provide the fully-qualified Python module & class name when doing so.

*Example Configuration*

```yaml
# custom_action.yaml
name: "custom_action_test"
source:
  type: "kafka"
  config:
    connection:
      bootstrap: ${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}
      schema_registry_url: ${SCHEMA_REGISTRY_URL:-http://localhost:8081}
action:
  type: "custom_action_example.custom_action:CustomAction"
  config:
    # Some sample configuration which should be printed on create.
    config1: value1
```

Next, run the `datahub actions` command as usual:

```shell
datahub actions -c custom_action.yaml
```

If all is well, your Action should now be receiving & printing Events.


## (Optional) Step 4: Contributing the Action

If your Action is generally applicable, you can raise a PR to include it in the core Action library
provided by DataHub. All Actions will live under the `datahub_actions/plugin/action` directory inside the 
[datahub-actions](https://github.com/acryldata/datahub-actions) repository.

Once you've added your new Action there, make sure that you make it discoverable by updating the `entry_points` section
of the `setup.py` file. This allows you to assign a globally unique name for you Action, so that people can use
it without defining the full module path.

### Prerequisites:

Prerequisites to consideration for inclusion in the core Actions library include

- **Testing** Define unit tests for your Action
- **Deduplication** Confirm that no existing Action serves the same purpose, or can be easily extended to serve the same purpose
