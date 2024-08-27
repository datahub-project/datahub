---
title: "Using a Custom Ingestion Source"
---


# How to use a custom ingestion source without forking Datahub?

Adding a custom ingestion source is the easiest way to extend Datahubs ingestion framework to support source systems
which are not yet officially supported by Datahub.

## What you need to do

First thing to do is building a custom source like it is described in
the [metadata-ingestion source guide](../../metadata-ingestion/adding-source.md) in your own project.

### How to use this source?

:::note
[UI Based Ingestion](../ui-ingestion.md) currently does not support custom ingestion sources.
:::

To be able to use this source you just need to do a few things.

1. Build a python package out of your project including the custom source class.
2. Install this package in your working environment where you are using the Datahub CLI to ingest metadata.

Now you are able to just reference your ingestion source class as a type in the YAML recipe by using the fully qualified
package name. For example if your project structure looks like this `<project>/src/my-source/custom_ingestion_source.py`
with the custom source class named `MySourceClass` your YAML recipe would look like the following:

```yaml
source:
  type: my-source.custom_ingestion_source.MySourceClass
  config:
  # place for your custom config defined in the configModel
```

If you now execute the ingestion the datahub client will pick up your code and call the `get_workunits` method and do
the rest for you. That's it.

### Example code?

For examples how this setup could look like and a good starting point for building your first custom source visit
our [meta-world](https://github.com/acryldata/meta-world) example repository.