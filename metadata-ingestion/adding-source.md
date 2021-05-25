# Adding a Metadata Ingestion Source

:::note

This guide assumes that you've already followed the metadata ingestion [developing guide](./developing.md) to set up your local environment.

:::

### 1. Set up the configuration model

We use [pydantic](https://pydantic-docs.helpmanual.io/) for configuration, and all models must inherit from `ConfigModel`. The [file source](./src/datahub/ingestion/source/mce_file.py) is a good example.

### 2. Set up the reporter

The reporter interface enables the source to report statistics, warnings, failures, and other information about the run. Some sources use the default `SourceReport` class, but others inherit and extend that class.

### 3. Implement the source itself

The core for the source is the `get_workunits` method, which produces a stream of MCE objects. The [file source](./src/datahub/ingestion/source/mce_file.py) is a good and simple example.

The MetadataChangeEventClass is defined in the [metadata models](./src/datahub/metadata/schema_classes.py). There are also some [convenience methods](./src/datahub/emitter/mce_builder.py) for commonly used operations.

### 4. Set up the dependencies

Declare the source's pip dependencies in the `plugins` variable of the [setup script](./setup.py).

### 5. Enable discoverability

Declare the source under the `entry_points` variable of the [setup script](./setup.py). This enables the source to be listed when running `datahub check plugins`, and sets up the source's shortened alias for use in recipes.

### 6. Write tests

Tests go in the `tests` directory. We use the [pytest framework](https://pytest.org/).

### 7. Write docs

Add the plugin to the table at the top of the README file, and add the source's documentation underneath the sources header.
