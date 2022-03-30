# Adding a Metadata Ingestion Source

There are two ways of adding a metadata ingestion source.

1. You are going to contribute the custom source directly to the Datahub project.
2. You are writing the custom source for yourself and are not going to contribute back (yet).

If you are going for case (1) just follow the steps 1 to 9 below. In case you are building it for yourself you can skip
steps 4-9 (but maybe write tests and docs for yourself as well) and follow the documentation
on [how to use custom ingestion sources](../docs/how/add-custom-ingestion-source.md)
without forking Datahub.

:::note

This guide assumes that you've already followed the metadata ingestion [developing guide](./developing.md) to set up
your local environment.

:::

### 1. Set up the configuration model

We use [pydantic](https://pydantic-docs.helpmanual.io/) for configuration, and all models must inherit
from `ConfigModel`. The [file source](./src/datahub/ingestion/source/file.py) is a good example.

### 2. Set up the reporter

The reporter interface enables the source to report statistics, warnings, failures, and other information about the run.
Some sources use the default `SourceReport` class, but others inherit and extend that class.

### 3. Implement the source itself

The core for the source is the `get_workunits` method, which produces a stream of MCE objects.
The [file source](./src/datahub/ingestion/source/file.py) is a good and simple example.

The MetadataChangeEventClass is defined in the metadata models which are generated
under `metadata-ingestion/src/datahub/metadata/schema_classes.py`. There are also
some [convenience methods](./src/datahub/emitter/mce_builder.py) for commonly used operations.

### 4. Set up the dependencies

Declare the source's pip dependencies in the `plugins` variable of the [setup script](./setup.py).

### 5. Enable discoverability

Declare the source under the `entry_points` variable of the [setup script](./setup.py). This enables the source to be
listed when running `datahub check plugins`, and sets up the source's shortened alias for use in recipes.

### 6. Write tests

Tests go in the `tests` directory. We use the [pytest framework](https://pytest.org/).

### 7. Write docs

Create a copy of [`source-docs-template.md`](./source-docs-template.md) and edit all relevant components. 

Add the plugin to the table under [CLI Sources List](../docs/cli.md#sources), and add the source's documentation underneath the [sources folder](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion/source_docs).

### 8. Add SQL Alchemy mapping (if applicable)

Add the source in `get_platform_from_sqlalchemy_uri` function
in [sql_common.py](./src/datahub/ingestion/source/sql/sql_common.py) if the source has an sqlalchemy source

### 9. Add logo

Add logo image in [images folder](../datahub-web-react/src/images) and add it to be ingested
in [boot](../metadata-service/war/src/main/resources/boot/data_platforms.json)
