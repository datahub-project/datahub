# CLI Ingestion

## Installing the CLI

Make sure you have installed DataHub CLI before following this guide. 
```shell
# Requires Python 3.7+
python3 -m pip install --upgrade pip wheel setuptools
python3 -m pip install --upgrade acryl-datahub
# validate that the install was successful
datahub version
# If you see "command not found", try running this instead: python3 -m datahub version
```
Check out the [CLI Installation Guide](../docs/cli.md#installation) for more installation options and troubleshooting tips. 

After that, install the required plugin for the ingestion.

```shell
pip install 'acryl-datahub[datahub-rest]'  # install the required plugin
```
Check out the [alternative installation options](../docs/cli.md#alternate-installation-options) for more reference. 

## Configuring a Recipe
Create a recipe.yml file that defines the source and sink for metadata, as shown below.
```yaml
# my_reipe.yml
source:
  type: <source_name>
  config:
    option_1: <value>
    ...
  
sink:
  type: <sink_type_name>
  config:
    ...
```

For more information and examples on configuring recipes, please refer to [Recipes](recipe_overview.md).

## Ingesting Metadata
You can run ingestion using `datahub ingest` like below.  

```shell
datahub ingest -c <path_to_recipe_file.yml>
```

## Reference

Please refer the following pages for advanced guids on CLI ingestion.
- [Reference for `datahub ingest` command](../docs/cli.md#ingest)
- [UI Ingestion Guide](../docs/ui-ingestion.md)

:::Tip Compatibility
DataHub server uses a 3 digit versioning scheme, while the CLI uses a 4 digit scheme. For example, if you're using DataHub server version 0.10.0, you should use CLI version 0.10.0.x, where x is a patch version.
We do this because we do CLI releases at a much higher frequency than server releases, usually every few days vs twice a month.

For ingestion sources, any breaking changes will be highlighted in the [release notes](../docs/how/updating-datahub.md). When fields are deprecated or otherwise changed, we will try to maintain backwards compatibility for two server releases, which is about 4-6 weeks. The CLI will also print warnings whenever deprecated options are used.
:::