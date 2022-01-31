# Business Glossary

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

Works with `acryl-datahub` out of the box.

## Capabilities

This plugin pulls business glossary metadata from a yaml-formatted file. An example of one such file is located in the examples directory [here](../examples/bootstrap_data/business_glossary.yml).

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: datahub-business-glossary
  config:
    # Coordinates
    file: /path/to/business_glossary_yaml

sink:
  # sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field      | Required | Default | Description             |
| ---------- | -------- | ------- | ----------------------- |
| `file` | ✅       |         | Path to business glossary file to ingest. |

### Business Glossary File Format

The business glossary source file should be a `.yml` file with the following top-level keys:

**Glossary**: the top level keys of the business glossary file
- **version**: the version of business glossary file config the config conforms to. Currently the only version released is `1`.
- **source**: the source format of the terms. Currently only supports `DataHub`
- **owners**: owners contains two nested fields
  - **users**: (optional) a list of user ids
  - **groups**: (optional) a list of group ids
- **url**: (optional) external url pointing to where the glossary is defined externally, if applicable.
- **nodes**: (optional) list of child **GlossaryNode** objects
- **terms**: (optional) list of child **GlossaryTerm** objects


**GlossaryNode**: a container of **GlossaryNode** and **GlossaryTerm** objects
- **name**: name of the node
- **description**: description of the node
- **owners**: (optional) owners contains two nested fields
  - **users**: (optional) a list of user ids
  - **groups**: (optional) a list of group ids
- **terms**: (optional) list of child **GlossaryTerm** objects
- **nodes**: (optional) list of child **GlossaryNode** objects

**GlossaryTerm**: a term in your business glossary
- **name**: name of the term
- **description**: description of the term
- **owners**: (optional) owners contains two nested fields
  - **users**: (optional) a list of user ids
  - **groups**: (optional) a list of group ids
- **term_source**: One of `EXTERNAL` or `INTERNAL`. Whether the term is coming from an external glossary or one defined in your organization.
- **source_ref**: (optional) If external, what is the name of the source the glossary term is coming from?
- **source_url**: (optional) If external, what is the url of the source definition?
- **inherits**: (optional) List of **GlossaryTerm** that this term inherits from
- **contains**: (optional) List of **GlossaryTerm** that this term contains
- **custom_properties**: A map of key/value pairs of arbitrary custom properties

You can also view an example business glossary file checked in [here](../examples/bootstrap_data/business_glossary.yml)

## Compatibility

Compatible with version 1 of business glossary format. 
The source will be evolved as we publish newer versions of this format.

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
