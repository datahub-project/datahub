---
title: "Introduction"
---

# Transformers

## What’s a transformer?

Oftentimes we want to modify metadata before it reaches the ingestion sink – for instance, we might want to add custom tags, ownership, properties, or patch some fields. A transformer allows us to do exactly these things.

Moreover, a transformer allows one to have fine-grained control over the metadata that’s ingested without having to modify the ingestion framework's code yourself. Instead, you can write your own module that can transform metadata events however you like. To include a transformer into a recipe, all that's needed is the name of the transformer as well as any configuration that the transformer needs.

## Provided transformers

Aside from the option of writing your own transformer (see below), we provide some simple transformers for the use cases of adding: tags, glossary terms, properties and ownership information.

DataHub provided transformers for dataset are:
- [Simple Add Dataset ownership](./dataset_transformer.md#simple-add-dataset-ownership)
- [Pattern Add Dataset ownership](./dataset_transformer.md#pattern-add-dataset-ownership)
- [Simple Remove Dataset ownership](./dataset_transformer.md#simple-remove-dataset-ownership)
- [Mark Dataset Status](./dataset_transformer.md#mark-dataset-status)
- [Simple Add Dataset globalTags](./dataset_transformer.md#simple-add-dataset-globaltags)
- [Pattern Add Dataset globalTags](./dataset_transformer.md#pattern-add-dataset-globaltags)
- [Add Dataset globalTags](./dataset_transformer.md#add-dataset-globaltags)
- [Set Dataset browsePath](./dataset_transformer.md#set-dataset-browsepath)
- [Simple Add Dataset glossaryTerms](./dataset_transformer.md#simple-add-dataset-glossaryterms)
- [Pattern Add Dataset glossaryTerms](./dataset_transformer.md#pattern-add-dataset-glossaryterms)
- [Pattern Add Dataset Schema Field glossaryTerms](./dataset_transformer.md#pattern-add-dataset-schema-field-glossaryterms)
- [Pattern Add Dataset Schema Field globalTags](./dataset_transformer.md#pattern-add-dataset-schema-field-globaltags)
- [Simple Add Dataset datasetProperties](./dataset_transformer.md#simple-add-dataset-datasetproperties)
- [Add Dataset datasetProperties](./dataset_transformer.md#add-dataset-datasetproperties)
- [Simple Add Dataset domains](./dataset_transformer.md#simple-add-dataset-domains)
- [Pattern Add Dataset domains](./dataset_transformer.md#pattern-add-dataset-domains)
