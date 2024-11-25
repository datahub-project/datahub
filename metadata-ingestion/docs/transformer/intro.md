---
title: "Introduction"
---

# Transformers

## What’s a transformer?

Oftentimes we want to modify metadata before it reaches the ingestion sink – for instance, we might want to add custom tags, ownership, properties, or patch some fields. A transformer allows us to do exactly these things.

Moreover, a transformer allows one to have fine-grained control over the metadata that’s ingested without having to modify the ingestion framework's code yourself. Instead, you can write your own module that can transform metadata events however you like. To include a transformer into a recipe, all that's needed is the name of the transformer as well as any configuration that the transformer needs.

:::note

Providing urns for metadata that does not already exist will result in unexpected behavior. Ensure any tags, terms, domains, etc. urns that you want to apply in your transformer already exist in your DataHub instance.

For example, adding a domain urn in your transformer to apply to datasets will not create the domain entity if it doesn't exist. Therefore, you can't add documentation to it and it won't show up in Advanced Search. This goes for any metadata you are applying in transformers.

:::

## Provided transformers

Aside from the option of writing your own transformer (see below), we provide some simple transformers for the use cases of adding: tags, glossary terms, properties and ownership information.

DataHub provided transformers for dataset are:
- [Simple Add Dataset ownership](./dataset_transformer.md#simple-add-dataset-ownership)
- [Pattern Add Dataset ownership](./dataset_transformer.md#pattern-add-dataset-ownership)
- [Simple Remove Dataset ownership](./dataset_transformer.md#simple-remove-dataset-ownership)
- [Extract Ownership from Tags](./dataset_transformer.md#extract-ownership-from-tags)
- [Clean suffix prefix from Ownership](./dataset_transformer.md#clean-suffix-prefix-from-ownership)
- [Mark Dataset Status](./dataset_transformer.md#mark-dataset-status)
- [Simple Add Dataset globalTags](./dataset_transformer.md#simple-add-dataset-globaltags)
- [Pattern Add Dataset globalTags](./dataset_transformer.md#pattern-add-dataset-globaltags)
- [Add Dataset globalTags](./dataset_transformer.md#add-dataset-globaltags)
- [Set Dataset browsePath](./dataset_transformer.md#set-dataset-browsepath)
- [Simple Add Dataset glossaryTerms](./dataset_transformer.md#simple-add-dataset-glossaryterms)
- [Pattern Add Dataset glossaryTerms](./dataset_transformer.md#pattern-add-dataset-glossaryterms)
- [Add Dataset globalTags](./dataset_transformer.md#add-dataset-globaltags)
- [Pattern Add Dataset Schema Field glossaryTerms](./dataset_transformer.md#pattern-add-dataset-schema-field-glossaryterms)
- [Pattern Add Dataset Schema Field globalTags](./dataset_transformer.md#pattern-add-dataset-schema-field-globaltags)
- [Simple Add Dataset datasetProperties](./dataset_transformer.md#simple-add-dataset-datasetproperties)
- [Add Dataset datasetProperties](./dataset_transformer.md#add-dataset-datasetproperties)
- [Simple Add Dataset domains](./dataset_transformer.md#simple-add-dataset-domains)
- [Pattern Add Dataset domains](./dataset_transformer.md#pattern-add-dataset-domains)
- [Domain Mapping Based on Tags](./dataset_transformer.md#domain-mapping-based-on-tags)  
- [Simple Add Dataset dataProduct ](./dataset_transformer.md#simple-add-dataset-dataproduct)
- [Pattern Add Dataset dataProduct](./dataset_transformer.md#pattern-add-dataset-dataproduct)
- [Add Dataset dataProduct](./dataset_transformer.md#add-dataset-dataproduct)
