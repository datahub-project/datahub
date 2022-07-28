---
title: "Introduction"
---

# Transformers

## What’s a transformer?

Oftentimes we want to modify metadata before it reaches the ingestion sink – for instance, we might want to add custom tags, ownership, properties, or patch some fields. A transformer allows us to do exactly these things.

Moreover, a transformer allows one to have fine-grained control over the metadata that’s ingested without having to modify the ingestion framework's code yourself. Instead, you can write your own module that can transform metadata events however you like. To include a transformer into a recipe, all that's needed is the name of the transformer as well as any configuration that the transformer needs.

## Provided transformers

Aside from the option of writing your own transformer (see below), we provide some simple transformers for the use cases of adding: tags, glossary terms, properties and ownership information.

DataHub provided transformers are 
- [Dataset Transformer](./dataset_transformer.md)
