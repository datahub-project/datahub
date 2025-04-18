---
title: "Components"
---

# DataHub Components Overview

The DataHub platform consists of the components shown in the following diagram. 


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/datahub-components.png"/>
</p>


## Metadata Store 

The Metadata Store is responsible for storing the [Entities & Aspects](https://datahubproject.io/docs/metadata-modeling/metadata-model/) comprising the Metadata Graph. This includes
exposing an API for [ingesting metadata](https://datahubproject.io/docs/metadata-service#ingesting-entities), [fetching Metadata by primary key](https://datahubproject.io/docs/metadata-service#retrieving-entities), [searching entities](https://datahubproject.io/docs/metadata-service#search-an-entity), and [fetching Relationships](https://datahubproject.io/docs/metadata-service#get-relationships-edges) between
entities. It consists of a Spring Java Service hosting a set of [Rest.li](https://linkedin.github.io/rest.li/) API endpoints, along with
MySQL, Elasticsearch, & Kafka for primary storage & indexing. 

Get started with the Metadata Store by following the [Quickstart Guide](https://datahubproject.io/docs/quickstart/). 

## Metadata Models

Metadata Models are schemas defining the shape of the Entities & Aspects comprising the Metadata Graph, along with the relationships between them. They are defined
using [PDL](https://linkedin.github.io/rest.li/pdl_schema), a modeling language quite similar in form to Protobuf while serializes to JSON. Entities represent a specific class of Metadata
Asset such as a Dataset, a Dashboard, a Data Pipeline, and beyond. Each *instance* of an Entity is identified by a unique identifier called an `urn`. Aspects represent related bundles of data attached
to an instance of an Entity such as its descriptions, tags, and more. View the current set of Entities supported [here](https://datahubproject.io/docs/metadata-modeling/metadata-model#exploring-datahubs-metadata-model). 

Learn more about DataHub models Metadata [here](https://datahubproject.io/docs/metadata-modeling/metadata-model/). 

## Ingestion Framework

The Ingestion Framework is a modular, extensible Python library for extracting Metadata from external source systems (e.g.
Snowflake, Looker, MySQL, Kafka), transforming it into DataHub's [Metadata Model](https://datahubproject.io/docs/metadata-modeling/metadata-model/), and writing it into DataHub via
either Kafka or using the Metadata Store Rest APIs directly. DataHub supports an [extensive list of Source connectors](https://datahubproject.io/docs/metadata-ingestion/#installing-plugins) to choose from, along with
a host of capabilities including schema extraction, table & column profiling, usage information extraction, and more.  

Getting started with the Ingestion Framework is as simple: just define a YAML file and execute the `datahub ingest` command.
Learn more by heading over the [Metadata Ingestion](https://datahubproject.io/docs/metadata-ingestion/) guide. 

## GraphQL API

The [GraphQL](https://graphql.org/) API provides a strongly-typed, entity-oriented API that makes interacting with the Entities comprising the Metadata
Graph simple, including APIs for adding and removing tags, owners, links & more to Metadata Entities! Most notably, this API is consumed by the User Interface (discussed below) for enabling Search & Discovery, Governance, Observability
and more. 

To get started using the GraphQL API, check out the [Getting Started with GraphQL](https://datahubproject.io/docs/api/graphql/getting-started) guide. 

## User Interface

DataHub comes with a React UI including an ever-evolving set of features to make Discovering, Governing, & Debugging your Data Assets easy & delightful.
For a full overview of the capabilities currently supported, take a look at the [Features](features.md) overview. For a look at what's coming next,
head over to the [Roadmap](https://datahubproject.io/docs/roadmap/). 

## Learn More

Learn more about the specifics of the [DataHub Architecture](./architecture/architecture.md) in the Architecture Overview. Learn about using & developing the components
of the Platform by visiting the Module READMEs. 

## Feedback / Questions / Concerns

We want to hear from you! For any inquiries, including Feedback, Questions, or Concerns, reach out on [Slack](https://datahubspace.slack.com/join/shared_invite/zt-nx7i0dj7-I3IJYC551vpnvvjIaNRRGw#/shared-invite/email)!
