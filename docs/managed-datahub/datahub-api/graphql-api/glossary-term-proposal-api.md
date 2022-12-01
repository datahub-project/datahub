---
description: >-
  This page will provide an overview about how to use the DataHub Glossary Term
  Proposal API to submit proposals to attach particular Business Glossary Terms
  to a Metadata Entity.
---

# Glossary Term Proposal API

## Introduction&#x20;

DataHub exposes a [GraphQL API](https://graphql.org/) for proposing Glossary Terms for Metadata Entities indexed on the platform. Proposed Business Glossary Terms can subsequently be approved or denied inside the DataHub Control Center (UI).&#x20;

At a high level, callers of this API will be required to provide the following details:

1. A unique identifier for the target Metadata Entity (URN)&#x20;
2. An optional **** sub-resource identifier which designates a sub-resource to attach the Term to, for example reference to a particular "field" within a Dataset.&#x20;
3. A unique identifier **** for the Term they wish to propose (URN)&#x20;

In the following sections, we will describe how to construct each of these items and use the DataHub GraphQL API to submit Term proposals.&#x20;

## Constructing an Entity Identifier

Inside DataHub, each Metadata Entity is uniquely identifier by a universal resource name, or an **URN**. URNs are human-readable, and embed the unique physical coordinates of a Metadata Entity, often in a 3rd party system such as a database instance. In order to propose a term for an entity, the client must be capable of constructing an URN.

&#x20;For information about URN format for each Metadata Entity type, see [URNs](../concepts/urns.md).

## Constructing a Sub-Resource Identifier&#x20;

Specific Metadata Entity types have additional sub-resources to which terms may be applied.&#x20;

Today, this only applies for **Dataset** Metadata Entities, which have a "fields" sub-resource, with each field uniquely identified using a standard **path** format, reflecting its place within the Dataset's data schema.&#x20;

To construct a field path, see [Constructing Dataset Field Paths](../concepts/constructing-dataset-field-paths.md).&#x20;

## Constructing a Term Identifier&#x20;

Terms are also uniquely identified by an URN. Term URNs have the following format:

```
urn:li:glossaryTerm:<term-name>
```

For example, a Term called PII would have the following URN:

```
urn:li:glossaryTerm:PII
```

When proposing a Term, we must provide the Term as an URN, following the format shown above.&#x20;

## Creating a Proposal via GraphQL&#x20;

Once we've constructed an Entity URN, any relevant sub-resource identifiers, and a Term URN, we're ready to propose a Term association! To do so, we'll use the DataHub GraphQL API.&#x20;

If you're unfamiliar with GraphQL, we'd highly recommend checking out the comprehensive [Intro to GraphQL](https://www.google.com/search?q=graphql+introduce\&oq=graphql+introduce\&aqs=chrome..69i57j0i433i512l3j69i60l4.1493j0j9\&sourceid=chrome\&ie=UTF-8) primer provided by the GraphQL Community.&#x20;

Proposing a Term involves executing a [GraphQL Mutation](https://graphql.org/learn/queries/#mutations) against the DataHub GraphQL API, located by default at `http://your-datahub-domain>/api/graphql` . In particular, we'll be using the `proposeTerm` Mutation, which has the following interface:&#x20;

```
type Mutation {
    proposeTerm(input: TermProposalInput!): String! # Returns Proposal URN.
}

input TermProposalInput {
    termUrn: String! # Required. e.g. "urn:li:glossaryTerm:PII"
    subResource: String # Optional. e.g. "fieldName"
    subResourceType: String # Optional. "DATASET_FIELD" for dataset fields
    resourceUrn: String! # Required. e.g. "urn:li:dataset:(...)"
}
```

As illustrated above, the `proposeTerm` Mutation accepts a single 'input' argument, which in turn contains 3 fields:

1. _resourceUrn_: The URN associated with the target Entity.&#x20;
2. _subResource_: An optional sub-resource identifier.
3. _subResourceType:_ An optional enum indicating what type of subresource is being referenced.
4. &#x20;_termUrn_: The URN associated with the Term to propose.&#x20;

:::info
By default, DataHub deploys with an interactive, web-based API Explorer (GraphiQL)  located at`http://your-datahub-domain/api/graphiql`. Check it out to familiarize yourself with the API.
:::

### Adding the Actor Header

Along with the GraphQL payload itself, DataHub's [Metadata Service](https://datahubproject.io/docs/metadata-service/) requires the presence of a special header identifying the caller. Today, the caller identity is self-provided and not validated, something that will change in the near future.&#x20;

For now, you can simply attach a header `X-DataHub-Actor` which contains a "service" URN capable of uniquely identifying a programmatic caller for auditing purposes. In the future, this value will be replaced by an Authorization Token which will be validated by the Metadata Service, and embed the caller identity.&#x20;

```
X-DataHub-Actor: urn:li:service:<service-principal-name>
```

For example, if we're proposing Terms from an internal agent, we may provide the following header value:

```
X-DataHub-Actor: urn:li:service:term-proposal-agent
```

### Examples

#### CURL

To issue a CURL invoking the `termProposal` GraphQL Mutation , you can use the following template:

```
curl --location --request POST 'http://your-datahub-domain/api/graphql' \
--header 'Content-Type: application/json' \
--header 'X-DataHub-Actor: urn:li:service:<service-principal-name>' \
--data-raw '{"query":"mutation proposeTerm($resourceUrn: String!, $termUrn: String!) {\n    proposeTerm(resourceUrn: $resourceUrn, termUrn: $termUrn)","variables":{"resourceUrn":"<resource-urn>","termUrn":"<term-urn>"}}'
```

For example, to add the Term "PII" to a MySQL Dataset with name "product.sales" in a production environment, I would issue the following query.&#x20;

```
curl --location --request POST 'http://your-datahub-domain/api/graphql' \
--header 'Content-Type: application/json' \
--header 'X-DataHub-Actor: urn:li:service:term-proposal-agent' \
--data-raw '{"query":"mutation proposeTerm($resourceUrn: String!, $termUrn: String!) {\n    proposeTerm(resourceUrn: $resourceUrn, termUrn: $termUrn)","variables":{"resourceUrn":"urn:li:dataset:(urn:li:dataPlatform:mysql,product.sales,PROD)","termUrn":"urn:li:glossaryTerm:PII"}}'
```

### Language Support&#x20;

To issue a GraphQL query programmatically, you can make use of a GraphQL client library. For a full list, see [GraphQL Code Libraries, Tools, & Services](https://graphql.org/code/).&#x20;

## Querying for Proposals using GraphQL

You can also use the GraphQL API to search for proposals. We can use the  [`searchAcrossEntities`](https://datahubproject.io/docs/graphql/queries/#searchacrossentities) query to find entities with a given glossary term proposed on them. For example, if we want to find the first 10 datasets that have the glossary term `Classification.Confidential` proposed on them at the dataset level, issue the following query.

```
query searchAcrossEntities($input: SearchAcrossEntitiesInput!) {
    searchAcrossEntities(input: $input) {
    start
    count
    total
    searchResults {
      entity {
        urn
      }
    }
  }
}

{
    "input": {
        "types": ["DATASET"],
      	"query": "Confidential",
        "start": 0,
        "filters": [
          {
            "field": "proposedGlossaryTerms",
            "value": "urn:li:glossaryTerm:Classification.Confidential"
          }
        ],
        "count": 10
    }
}
```

If we want to find the first 10 datasets that have the glossary term `Confidential.Sensitive` proposed on them at the schema level, issue the following query.

```
query searchAcrossEntities($input: SearchAcrossEntitiesInput!) {
    searchAcrossEntities(input: $input) {
    start
    count
    total
    searchResults {
      entity {
        urn
      }
    }
  }
}

{
    "input": {
        "types": ["DATASET"],
      	"query": "Confidential",
        "start": 0,
        "filters": [
          {
            "field": "proposedSchemaGlossaryTerms",
            "value": "urn:li:glossaryTerm:Classification.Confidential"
          }
        ],
        "count": 10
    }
}
```
