---
description: >-
  This page will provide an overview about how to use the DataHub Tag Proposal
  API to submit proposals to attach particular Tags to a Metadata Entity.
---

# Tag Proposal API

## Introduction&#x20;

DataHub exposes a [GraphQL API](https://graphql.org/) for proposing tags for Metadata Entities indexed on the platform. Proposed tags can subsequently be approved or denied inside the DataHub Control Center (UI).&#x20;

At a high level, callers of this API will be required to provide the following details:

1. A unique identifier for the target Metadata Entity (URN)&#x20;
2. An optional **** sub-resource identifier which designates a sub-resource to attach the Tag to, for example reference to a particular "field" within a Dataset.&#x20;
3. A unique identifier **** for the tag they wish to propose (URN)&#x20;

In the following sections, we will describe how to construct each of these items and use the DataHub GraphQL API to submit Tag proposals.&#x20;

## Constructing an Entity Identifier

Inside DataHub, each Metadata Entity is uniquely identifier by a universal resource name, or an **URN**. URNs are human-readable, and embed the unique physical coordinates of a Metadata Entity, often in a 3rd party system such as a database instance. In order to propose a tag for an entity, the client must be capable of constructing an URN.

&#x20;For information about URN format for each Metadata Entity type, see [URNs](../concepts/urns.md).

## Constructing a Sub-Resource Identifier&#x20;

Specific Metadata Entity types have additional sub-resources to which tags may be applied.&#x20;

Today, this only applies for **Dataset** Metadata Entities, which have a "fields" sub-resource, with each field uniquely identified using a standard **path** format, reflecting its place within the Dataset's data schema.&#x20;

To construct a field path, see [Constructing Dataset Field Paths](../concepts/constructing-dataset-field-paths.md).&#x20;

## Constructing a Tag Identifier&#x20;

Tags are also uniquely identified by an URN. Tag URNs have the following format:

```
urn:li:tag:<tag-name>
```

For example, a Tag called Marketing would have the following URN:

```
urn:li:tag:Marketing
```

When proposing a Tag, we must provide the Tag as an URN, following the format shown above.&#x20;

## Issuing a GraphQL Query&#x20;

Once we've constructed an Entity URN, any relevant sub-resource identifiers, and a Tag URN, we're ready to propose a Tag! To do so, we'll use the DataHub GraphQL API.&#x20;

If you're unfamiliar with GraphQL, we'd highly recommend checking out the comprehensive [Intro to GraphQL](https://www.google.com/search?q=graphql+introduce\&oq=graphql+introduce\&aqs=chrome..69i57j0i433i512l3j69i60l4.1493j0j9\&sourceid=chrome\&ie=UTF-8) primer provided by the GraphQL Community.&#x20;

Proposing a Tag involves executing a [GraphQL Mutation](https://graphql.org/learn/queries/#mutations) against the DataHub GraphQL API, located by default at `http://your-datahub-domain>/api/graphql` . In particular, we'll be using the `proposeTag` Mutation, which has the following interface:&#x20;

```
type Mutation {
    proposeTag(input: TagProposalInput!): String! # Returns Proposal URN.
}

input TagProposalInput {
    resourceUrn: String! # Required. e.g. "urn:li:dataset:(...)"
    subResource: String # Optional. e.g. "fieldName"
    subResourceType: String # Optional. "DATASET_FIELD" for dataset fields
    tagUrn: String! # Required. e.g. "urn:li:tag:Marketing"
}
```

As illustrated above, the `proposeTag` Mutation accepts a single 'input' argument, which in turn contains 3 fields:

1. _resourceUrn_: The URN associated with the target Entity.&#x20;
2. _subResource_: An optional sub-resource identifier.
3. _subResourceType:_ An optional enum indicating what type of subresource is being referenced.
4. &#x20;_tagUrn_: The URN associated with the Tag to propose.&#x20;

{% hint style="info" %}
By default, DataHub deploys with an interactive, web-based API Explorer (GraphiQL)  located at`http://your-datahub-domain/api/graphiql`. Check it out to familiarize yourself with the API.
{% endhint %}

### Adding the Actor Header

Along with the GraphQL payload itself, DataHub's [Metadata Service](https://datahubproject.io/docs/metadata-service/) requires the presence of a special header identifying the caller. Today, the caller identity is self-provided an not validated, something that will change in the near future.&#x20;

For now, you can simply attach a header `X-DataHub-Actor` which contains a "service" URN capable of uniquely identifying a programmatic caller for auditing purposes. In the future, this value will be replaced by an Authorization Token which will be validated by the Metadata Service, and embed the caller identity.&#x20;

```
X-DataHub-Actor: urn:li:service:<service-principal-name>
```

For example, if we're proposing Tags from an internal agent, we may provide the following header value:

```
X-DataHub-Actor: urn:li:service:tag-proposal-agent
```

### Examples

#### CURL

To issue a CURL invoking the `tagProposal` GraphQL Mutation , you can use the following template:

```
curl --location --request POST 'http://your-datahub-domain/api/graphql' \
--header 'Content-Type: application/json' \
--header 'X-DataHub-Actor: urn:li:service:<service-principal-name>' \
--data-raw '{"query":"mutation proposeTag($resourceUrn: String!, $tagUrn: String!) {\n    proposeTag(resourceUrn: $resourceUrn, tagUrn: $tagUrn)","variables":{"resourceUrn":"<resource-urn>","tagUrn":"<tag-urn>"}}'
```

For example, to add the tag "Marketing" to a MySQL Dataset with name "product.sales" in a production environment, I would issue the following query.&#x20;

```
curl --location --request POST 'http://your-datahub-domain/api/graphql' \
--header 'Content-Type: application/json' \
--header 'X-DataHub-Actor: urn:li:service:tag-proposal-agent' \
--data-raw '{"query":"mutation proposeTag($resourceUrn: String!, $tagUrn: String!) {\n    proposeTag(resourceUrn: $resourceUrn, tagUrn: $tagUrn)","variables":{"resourceUrn":"urn:li:dataset:(urn:li:dataPlatform:mysql,product.sales,PROD)","tagUrn":"urn:li:tag:Marketing"}}'
```

### Language Support&#x20;

To issue a GraphQL query programmatically, you can make use of a variety of per-language client libraries. For a full list, see [GraphQL Code Libraries, Tools, & Services](https://graphql.org/code/).&#x20;
