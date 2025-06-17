import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Approval Workflows

<FeatureAvailability saasOnly />

## Overview

Keeping all your metadata properly classified can be hard work when you only have a limited number of trusted data stewards. With DataHub Cloud, you can source proposals of Tags and Glossary Terms associated to datasets or dataset columns. These proposals may come from users with limited context or programatic processes using hueristics. Then, data stewards and data owners can go through them and only approve proposals they consider correct. This reduces the burden of your stewards and owners while increasing coverage.

Approval workflows also cover the Business Glossary itself. This allows you to source Glossary Terms and Glossary Term description changes from across your organization while limiting who has final control over what gets in.

## Using Approval Workflows

**Note:** You can select and propose multiple Tags, Glossary Terms, Owners, or Structure Properties at once. The workflow process remains the same whether proposing single or multiple items.

### Proposing Tags and Glossary Terms

1. When adding a Tag or Glossary Term to a column or entity, you will see a propose button.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/propose_term_on_dataset.png"/>
</p>

2. After proposing the Glossary Term, you will see it appear in a proposed state.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/proposed_term_on_dataset.png"/>
</p>

3. This proposal will be sent to the inbox of Admins with proposal permissions and data owners.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/proposed_term_to_dataset_in_inbox.png"/>
</p>

4. From there, they can choose to either accept or reject the proposal. A full log of all accepted or rejected proposals is kept for each user.

### Proposing Owners

1. When adding an Owner to an entity, you will see a propose button.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/propose_owner_on_dataset.png"/>
</p>

2. After proposing the Owner(s), you will see the owner(s) appear in a proposed state.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/proposed_owner_on_dataset.png"/>
</p>

3. This proposal will be sent to the inbox of Admins with proposal permissions and data owners.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/proposed_owner_to_dataset_in_inbox.png"/>
</p>

4. From there, they can choose to either accept or reject the proposal. A full log of all accepted or rejected proposals is kept for each user.

### Proposing Domain

1. When adding a Domain to an entity, you will see a propose button.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/propose_domain_on_dataset.png"/>
</p>

2. After proposing the Domain, you will see the Domain appear in a proposed state.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/proposed_domain_on_dataset.png"/>
</p>

3. This proposal will be sent to the inbox of Admins with proposal permissions and data owners.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/proposed_domain_to_dataset_in_inbox.png"/>
</p>

4. From there, they can choose to either accept or reject the proposal. A full log of all accepted or rejected proposals is kept for each user.

### Proposing Structured Properties

1. When adding a Structured property to a column or an entity, you will see a propose button.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/propose_property_on_dataset.png"/>
</p>

2. After proposing the Structured Properties, you will see them appear in a proposed state.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/proposed_property_on_dataset.png"/>
</p>

3. This proposal will be sent to the inbox of Admins with proposal permissions and data owners.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/proposed_property_to_dataset_in_inbox.png"/>
</p>

4. From there, they can choose to either accept or reject the proposal. A full log of all accepted or rejected proposals is kept for each user.

### Proposing Documentation or Description Updates

1. When updating the documentation of any entity, or description of a dataset column, you can click the propose button

2. This proposal will be sent to the inbox of Admins with proposal permissions and data owners.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/inbox_with_description_proposal.png"/>
</p>

3. From there, they can choose to either accept or reject the proposal.

### Proposing additions to your Business Glossary

1. Navigate to your glossary by going to the Govern menu in the top right and selecting Glossary.

2. Click the plus button to create a new Glossary Term. From that menu, select Propose.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/proposing_new_glossary_term.png"/>
</p>

3. This proposal will be sent to the inbox of Admins with proposal permissions and data owners.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/inbox_with_new_glossary_proposal.png"/>
</p>

4. From there, they can choose to either accept or reject the proposal. A full log of all accepted or rejected proposals is kept for each user.

## Proposing Programatically

DataHub exposes a GraphQL API for proposing Tags and Glossary Terms.

At a high level, callers of this API will be required to provide the following details:

1. A unique identifier for the target Metadata Entity (URN)
2. An optional sub-resource identifier which designates a sub-resource to attach the Tag, Glossary Term, owner, domain or Structured property to. For example reference to a particular "field" within a Dataset.
3. A unique identifier for the Tag/Glossary Term/Owner/Domain/Structured property they wish to propose (URN)

In the following sections, we will describe how to construct each of these items and use the DataHub GraphQL API to submit Tag or Glossary Term proposals.

#### Constructing an Entity Identifier

Inside DataHub, each Metadata Entity is uniquely identified by a Universal Resource Name, or an URN. This identifier can be copied from the entity page, extracted from the API, or read from a downloaded search result. You can also use the helper methods in the datahub python library given a set of components.

#### Constructing a Sub-Resource Identifier

Specific Metadata Entity types have additional sub-resources to which Tags may be applied.
Today, this only applies for Dataset Metadata Entities, which have a "fields" sub-resource. In this case, the `subResource` value would be the field path for the schema field.

#### Finding an Identifier for Tag/Glossary Term/Owner/Domain/Structure property

All of these are uniquely identified by an URN.

Tag URNs have the following format:
`urn:li:tag:<id>`

Glossary Term URNs have the following format:
`urn:li:glossaryTerm:<id>`

Domain URNs have the following format:
`urn:li:domain:<id>`

Owner URNs have the following format:
`urn:li:corpuser:<id>` or `urn:li:corpGroup:<id>`

Structured Property URNs have the following format:
`urn:li:structuredProperty:<id>`

These identifiers can be copied from the url of the corresponding entity pages.

#### Issuing a GraphQL Query

Once we've constructed an Entity URN, any relevant sub-resource identifiers, we're ready to propose! To do so, we'll use the DataHub GraphQL API.

In particular, we'll be using the proposeTags, proposeTerms, proposeDomain, proposeOwners, proposeStructuredProperties, proposeCreateGlossaryTerm, proposeCreateGlossaryNode, proposeDataContract, and proposeUpdateDescription Mutations, which have the following interface:

```
type Mutation {
  proposeTags(input: ProposeTagsInput!): String! # Returns Proposal URN.
}

input ProposeTagsInput {
  description: String # Optional note explaining the proposal
  resourceUrn: String! # Required. e.g. "urn:li:dataset:(...)"
	subResource: String # Optional. e.g. "fieldName"
	subResourceType: String # Optional. "DATASET_FIELD" for dataset fields
	tagUrns: [String!]! # Required. e.g. ["urn:li:tag:Marketing"]
}
```

```
type Mutation {
  proposeTerms(input: ProposeTermsInput!): String! # Returns Proposal URN.
}

input ProposeTermsInput {
	description: String # Optional note explaining the proposal
  resourceUrn: String! # Required. e.g. "urn:li:dataset:(...)"
	subResource: String # Optional. e.g. "fieldName"
	subResourceType: String # Optional. "DATASET_FIELD" for dataset fields
	termUrns: [String!]! # Required. e.g. ["urn:li:glossaryTerm:Marketing"]
}
```

```
type Mutation {
  proposeDomain(input: ProposeDomainInput!): String! # Returns Proposal URN.
}

input ProposeDomainInput {
	description: String # Optional note explaining the proposal
  resourceUrn: String! # Required. e.g. "urn:li:dataset:(...)"
	domainUrn: String! # Required. e.g. ["urn:li:domain:Marketing"]
}
```

```
type Mutation {
  proposeOwners(input: ProposeOwnersInput!): String! # Returns Proposal URN.
}

input ProposeOwnersInput {
	description: String # Optional note explaining the proposal
  resourceUrn: String! # Required. e.g. "urn:li:dataset:(...)"
	owners: [OwnerInput!]! # Required
}

input OwnerInput {
  ownerUrn: String! # Required. e.g. "urn:li:owner:(...)"
  ownerEntityType: OwnerEntityType! # Required. e.g. "CORP_USER"
  type: OwnershipType # Optional
  ownershipTypeUrn: String # Optional. The urn of the ownership type entity.
}
```

```
type Mutation {
  proposeStructuredProperties(input: ProposeStructuredPropertiesInput!): String! # Returns Proposal URN.
}

input ProposeStructuredPropertiesInput {
	description: String # Optional note explaining the proposal
  resourceUrn: String! # Required. e.g. "urn:li:dataset:(...)"
	subResource: String # Optional. e.g. "fieldName"
	subResourceType: String # Optional. "DATASET_FIELD" for dataset fields
	structuredProperties: [StructuredPropertyInputParams!]!
}

input StructuredPropertyInputParams {
  structuredPropertyUrn: String! # Required. e.g. "urn:li:structuredProperty:(...)"
  values: [PropertyValueInput!]! # Required. e.g. "{ stringValue: ''}"
}
```

```
type Mutation {
  proposeCreateGlossaryTerm(input: CreateGlossaryEntityInput!): Boolean
}

input CreateGlossaryEntityInput {
  id: String # Optional. Otherwise uuid is generated
  name: String! # Required. e.g. "Marketing"
  description: String # Optional
  parentNode: String # Optional. e.g. "urn:li:glossaryNode:(...)"
  proposalNote: String # Optional. Context for the proposal
}
```

```
type Mutation {
  proposeCreateGlossaryNode(input: CreateGlossaryEntityInput!): Boolean
}

input CreateGlossaryEntityInput {
  id: String # Optional. Otherwise uuid is generated
  name: String! # Required. e.g. "Marketing"
  description: String # Optional
  parentNode: String # Optional. e.g. "urn:li:glossaryNode:(...)"
  proposalNote: String # Optional. Context for the proposal
}
```

```
mutation proposeUpdateDescription($input: DescriptionUpdateInput!) {
proposeUpdateDescription(input: $input)
}

"""
Currently supports DatasetField descriptions only
"""
input DescriptionUpdateInput {
description: String! # the new description
resourceUrn: String!
subResourceType: SubResourceType
subResource: String
proposalNote: String # Context for the proposal
}

```

## Additional Resources

### Permissions

To create & manage metadata proposals, certain access policies or roles are required.

https://docs.datahub.com/docs/authorization/policies#proposals
