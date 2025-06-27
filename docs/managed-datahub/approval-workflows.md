import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Change Proposals & Approval Workflows

<FeatureAvailability saasOnly />

## Overview

Keeping your data organized can be hard work when you have a limited number of data owners. With DataHub Cloud, you can crowdsource metadata completion using change proposals. Change Proposals enable data users to suggest Tags, Terms, Domains, Owners, Descriptions, and even Structured Properties to be added to data assets. Once a change proposal is raised, data owners and stewards can review change proposals, approving or denying these suggestion.

## Permissions

### Creating Proposals

To create proposals on assets, users need to have the following resource privileges:

- `Propose Tags`
- `Propose Glossary Terms`
- `Propose Owners`
- `Propose Domain`
- `Propose Properties`
- `Propose Description`
- `Propose Dataset Column Tags`
- `Propose Dataset Column Glossary Terms`
- `Propose Dataset Column Structured Properties`
- `Propose Dataset Column Descriptions`

To create proposals to change the Business Glossary, users need to have the following platform privileges:

- `Propose Create Glossary Term`
- `Propose Create Glossary Node (Term Group)`

Which are granted by default using the **Reader**, **Editor**, and **Admin** roles by default.

### Reviewing Proposals

To review proposals for an asset, users need to have the following resource privileges:

- `Manage Tag Proposals`
- `Manage Glossary Term Proposals`
- `Manage Property Proposals`
- `Manage Domain Proposals`
- `Manage Owner Proposals`
- `Manage Description Proposals`

To review proposals to change the Business Glossary, users need to have the following platform privileges:

- `Manage Glossaries`

Which are granted by default **Editor** and **Admin** roles.

## Using Approval Workflows

### Proposing Tags and Glossary Terms

1. When adding a Tag or Glossary Term to a column or entity, you will see a propose button.

<p align="left">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/proposals/propose_term_on_dataset.png"/>
</p>

2. After proposing the Glossary Term, you will see it appear in a proposed state.

<p align="left">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/proposals/proposed_term_on_dataset.png"/>
</p>

3. This proposal will be sent to the inbox of reviewers.

<p align="left">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/proposals/proposed_term_to_dataset_in_inbox.png"/>
</p>

4. From there, they can choose to either accept or reject the proposal. A full log of all accepted or rejected proposals is kept for each user.

### Proposing Owners

1. When adding an Owner to an entity, you will see a propose button.

<p align="left">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/proposals/propose_owner_on_dataset.png"/>
</p>

2. After proposing the Owner(s), you will see the owner(s) appear in a proposed state.

<p align="left">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/proposals/proposed_owner_on_dataset.png"/>
</p>

3. This proposal will be sent to the inbox of reviewers.

<p align="left">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/proposals/proposed_owner_to_dataset_in_inbox.png"/>
</p>

4. From there, they can choose to either accept or reject the proposal. A full log of all accepted or rejected proposals is kept for each user.

### Proposing Domain

1. When adding a Domain to an entity, you will see a propose button.

<p align="left">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/proposals/propose_domain_on_dataset.png"/>
</p>

2. After proposing the Domain, you will see the Domain appear in a proposed state.

<p align="left">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/proposals/proposed_domain_on_dataset.png"/>
</p>

3. This proposal will be sent to the inbox of reviewers.

<p align="left">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/proposals/proposed_domain_to_dataset_in_inbox.png"/>
</p>

4. From there, they can choose to either accept or reject the proposal. A full log of all accepted or rejected proposals is kept for each user.

### Proposing Structured Properties

1. When adding a Structured property to a column or an entity, you will see a propose button.

<p align="left">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/proposals/propose_property_on_dataset.png"/>
</p>

2. After proposing the Structured Properties, you will see them appear in a proposed state.

<p align="left">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/proposals/proposed_property_on_dataset.png"/>
</p>

3. This proposal will be sent to the inbox of reviewers.

<p align="left">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/proposals/proposed_property_to_dataset_in_inbox.png"/>
</p>

4. From there, they can choose to either accept or reject the proposal. A full log of all accepted or rejected proposals is kept for each user.

### Proposing Documentation or Description Updates

1. When updating the documentation of any entity, or description of a dataset column, you can click the propose button

2. This proposal will be sent to the inbox of reviewers.

<p align="left">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/proposals/inbox_with_description_proposal.png"/>
</p>

3. From there, they can choose to either accept or reject the proposal.

### Proposing additions to your Business Glossary

1. Navigate to your glossary by going to the Govern menu in the top right and selecting Glossary.

2. Click the plus button to create a new Glossary Term. From that menu, select Propose.

<p align="left">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/proposals/proposing_new_glossary_term.png"/>
</p>

3. This proposal will be sent to the inbox of reviewers.

<p align="left">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/proposals/inbox_with_new_glossary_proposal.png"/>
</p>

4. From there, they can choose to either accept or reject the proposal. A full log of all accepted or rejected proposals is kept for each user.

### Reviewing Proposals

Proposals will be visible inside your **Task Center**, which is accessible via the navigation sidebar. From the task center, you can choose to accept or deny proposals sourced for assets you are responsible for.

## Change Proposal Notifications

You can enable notifications in the following scenarios:

- A proposal you raised is approved or denied
- You are assigned to a new change proposal
- A proposal you are assigned to is approved or denied

Via **Slack** and **Email**.

To enable notifications, navigate to **Settings > My Notifications**.

## Creating Proposals via API

DataHub exposes a GraphQL API for each type of change proposal. At a high level, callers of this API will be required to provide the following details:

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

## FAQ

**1. My colleagues have created some proposals, but I'm not seeing this in my Task Center. Why not?**

Most likely, this means your privileges are not configured properly. If you are a DataHub Admin, navigate to **Settings > Permissions** to edit your roles and policies to ensure you have the privileges listed in the **Permissions** section above.
