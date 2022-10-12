import FeatureAvailability from '@site/src/components/FeatureAvailability';

# About DataHub Approval Workflows

<FeatureAvailability saasOnly />

## Overview

Keeping all your metadata properly classified can be hard work when you only have a limited number of trusted data stewards. With Managed DataHub, you can source proposals of Tags and Glossary Terms associated to datasets or dataset columns. These proposals may come from users with limited context or programatic processes using hueristics. Then, data stewards and data owners can go through them and only approve proposals they consider correct. This reduces the burden of your stewards and owners while increasing coverage.

Approval workflows also cover the Business Glossary itself. This allows you to source Glossary Terms and Glossary Term description changes from across your organization while limiting who has final control over what gets in.

## Using Approval Workflows

### Proposing Tags and Glossary Terms

1. When adding a Tag or Glossary Term to a column or entity, you will see a propose button.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/propose_term_on_dataset.png"/>
</p>

2. After proposing the Glossary Term, you will see it appear in a proposed state.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/propsed_term_on_dataset.png"/>
</p>

3. This proposal will be sent to the inbox of Admins with proposal permissions and data owners.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/proposed_term_to_dataset_in_inbox.png"/>
</p>

4. From there, they can choose to either accept or reject the proposal. A full log of all accepted or rejected proposals is kept for each user.

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

### Proposing Glossary Term Description Updates

1. When updating the description of a Glossary Term, click propse after making your change.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/propose_glossary_description_change.png"/>
</p>

2. This proposal will be sent to the inbox of Admins with proposal permissions and data owners.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/inbox_with_description_proposal.png"/>
</p>

3. From there, they can choose to either accept or reject the proposal.

## Proposing Programatically

DataHub exposes a GraphQL API for proposing Tags and Glossary Terms.

At a high level, callers of this API will be required to provide the following details:

1. A unique identifier for the target Metadata Entity (URN)
2. An optional sub-resource identifier which designates a sub-resource to attach the Tag or Glossary Term to. for example reference to a particular "field" within a Dataset.
3. A unique identifier for the Tag or Glossary Term they wish to propose (URN)

In the following sections, we will describe how to construct each of these items and use the DataHub GraphQL API to submit Tag or Glossary Term proposals.

#### Constructing an Entity Identifier

Inside DataHub, each Metadata Entity is uniquely identified by a Universal Resource Name, or an URN. This identifier can be copied from the entity page, extracted from the API, or read from a downloaded search result. You can also use the helper methods in the datahub python library given a set of components.

#### Constructing a Sub-Resource Identifier

Specific Metadata Entity types have additional sub-resources to which Tags may be applied.
Today, this only applies for Dataset Metadata Entities, which have a "fields" sub-resource. In this case, the `subResource` value would be the field path for the schema field.

#### Finding a Tag or Glossary Term Identifier

Tags and Glossary Terms are also uniquely identified by an URN.

Tag URNs have the following format:
`urn:li:tag:<id>`

Glossary Term URNs have the following format:
`urn:li:glossaryTerm:<id>`

These full identifiers can be copied from the entity pages of the Tag or Glossary Term.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/copy_urn_from_entity.png"/>
</p>

#### Issuing a GraphQL Query

Once we've constructed an Entity URN, any relevant sub-resource identifiers, and a Tag or Term URN, we're ready to propose! To do so, we'll use the DataHub GraphQL API.

In particular, we'll be using the proposeTag, proposeGlossaryTerm, and proposeUpdateDescription Mutations, which have the following interface:

```
type Mutation {
proposeTerm(input: TermAssociationInput!): String! # Returns Proposal URN.
}

input TermAssociationInput {
	resourceUrn: String! # Required. e.g. "urn:li:dataset:(...)"
	subResource: String # Optional. e.g. "fieldName"
	subResourceType: String # Optional. "DATASET_FIELD" for dataset fields
	term: String! # Required. e.g. "urn:li:tag:Marketing"
}
```

```
type Mutation {
proposeTag(input: TagAssociationInput!): String! # Returns Proposal URN.
}

input TagAssociationInput {
	resourceUrn: String! # Required. e.g. "urn:li:dataset:(...)" subResource: String # Optional. e.g. "fieldName"
	subResourceType: String # Optional. "DATASET_FIELD" for dataset fields
	tagUrn: String! # Required. e.g. "urn:li:tag:Marketing"
}
```

```
mutation proposeUpdateDescription($input: DescriptionUpdateInput!) {
    proposeUpdateDescription(input: $input)
}

"""
Currently supports updates to Glossary Term descriptions only
"""
input DescriptionUpdateInput {
    description: String! # the new description

    resourceUrn: String!

    subResourceType: SubResourceType

    subResource: String
}
```

## Additional Resources

### Permissions

To create & manage metadata proposals, certain access policies or roles are required.

#### Privileges for Creating Proposals

To create a new proposal one of these Metadata privileges are required. All roles have these priveleges by default.

- Propose Tags - Allows to propose tags at the Entity level
- Propose Dataset Column Tags - Allows to propose tags at the Dataset Field level
- Propose Glossary Terms - Allows to propose terms at the Entity level
- Propose Dataset Column Glossary Terms - Allows to propose terms at the Dataset Field level

To be able to see the Proposals Tab you need the <strong>"View Metadata Proposals"</strong> PLATFORM privilege

#### Privileges for Managing Proposals

To be able to approve or deny proposals you need one of the following Metadata privileges. `Admin` and `Editor` roles already have these by default.

- Manage Tag Proposals
- Manage Glossary Term Proposals
- Manage Dataset Column Tag Proposals
- Manage Dataset Column Term Proposals

These map directly to the 4 privileges for doing the proposals.

To be able to approve or deny proposals to the glossary itself, you just need one permission:
- Manage Glossaries

### Videos

<p align="center">
<iframe width="560" height="315" src="https://www.loom.com/embed/1e450bde6b544653bbbf7f9762d1b157" title="Approval Workflows" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>
</p>
