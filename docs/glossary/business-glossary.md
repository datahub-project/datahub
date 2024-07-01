---
title: Business Glossary
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Business Glossary

<FeatureAvailability/>

## Introduction

When working in complex data ecosystems, it is very useful to organize data assets using a shared vocabulary. The Business Glossary feature in DataHub helps you do this, by providing a framework for defining a standardized set of data concepts and then associating them with the physical assets that exist within your data ecosystem.

Within this document, we'll introduce the core concepts comprising DataHub's Business Glossary feature and show you how to put it to work in your organization. 

### Terms & Term Groups

A Business Glossary is comprised of two important primitives: Terms and Term Groups.

- **Terms**: words or phrases with a specific business definition assigned to them.
- **Term Groups**: act like folders, containing Terms and even other Term Groups to allow for a nested structure.

Both Terms and Term Groups allow you to add documentation and unique owners.

For Glossary Terms, you are also able to establish relationships between different Terms in the **Related Terms** tab. Here you can create Contains and Inherits relationships. Finally, you can view all of the entities that have been tagged with a Term in the **Related Entities** tab.

## Getting to your Glossary

In order to view a Business Glossary, users must have the Platform Privilege called `Manage Glossaries` which can be granted by creating a new Platform [Policy](../authorization/policies.md).

Once granted this privilege, you can access your Glossary by clicking the dropdown at the top of the page called **Govern** and then click **Glossary**:


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/glossary/glossary-button.png"/>
</p>


You are now at the root of your Glossary and should see all Terms and Term Groups with no parents assigned to them. You should also notice a hierarchy navigator on the left where you can easily check out the structure of your Glossary!


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/glossary/root-glossary.png"/>
</p>


## Creating a Term or Term Group

There are two ways to create Terms and Term Groups through the UI. First, you can create directly from the Glossary home page by clicking the menu dots on the top right and selecting your desired option:


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/glossary/root-glossary-create.png"/>
</p>


You can also create Terms or Term Groups directly from a Term Group's page. In order to do that you need to click the menu dots on the top right and select what you want:


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/glossary/create-from-node.png"/>
</p>


Note that the modal that pops up will automatically set the current Term Group you are in as the **Parent**. You can easily change this by selecting the input and navigating through your Glossary to find your desired Term Group. In addition, you could start typing the name of a Term Group to see it appear by searching. You can also leave this input blank in order to create a Term or Term Group with no parent.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/glossary/create-modal.png"/>
</p>


## Editing a Term or Term Group

In order to edit a Term or Term Group, you first need to go the page of the Term or Term group you want to edit. Then simply click the edit icon right next to the name to open up an inline editor. Change the text and it will save when you click outside or hit Enter.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/glossary/edit-term.png"/>
</p>


## Moving a Term or Term Group

Once a Term or Term Group has been created, you can always move it to be under a different Term Group parent. In order to do this, click the menu dots on the top right of either entity and select **Move**.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/glossary/move-term-button.png"/>
</p>


This will open a modal where you can navigate through your Glossary to find your desired Term Group.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/glossary/move-term-modal.png"/>
</p>


## Deleting a Term or Term Group

In order to delete a Term or Term Group, you need to go to the entity page of what you want to delete then click the menu dots on the top right. From here you can select **Delete** followed by confirming through a separate modal. **Note**: at the moment we only support deleting Term Groups that do not have any children. Until cascade deleting is supported, you will have to delete all children first, then delete the Term Group.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/glossary/delete-button.png"/>
</p>


## Adding a Term to an Entity

Once you've defined your Glossary, you can begin attaching terms to data assets. To add a Glossary Term to an asset, go to the entity page of your asset and find the **Add Terms** button on the right sidebar.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/glossary/add-term-to-entity.png"/>
</p>


In the modal that pops up you can select the Term you care about in one of two ways:
- Search for the Term by name in the input
- Navigate through the Glossary dropdown that appears after clicking into the input


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/glossary/add-term-modal.png"/>
</p>


## Privileges

Glossary Terms and Term Groups abide by metadata policies like other entities. However, there are two special privileges provided for configuring privileges within your Business Glossary.

- **Manage Direct Glossary Children**: If a user has this privilege on a Glossary Term Group, they will be able to create, edit, and delete Terms and Term Groups directly underneath the Term Group they have this privilege on.
- **Manage All Glossary Children**: If a user has this privilege on a Glossary Term Group, they will be able to create, edit, and delete any Term or Term Group anywhere underneath the Term Group they have this privilege on. This applies to the children of a child Term Group as well (and so on).

## Shift left and bring your glossary into Git

You can use this [Github Action](https://github.com/acryldata/business-glossary-sync-action) and bring your Business Glossary into your git repositories. This can be the starting point to manage glossary in git.

## Managing Glossary with Git 

In many cases, it may be preferable to manage the Business Glossary in a version-control system like git. This can make
managing changes across teams easier, by funneling all changes through a change management and review process.

To manage your glossary using Git, you can define it within a file and then use the DataHub CLI to ingest
it into DataHub whenever a change is made (e.g. on a `git commit` hook). For detailed information about the format of
the glossary file, and how to ingest it into DataHub, check out the [Business Glossary](../generated/ingestion/sources/business-glossary.md) source guide.

## About Glossary Term Relationships

DataHub supports 2 different kinds of relationships _between_ individual Glossary Terms: **Inherits From** and **Contains**. 

**Contains** can be used to relate two Glossary Terms when one is a _superset_ of or _consists_ of another.
For example: **Address** Term _Contains_ **Zip Code** Term, **Street** Term, & **City** Term (_Has-A_ style relationship)

**Inherits** can be used to relate two Glossary Terms when one is a _sub-type_ or _sub-category_ of another.
For example: **Email** Term _Inherits From_  **PII** Term (_Is-A_ style relationship)

These relationship types allow you to map the concepts existing within your organization, enabling you to
change the mapping between concepts behind the scenes, without needing to change the Glossary Terms
that are attached to individual Data Assets and Columns. 

For example, you can define a very specific, concrete Glossary Term like `Email Address` to represent a physical
data type, and then associate this with a higher-level `PII` Glossary Term via an `Inheritance` relationship. 
This allows you to easily maintain a set of all Data Assets that contain or process `PII`, while keeping it easy to add 
and remove new Terms from the `PII` Classification, e.g. without requiring re-annotation of individual Data Assets or Columns.




## Demo

Check out [our demo site](https://demo.datahubproject.io/glossary) to see an example Glossary and how it works!

### GraphQL

* [addTerm](../../graphql/mutations.md#addterm)
* [addTerms](../../graphql/mutations.md#addterms)
* [batchAddTerms](../../graphql/mutations.md#batchaddterms)
* [removeTerm](../../graphql/mutations.md#removeterm)
* [batchRemoveTerms](../../graphql/mutations.md#batchremoveterms)
* [createGlossaryTerm](../../graphql/mutations.md#createglossaryterm)
* [createGlossaryNode](../../graphql/mutations.md#createglossarynode) (Term Group)

You can easily fetch the Glossary Terms for an entity with a given its URN using the **glossaryTerms** property. Check out [Working with Metadata Entities](../api/graphql/how-to-set-up-graphql.md#querying-for-glossary-terms-of-an-asset) for an example.

## Resources
- [Creating a Business Glossary and Putting it to use in DataHub](https://blog.datahubproject.io/creating-a-business-glossary-and-putting-it-to-use-in-datahub-43a088323c12)
- [Tags and Terms: Two Powerful DataHub Features, Used in Two Different Scenarios](https://medium.com/datahub-project/tags-and-terms-two-powerful-datahub-features-used-in-two-different-scenarios-b5b4791e892e)

## Feedback / Questions / Concerns

We want to hear from you! For any inquiries, including Feedback, Questions, or Concerns, reach out on Slack!
