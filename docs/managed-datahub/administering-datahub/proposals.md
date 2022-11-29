---
description: Learn how to propose application of Tags & Glossary Terms in Acryl DataHub.
---

# Proposals

### Prerequisites

To create & manage metadata proposals, certain [access policies](policies-guide.md) are required.&#x20;

#### Privileges for Creating Proposals

To create a new proposal one of these **Metadata** privileges are required

* **Propose Tags** - Allows to propose tags at the Entity level
* **Propose Dataset Column Tags** - Allows to propose tags at the Dataset Field level
* **Propose Glossary Terms** - Allows to propose terms at the Entity level
* **Propose Dataset Column Glossary Terms** - Allows to propose terms at the Dataset Field level

To be able to see the proposals Tab you need the "View Metadata Proposals" PLATFORM privilege

#### Privileges for Managing Proposals

To be able to approve or deny proposals you need one of the following **Metadata** privileges

* Manage Tag Proposals
* Manage Glossary Term Proposals
* Manage Dataset Column Tag Proposals
* Manage Dataset Column Term Proposals

These map directly to the 4 privileges for doing the proposals

### Creating Proposals

When adding metadata the option to Propose will be enabled if the required privileges are there.

![Proposing Tag on a dataset](../imgs/saas/assets/image (8).png)

After someone has proposed a tag or term it will appear on the Dataset as following with a special indicator that the Tag or Term has not yet been applied and is waiting approval.

![Dataset with 1 Tag proposed](../imgs/saas/assets/image (16).png)

### Approving Proposals

For approving these proposals the owners of Dataset can go to `https://<your-account>.acryl.io/requests` or use the `My Requests` Tab at the top of the screen and see the list of all proposals awaiting decision.

![Proposal Approval Screen](../imgs/saas/assets/image (1).png)

Note that in the above screenshot we have two Tabs "Personal" and "Admin". Proposals will appear under the "Personal" tab if you are directly an owner of the dataset. The "Admin" tab corresponds to a group "Admin". If a group is made an owner of a dataset then it will appear under the corresponding group tab for decision.

The Owner can click `Approve & Add` or `Decline` as per their decision. If it is approved then it appears as metadata on the Dataset.

![Dataset with 1 Tag applied through Proposal process](../imgs/saas/assets/image (2).png)

### API

[Tag Proposal API doc](../datahub-api/graphql-api/tag-proposal-api.md#introduction) has details on using the API.
