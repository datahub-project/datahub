---
title: Create a Form
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Create a DataHub Compliance Form
<FeatureAvailability/>

## Creating and Assigning Compliance Forms

The following steps will walk you through creating and assigning Compliance Forms, including:

1. Defining your Compliance Form
2. Creating Questions to be completed by assignees
3. Selecting the in-scope Assets for the Compliance Form
4. Assigning Forms to specific Users
5. Publish your Form

:::note
Creating and managing Compliance Forms via the UI is only available in DataHub Cloud. If you are deployed with DataHub Core, please see the [Compliance Forms API Guide](../../../docs/api/tutorials/forms.md).
:::

### Step 1: Define your Complinace Form

From the navigation bar, head to **Govern** > **Compliance Forms**. Click **+ Create** to start building your Form.

<p align="center">
  <img width="90%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/compliance_forms/management/list-compliance-forms-before.png"/>
</p>

First up, provide the following details:

1. **Name:** Give your Compliance Form a unique name.
2. **Description:** Describe the purpose of the Form to help your users understand the reason of the exercise.
3. **Type:** Determine the collection type of the Form:
    - **Verification:** Collect required information and require final verification to complete the Form.
    - **Completion:** Collect required information; final verification is not required.
4. Click **Add Question** to begin setting the requirements for your Form.   

<p align="center">
  <img width="90%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/compliance_forms/management/create-compliance-form-add-question.png"/>
</p>

### Step 2: Add Questions to your Form

Next, create Questions you want your users to complete to capture the desired metadata with this Compliance Form. There are five types of Questions that can be created, each of which can be set to be **require response**:

* **Ownership:** Assign one or more Owners to the Asset, with the option to pre-define the set of allowed Owners and/or Ownership Types.
    * _E.g. Who is responsible for ensuring the accuracy of this Dataset?_
* **Domain:** Assign a Domain to the Asset, with the option to pre-define the set of allowed Domains.
    * _E.g. Which Domain does this Dashboard belong to? Sales, Marketing, Finance._
* **Documentation:** Provide Documentation about the Asset and/or Column.
    * _E.g. What is the primary use case of this Dataset? What caveats should others be aware of?_
* **Glossary Terms:** Assign one or more Glossary Term to the Asset and/or Column, with the option to pre-define the set of allowed Glossary Terms. 
    * _E.g. What types of personally identifiable information (PII) are included in this Asset? Email, Address, SSN, etc._
* **Structured Properties:** Apply custom properties to an Asset and/or Column.
    * _E.g. What date will this Dataset be deprecated and deleted?_

When creating a Question, be sure to give it an easy-to-understand Title, and provide additional context or direction in the Description.
<p align="center">
  <img width="90%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/compliance_forms/management/create-compliance-form-prompt.png"/>
</p>

### Step 3: Assign your Form to relevant Assets

Now that you have defined the Questions you want Users to complete, it's now time to select the in-scope Assets for this exercise.

In the **Assign Assets** section, you can easily target the specific set of Assets that are relevant for this Form with the following steps:

1. Add a Condition or Group of Conditions
2. Choose the appropriate filter type, such as:
    * Asset Type (Dataset, Chart, etc.)
    * Platform (Snowflake, dbt, etc.)
    * Domain (Sales, Marketing, Finance, etc.)
    * Assigned Owners
    * Assigned Glossary Terms
3. Decide between **All**, **Any**, or **None** of the filters should apply
4. Preview the relevant Assets to confirm you have applied the appropriate filters

<p align="center">
  <img width="90%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/compliance_forms/management/create-compliance-form-assign-assets.png"/>
</p>

### Step 4: Add Recipients to your Form

Now that you have defined the set of Questions to be answered for a set of Assets, it's now time to delegate out to your Users.

In the **Add Recipients** section, decide who is responsible for completing the Form:

* **Asset Owners:** Any User that is assigned to one of the in-scope Assets will be able to complete the Form. This is useful for larger initiatives when you may not know the full set of Users.
* **Specific Users and/or Groups:** Select a specific set of Users and/or Groups within DataHub. This is useful when Ownership of the Assets may be poorly-defined.

<p align="center">
  <img width="90%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/compliance_forms/management/create-compliance-form-add-users-or-groups.png"/>
</p>

### Step 5: Publish your Form

Once you have defined the set of Questions to be completed, the in-scope Assets, and the relevant Recipients, it's now time to publish your Form!

:::caution
Once you have published a Form, you **cannot** change or add Questions.
:::

<p align="center">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/compliance_forms/management/create-compliance-form-publish.png"/>
</p>

## FAQ and Troubleshooting

**What is the difference between Completion and Verification Forms?**

Both form types are a way to configure a set of optional and/or required questions for DataHub users to complete. When using Verification Forms, users will be presented with a final verification step once all required questions have been completed; you can think of this as a final acknowledgement of the accuracy of information submitted.

**Can I assign multiple Forms to a single asset?**

You sure can! Please keep in mind that an Asset will only be considered Documented or Verified if all required questions are completed on all assigned Forms.

### API Tutorials

- [API Guides on Documentation Form](../../../docs/api/tutorials/forms.md)

:::note
You must create a Structured Property before including it in a Documentation Form.
To learn more about creating Structured Properties via CLI, please see the [Create Structured Properties](/docs/api/tutorials/structured-properties.md) tutorial.
:::

### Related Features

- [DataHub Properties](/docs/features/feature-guides/properties.md)