---
title: Create a Form
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Create a DataHub Compliance Form
<FeatureAvailability/>

This guide will walk you through creating and assigning Compliance Forms, including:

1. Creating a new Compliance Form
2. Building **Questions** for the Compliance Form
3. Assigning **Assets** for the Compliance Form
4. Selecting **Assignees** for the Compliance Form
5. Publishing a Compliance Form

:::note
Managing Compliance Forms via the DataHub UI is only available in DataHub Cloud. If you are using DataHub Core, please refer to the [Compliance Forms API Guide](../../../api/tutorials/forms.md).
:::

### Prerequisites

In order to create, edit, or remove Compliance Forms, you must have the **Manage Compliance Forms** Platform privilege.

### Step 1: Create a new Compliance Form

From the navigation bar, head to **Govern** > **Compliance Forms**. Click **+ Create** to start building your Form.

<p align="center">
  <img 
       width="90%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/compliance_forms/management/list-compliance-forms-before.png"
       alt="View of all Compliance Forms"/>
</p>

First up, provide the following details:

1. **Name:** Select a unique and descriptive name for your Compliance Form that clearly communicates its purpose, such as **"PII Certification Q4 2024"**.

    _**Pro Tip:** This name will be displayed to Assignees when they are assigned tasks, so make it clear and detailed to ensure it conveys the intent of the Form effectively._  

2. **Description:** Craft a concise yet informative description that explains the purpose of the Compliance Form. Include key details such as the importance of the initiative, its objectives, and the expected completion timeline. This helps Assignees understand the context and significance of their role in the process.

    _**Example:** "This Compliance Form is designed to ensure all datasets containing PII are reviewed and verified by Q4 2024. Completing this Form is critical for compliance with organizational and regulatory requirements."_

3. **Type:** Specify the collection type for the Form, based on your compliance requirements:  
    - **Completion:** The Form is considered complete once all required questions are answered for the selected Assets. We recommend this option for basic requirement completion use cases.

    - **Verification:** The Form is considered complete only when all required questions are answered for the selected Assets **and** an Assignee has explicitly "verified" the responses. We recommend this option when final sign-off by Assignees is necessary, ensuring they acknowledge the accuracy and validity of their responses.

4. Next, click **Add Question** to begin building the requirements for your Form.

<p align="center">
  <img
       width="90%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/compliance_forms/management/create-compliance-form-add-question.png"
       alt="Create a new Compliance Form"/>
</p>

### Step 2: Build Questions for your Form

Next, define the Questions for your Compliance Forms. These are used to collect required information about selected assets, and must be completed by an Assignee in order for the Form to be considered complete.

There are 5 different question types to choose from:

* **Ownership:** Request one or more owners to be assigned to selected assets. Optionally restrict responses to a specific set of valid users, groups, and ownership types.
    * _E.g. Who is responsible for ensuring the accuracy of this Dataset?_
* **Domain:** Assign a Domain to the Asset, with the option to predefine the set of allowed Domains.
    * _E.g. Which Domain does this Dashboard belong to? Sales, Marketing, Finance._
* **Documentation:** Provide Documentation about the Asset and/or Column.
    * _E.g. What is the primary use case of this Dataset? What caveats should others be aware of?_
* **Glossary Terms:** Assign one or more Glossary Term to the Asset and/or Column, with the option to predefine the set of allowed Glossary Terms. 
    * _E.g. What types of personally identifiable information (PII) are included in this Asset? Email, Address, SSN, etc._
* **Structured Properties:** Apply custom properties to an Asset and/or Column.
    * _E.g. What date will this Dataset be deprecated and deleted?_

When creating a Question, use a clear and concise Title that is easy for Assignees to understand. In the Description, include additional context or instructions to guide their responses. Both the Title and Description will be visible to Assignees when completing the Form, so make sure to provide any specific hints or details they may need to answer the Question accurately and confidently.

<p align="center">
  <img
       width="50%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/compliance_forms/management/create-compliance-form-prompt.png"
       alt="Create a new Compliance Form prompt"/>
</p>

### Step 3: Assign Assets to your Compliance Form

Now that you have defined the Questions you want Assignees to complete, it's now time to assign the in-scope Assets for this exercise.

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

For example, you can apply filters to focus on all **Snowflake Datasets** that are also associated with the **Finance Domain**. This allows you to break down your compliance initiatives into manageable chunks, so you don't have to go after your entire data ecosystem in one go.

<p align="center">
  <img 
       width="90%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/compliance_forms/management/create-compliance-form-assign-assets.png"
       alt="Assign assets to a Compliance Form"/>
</p>

### Step 4: Select Assignees to complete your Compliance Form

With the Questions and assigned Assets defined, the next step is to select the Assignees—the Users and/or Groups responsible for completing the Form.

In the **Add Recipients** section, decide who is responsible for completing the Form:

* **Asset Owners:** Any User that is assigned to one of the in-scope Assets will be able to complete the Form. This is useful for larger initiatives when you may not know the full set of Users.
* **Specific Users and/or Groups:** Select a specific set of Users and/or Groups within DataHub. This is useful when Ownership of the Assets may be poorly-defined.

<p align="center">
  <img 
       width="90%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/compliance_forms/management/create-compliance-form-add-users-or-groups.png"
       alt="Assign recipients to a Compliance Form"/>
</p>

### Step 5: Publish your Form

After defining the Questions, assigning Assets, and selecting the Assignees, your Form is ready to be published. Once published, Assignees will be notified to complete the Form for the Assets they are responsible for.


To publish a Form, simply click **Publish**.

:::caution
Once you have published a Form, you **cannot** change or add Questions. You can, however, change the set of Assets and/or Assignees for the Form.
:::

Not ready for primetime just yet? No worries! You also have the option to **Save Draft**.

<p align="center">
  <img
       width="80%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/compliance_forms/management/create-compliance-form-publish.png"
       alt="Publish a Compliance Form"/>
</p>

## FAQ and Troubleshooting

**Does answering a Compliance Form Question update the selected Asset?**

Yes! Compliance Forms serve as a powerful tool for gathering and updating key attributes for your mission-critical Data Assets at scale. When a Question is answered, the response directly updates the corresponding attributes of the selected Asset.

**How does a Compliance Form interact with existing metadata?**

If an Asset already has existing metadata that is also referenced in a Form Question, Assignees will have the option to confirm the existing value, overwrite the value, or append additional details.

_You can find more details and examples in the [Complete a Form](complete-a-form.md#understanding-different-form-question-completion-states) guide._

**What is the difference between Completion and Verification Forms?**

Both Form types are a way to configure a set of optional and/or required Questions for DataHub users to complete. When using Verification Forms, users will be presented with a final verification step once all required questions have been completed; you can think of this as a final acknowledgment of the accuracy of information submitted.

**Can I assign multiple Forms to a single Asset?**

You sure can! Please keep in mind that an Asset will only be considered Documented or Verified if all required questions are completed on all assigned Forms.

**How will DataHub Users know that a Compliance Form has been assigned to them?**

They have to check the Inbox on the navigation bar. There are no off-platform notifications for Compliance Forms at this time.

**How do I track the progress of Form completion?**

Great question. We are working on Compliance Forms Analytics that will directly show you the progress of your initiative across the selected Assets. Stay tuned!

### API Tutorials

- [Compliance Form API Guide](../../../api/tutorials/forms.md)

### Related Features

- [DataHub Structured Properties](../../feature-guides/properties/overview.md)

## Next Steps

Now that you have created a DataHub Compliance Form, you're ready to [Complete a Compliance Form](complete-a-form.md).