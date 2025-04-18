---
title: Complete a Form
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Complete a DataHub Compliance Form
<FeatureAvailability/>

Once a Compliance Form has been published (see [Create a Compliance Form](create-a-form.md)), Assignees will receive notifications in their Task Center prompting them to complete the Form for each Asset they are responsible for.

This guide provides an example of completing a Compliance Form, covering:

1. Accessing a Form from an Asset Page or the Task Center
2. Completing a Form for a single Asset or multiple Assets (DataHub Cloud only)
3. Understanding different Form Question completion states

The example uses the **Governance Initiative 2024**, a Verification Form with 3 Required Questions:

<p align="center">
  <img
       width="90%"
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/compliance_forms/sample-compliance-form.png" 
       alt="Sample Compliance Form"/>
</p>

## Access a Compliance Form

Once you have been assigned to complete a Compliance Form, you will see a **Complete Documentation** or **Complete Verification** option on the right-hand side of an Asset Page:

<p align="center">
  <img
       width="70%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/compliance_forms/completion/complete-task-from-asset-page.png"
       alt="Open Compliance Form from Asset Page"/>
</p>

**DataHub Cloud** users can find all outstanding Compliance Form requests by navigating to the **Task Center**:

<p align="center">
  <img
       width="80%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/compliance_forms/completion/complete-tasks-from-task-center.png"
       alt="Open Compliance Form from Task Center"/>
</p>

## Complete a Form for a Single Asset

When filling out a Compliance Form for a single Asset, you'll see a list of Questions tailored to that Asset, with clear labels showing which ones are required. Here's how it works:

- **Question Details:** Each Question specifies if it's required or optional. Required Questions must be completed to submit the Form.
- **Pre-Populated Metadata:** If metadata already exists for a Question, it will appear pre-filled. You can confirm the existing value or make updates as needed.
- **Assignee Contributions:** If another Assignee has already provided a response, their name and the time of submission will be displayed. This gives you visibility into previous input, though you can still update the response.

:::tip
For Verification Forms, after addressing all required Questions, you'll be prompted to provide final sign-off. This ensures all responses are complete and accurate, marking the Form ready for submission.
:::

Once you complete all required responses, the sidebar will update with the status of the Asset:

- **Documented**: All required Questions are completed, Verification is not needed
- **Verified**: All required Questions are completed and Verified

Here's what the **Governance Initiative 2024** Verification Form looks like for `dogs_in_movies` after responding to all Required Questions:

<p align="center">
  <img
       width="80%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/compliance_forms/completion/complete-task-ready-to-verify.png"
       alt="Asset Ready to Verify"/>
</p>

And here's the `dogs_in_movies` sidebar after Verifying all responses:

<p align="center">
  <img
       width="70%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/compliance_forms/completion/complete-task-asset-verified.png"
       alt="Asset is Verified"/>
</p>

### Navigate to the Next Asset

To continue working through the Compliance Forms assigned to you, **use the navigation arrows located in the top-right corner**. These arrows will take you to the next Asset that is still pending Form completion or Verification. Only Assets that require action will appear in this flow, allowing you to focus on the remaining tasks without unnecessary steps.

## Complete a Form Question for Multiple Assets

When you want to provide the same response for a question to multiple assets, you can apply it in bulk by selecting the **By Question** option in the top-right corner. This allows you to navigate through the Form question-by-question and apply the same response to multiple assets.

:::note
Completing Form Questions for multiple Assets is only supported for DataHub Cloud.
:::

### Example: Apply a Response in Bulk

Let's look at an example. Imagine we are trying to provide the same answer to a Question for all Assets in a Snowflake schema called `DEMO_DB`. Here's how we'd do it:

1. **Filter Assets**: Filter down to all datasets in the `DEMO_DB` Snowflake schema.
2. **Set a Response**: For the selected Question, provide a response. In this case, we'll set the Deletion Date to be `2024-12-31`.
3. **Apply to All Selected Assets**: Use the bulk application feature to apply this response to all filtered Assets.

<p align="center">
  <img
       width="90%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/compliance_forms/completion/complete-tasks-by-question.png"
       alt="Apply Response to Multiple Assets"/>
</p>

After setting the response, toggle through each Question, providing the necessary responses to combinations of Assets.

### Verification for Multiple Assets

For Verification Forms, as you complete Questions, you will see the number of assets eligible for Verification in the top-right corner. This makes it easy to track which Assets have met the requirements.

<p align="center">
  <img
       width="70%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/compliance_forms/completion/complete-tasks-by-question-ready-to-verify.png"
       alt="Multiple Assets ready to Verify"/>
</p>

When you are ready to bulk Verify Assets, you will be prompted to confirm that all responses are complete and accurate before proceeding.

<p align="center">
  <img
       width="70%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/compliance_forms/completion/complete-tasks-bulk-verify.png"
       alt="Final Bulk Verification"/>
</p>

### Switch Between Completion Modes

You can easily toggle between the **Complete By Asset** and **Complete By Question** views as needed, ensuring flexibility while completing and verifying the Compliance Forms.

## Understanding Different Form Question Completion States

When completing a Compliance Form, you may encounter various types of Questions, each with unique completion states based on existing metadata or prior responses from other Assignees. This section highlights examples of various completion states to help you understand how Questions can be answered, confirmed, or updated when completing a Form.

**_1. What is the primary use case for this asset?_**

This required Question is asking the Assignee to provide Documentation on how the Asset should be used. Note that there is no text populated in the description, meaning the Asset does not have any documentation at all.

<p align="center">
  <img
       width="90%"
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/compliance_forms/completion/complete-task-incomplete-question.png"
       alt="Sample Compliance Form"/>
</p>

**_2. When will this asset be deleted?_**

You may notice that this question has a pre-populated value. When metadata has been populated from a source _outside_ of a Form, users will have the option to update and save the value, or, simply **Confirm** that the value is accurate.

<p align="center">
  <img
       width="90%"
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/compliance_forms/completion/complete-task-confirm-value.png"
       alt="Sample Compliance Form"/>
</p>

**_3. Who is the Data Steward of this Asset?_**

Here's an example where a different Form Assignee has already provided an answer through the Compliance Form 3 days ago. All Assignees will still have the option to update the response, but this allows users to see how other Form Assignees have already answered the questions.

<p align="center">
  <img
       width="90%"
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/compliance_forms/completion/complete-task-question-answered.png"
       alt="Sample Compliance Form"/>
</p>


## FAQ and Troubleshooting

**Why don’t I see any Compliance Forms in the Task Center or on an Asset Page?**

If you don’t see any Compliance Forms, check with the Form author to ensure your DataHub user account has been assigned to complete a Form for one or more Assets. Forms can be assigned to Asset Owners, specific DataHub Users, or a combination of both.