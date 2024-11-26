---
title: Complete a Form
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Complete a DataHub Compliance Form
<FeatureAvailability/>

This guide provides an example of completing a Compliance Form, covering:

1. Accessing a Form from an Asset Page or the DataHub Cloud Task Center
2. Completing a Form for a single Asset or multiple Assets (DataHub Cloud only)
3. Understanding different Form Question completion states

The example uses the **Governance Initiative 2024**, a Verification Form with 3 Required Questions, assigned to the `dogs_in_movies` and `dog_rates_twitter` Datasets:

<p align="center">
  <img
       width="90%"
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/compliance_forms/sample-compliance-form.png" 
       alt="Sample Compliance Form"/>
</p>

## Accessing a Compliance Form

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

## Completing a Form for a Single Asset

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
       alt="Asset Ready to Verifiy"/>
</p>

And here's the `dogs_in_movies` sidebar after Verifying all responses:

<p align="center">
  <img
       width="80%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/compliance_forms/completion/complete-task-asset-verified.png"
       alt="Asset Ready to Verifiy"/>
</p>

### Navigating to the Next Asset

To continue working through the Compliance Forms assigned to you, use the navigation arrows located in the top-right corner. These arrows will take you to the next Asset that is still pending Form completion or Verification. Only Assets that require action will appear in this flow, allowing you to focus on the remaining tasks without unnecessary steps.

<!-- ## Completing a Form Question for Multiple Assets

:::note
Completing Form Questions in for multiple Assets is only supported for DataHub Cloud.
:::
 -->

## Understanding Different Form Question Completion States

When completing a Compliance Form, you may encounter various types of questions, each with unique completion states based on existing metadata or prior user responses. This section highlights these scenarios to help you understand how questions can be answered, confirmed, or updated during the process.

**_1. What is the primary use case for this asset?_**

This required Question is asking the Assignee to provide Documentation on how the Asset should be used. Note that there is no text populated in the description, meaning the Asset does not have any docuumenation at all.

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

**Why can't I complete a Compliance Form for an Asset?**

Forms can be assigned to Asset Owners, specific DataHub Users, or a combination of the two. Please confirm with the Form author that your DataHub user is in-scope for the Form.