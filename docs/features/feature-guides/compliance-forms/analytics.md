---
title: Form Analytics
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# DataHub Compliance Form Analytics

<FeatureAvailability saasOnly />

DataHub Cloud provides out-of-the-box analytics to help you monitor and track the success of your Compliance Form initiatives. This guide will walk you through the available reporting views and how to leverage them effectively.

### Prerequisites

To access Compliance Form Analytics, you must have the **View Compliance Forms** and/or **Manage Compliance Forms** Platform privilege.

## Overview

Form Analytics provides out-of-the-box reporting for Compliance Form completion, enabling Form Owners to easily monitor the progress of their initiatives. The analytics dashboard offers four distinct views to help you understand completion rates and identify areas that need attention:

1. Overall Progress
2. Form-specific Analysis
3. Domain-based Insights
4. Assignee Performance Tracking

Each reporting view can be filtered by Assigned Date with the following preset ranges:

- Last 7 days
- Last 30 days
- Last 90 days
- Last 365 Days
- All Time

For deeper analysis, you can download the raw data feeding into each dashboard view using the top-right **Download** button.

:::note
Form Analytics are calculated every day at 12am US PT.
:::

## Understanding Form States

Before diving into the different views, it's important to understand how Form and Asset states are tracked:

### Form States

Individual Forms can be in one of three states:

1. **Not Started**: No questions have been answered yet
2. **In Progress**: At least one question has been answered, but not all required questions are complete
3. **Completed**: All required questions are answered (and verified, if it's a Verification Form)

### Asset States

Assets can have multiple Forms assigned simultaneously. An Asset's overall status is determined by all its assigned Forms:

1. **Not Started**: All assigned Forms are in "Not Started" state
2. **In Progress**: At least one Form is either:
   - In "In Progress" state
   - Or there's a mix of "Completed" and "Not Started" Forms
3. **Completed**: All assigned Forms are in "Completed" state

## Analytics Views

### Overall View

The Overall view provides a high-level summary of Form completion across your organization, helping you quickly identify which Forms, Domains, or Assignees need attention. Use this view as your starting point to determine which drill-down view (Form-specific, Domain-based, or Assignee) will be most helpful for addressing completion gaps.

Key features:

- Aggregated view of all assets and their Form status
- Total number of assets in each state
- High-level compliance progress metrics
- Timeline visualization of progress

<p align="center">
  <img 
       width="90%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/compliance_forms/analytics/form-analytics-overall-view.png"
       alt="Overall Progress Dashboard"/>
</p>

### Form-Specific Analysis

The Form-specific view allows you to drill down into individual Forms to understand their completion status. This view is particularly useful for:

- Tracking progress of specific compliance initiatives
- Identifying questions that users might be slow to address
- Generating form-specific reports
- Pinpointing potential bottlenecks in your compliance process

<p align="center">
  <img 
       width="90%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/compliance_forms/analytics/form-analytics-by-form-view-questions.png"
       alt="Form-specific Analysis Dashboard"/>
</p>

### Domain-Based Insights

The Domain view provides visibility into Form completion rates across different business domains. This perspective helps:

- Domain managers focus on their area of responsibility
- Compare progress across different domains
- Identify if specific Forms within a domain need attention
- Enable targeted action where necessary

<p align="center">
  <img 
       width="90%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/compliance_forms/analytics/form-analytics-by-domain.png"
       alt="Domain-based Insights Dashboard"/>
</p>

### Assignee Performance Tracking

The Assignee view allows you to monitor how specific Users or User Groups are tracking toward their assigned tasks. Use this view to:

- Track individual user's compliance tasks
- Monitor workload distribution
- Identify potential bottlenecks
- Assess if certain individuals or teams need additional support

<p align="center">
  <img 
       width="90%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/compliance_forms/analytics/form-analytics-by-user.png"
       alt="Assignee Performance Dashboard"/>
</p>

## Best Practices

To make the most of Form Analytics:

1. **Regular Monitoring**: Check the Overall view weekly to stay on top of completion rates
2. **Targeted Follow-ups**: Use the Domain and Assignee views to identify specific areas or teams that might need additional support
3. **Question Analysis**: Leverage the Form-specific view to identify and address common bottlenecks in your Forms
4. **Data-Driven Decisions**: Download raw data for deeper analysis and to inform future Form design
5. **Workload Management**: Use the Assignee view to ensure tasks are distributed effectively
6. **Domain Oversight**: Leverage the Domain view to ensure consistent progress across your organization

### Related Features

- [Create a Compliance Form](create-a-form.md)
- [Complete a Compliance Form](complete-a-form.md)
