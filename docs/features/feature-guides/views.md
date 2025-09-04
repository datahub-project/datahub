---
title: Views
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# About DataHub Views

<FeatureAvailability/>

**DataHub Views** enable users to save and share sets of search filters for reuse when browsing and discovering data assets. Views provide a consistent way to apply complex filter combinations across search results, making it easier to focus on specific subsets of data that matter most to your workflow.

With Views, you can create personalized data discovery experiences or establish standardized filtering patterns across your organization, ensuring teams consistently access the data assets most relevant to their work.

<p align="center">
  <img
       width="70%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/views/select-view.png"
       alt="Views dropdown showing available views"/>
</p>

## What are Views?

A DataHub **View** is a saved combination of search filters that can be instantly applied to DataHub's search results. Views transform complex, multi-criteria searches into one-click filters, streamlining how you and your team discover and access data assets.

A View consists of:

1. **Filter Criteria:** The specific conditions that assets must match, including entity types, platforms, domains, tags, owners, glossary terms, and any other searchable metadata fields available in DataHub.

2. **Visibility Scope:** Views can be either **Private** (personal) or **Public** (global), determining who can access and use them.

Once a View is created, it appears in the Views dropdown in DataHub's Search bar. Users can instantly apply any available View to filter their search results according to the saved criteria.

### Why Use Views?

Views enable organizations and individuals to:

- **Save Complex Filters:** Preserve sophisticated filter combinations that would otherwise need to be recreated each time you search.
- **Share Team Perspectives:** Create standardized views that help your team consistently filter data according to shared criteria.
- **Streamline Workflows:** Apply saved filters instantly to focus on the data assets most relevant to your current tasks.
- **Maintain Context:** Switch between different filtered perspectives of your data landscape without losing your place.

By leveraging Views, users can ensure consistent data discovery patterns and foster collaboration between team members who need to access similar subsets of data assets.

<p align="center">
  <img
       width="70%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/views/create-view.png"
       alt="View builder modal showing view creation interface"/>
</p>

## Create a View

The following steps will guide you through creating a new Personal or Public View.

### Getting Started

There are two main entry points to create a new View in DataHub:

#### Option A: From the Search bar

In the top Search bar, click the **Views** icon on the right side and select **Create New View**.

<p align="center">
  <img
       width="70%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/views/create-view-from-view-list.png"
       alt="Create view option in views dropdown menu"/>
</p>

#### Option B: From Filtered Search Results

In the main Search experience, you can easily create a Personal or Public View by saving the filters you have already applied. Once you have found the set of Filters you would like to re-use, simply click **Save as a View**.

Note: this is only an option when you do not have an active View selected.

<p align="center">
  <img
       width="70%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/views/create-view-from-search-results.png"
       alt="Search results showing filters before saving as view"/>
</p>

### Configure View Details and Filters

In the View Builder, provide the following details:

1. **Name:** Choose a descriptive name that clearly communicates the view's purpose, such as "Finance Domain Datasets" or "Production BigQuery Tables".

   _**Pro Tip:** This name appears in the Views dropdown for all users (for public views), so make it clear and specific._

2. **Description:** (Optional) Add context about the view's purpose, when to use it, or what criteria it applies. This helps other users understand the intent behind the view.

3. **Type:** Choose the visibility scope:

   - **Personal:** Only visible and usable by you
   - **Public:** Visible to all users in your organization. Users must have the `MANAGE_GLOBAL_VIEWS` privilege to create, manage, and delete Public Views.

4. **Filters:** Configure the criteria assets must meet in order to be displayed, for example:
   - **Entity Type:** Narrow down results to Datasets, Dashboards, Charts, etc.
   - **Platform:** Filter by data platform (Snowflake, BigQuery, PostgreSQL, etc.)
   - **Domain:** Assets that are assigned to a specific business domain
   - **Owners:** Show assets owned by specific users or teams

Click **Save** and the View will be automatically applied.

## Manage Views

All users have the option of setting one Personal View as their own default. This View will be activated by default for the user across sessions; users always have the option of clearing or changing the View in a given session.

<p align="center">
  <img
       width="70%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/views/view-menu-options.png"
       alt="View menu options showing set as default and other management actions"/>
</p>

Users with the `MANAGE_GLOBAL_VIEWS` privilege can edit, create, delete, and set default Views for the entire platform.

Views can be edited via the 3-dot menu, either from the Views dropdown in the search bar, or via the **Manage Views** page.

<p align="center">
  <img
       width="70%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/views/manage-views.png"
       alt="View management interface showing edit and delete options"/>
</p>

## Best Practices

### Creating Effective Views

- **Descriptive names:** Use clear names that indicate the View's purpose and scope
- **Focused scope:** Create Views that serve specific use cases rather than overly broad filters
- **Regular maintenance:** Review and update Views periodically to ensure they remain relevant
- **Team coordination:** For public Views, collaborate with your team to ensure they meet collective needs

### Organizing Your Views

- **Personal workflow:** Create Personal Views for your most common search patterns
- **Team standards:** Use Public Views to establish consistent team perspectives
- **Purpose-driven:** Name and organize Views based on business function rather than technical details
- **Documentation:** Use descriptions to explain when and why to use each View
