---
title: Create and Add a Structured Property
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Create and Add a DataHub Structured Property
<FeatureAvailability/>

This guide walks you through creating a Structured Property via the DataHub UI, including:

1. Defining a new Structured Property
2. Configuring display preferences for a Structured Property
3. Adding a Structured Property to an Asset
4. Adding a Structured Property to a Column

:::note
To learn more about creating and assigning Structured Properties via the CLI, please see the [Create Structured Properties](/docs/api/tutorials/structured-properties.md) tutorial.
:::

### Prerequisites

To create, edit, or remove Structured Properties, you must have the **View Structured Properties** and **Manage Structured Properties** platform privileges.

To add an existing Structured Property to an Asset, change its value, or remove it from an Asset, you must have the **Edit Properties** metadata privilege.

## Define a New Structured Property

From the navigation bar, go to **Govern** > **Structured Properties**.

Click **+ Create** to start defining your Property.

<p align="center">
  <img 
       width="90%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/structured_properties/1-list-structured-properties.png"
       alt="View all Structured Properties"/>
</p>

First, provide the following details:

1. **Name and Description:** Clearly describe the purpose and meaning of the Structured Property so users understand its role and context.  
2. **Property Type:** Choose a type that best fits the metadata you want to collect. Available types include **Text**, **Number**, **Date**, **DataHub Entity**, or **Rich Text**. Choosing any of the "List" options allows multiple entries for the Property.
3. **Allowed Values (Optional):** For **Text**, **Number**, and **DataHub Entity** types, define a set of allowed values to ensure consistent input across assets.
4. **Applies To:** Specify which DataHub asset types (e.g., Datasets, Dashboards, Pipelines) the Structured Property can be associated with, ensuring relevance and precision.

:::caution
Once you you save a Structured Property, you **cannot** edit or remove Allowed Values. However, you can add additional Allowed Values.
:::

For example, imagine your organization wants to standardize how data assets (e.g., Datasets, Tasks, Pipelines) are categorized during their development cycle. By creating a **Lifecycle Stage** Structured Property, you can set a pre-defined list of allowed statuses, such as **Draft**, **Review**, and **Prod**, ensuring consistency and transparency.

<p align="center">
  <img 
       width="90%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/structured_properties/2.1-create-structured-property-settings.png"
       alt="View all Structured Properties"/>
</p>

## Set Display Preferences for the Structured Property

When defining a Structured Property, you can customize how it will be visible to DataHub users. By default, Structured Properties are visible in an Asset's **Properties** tab but can be conditionally configured with the following options:

1. **Hide Property:**  
   Use this option if the Structured Property contains sensitive metadata that should not be visible to DataHub users via the UI. This ensures that only users with the necessary permissions can view or interact with the property values.

2. **Customize Visibility:**  
   Decide where the Structured Property appears across the DataHub UI:  
   - **Asset Badge:** Display the property value as a badge on Assets to highlight key metadata.  
   - **Asset Sidebar:** Show the property in the Asset Sidebar for quick visibility while navigating an Asset.

3. **Show in Search Filters:**  
   Enable this option to allow users to filter Assets by the values of this Structured Property. This improves discoverability and facilitates searches for Assets with specific attributes or classifications.  

4. **Show in Columns Table:**  
   Use this option to display the Structured Property value in the Dataset Schema view’s Columns Table. This is particularly useful for capturing field-level custom metadata and making it accessible alongside schema details.

For the **Lifecycle Stage** example, imagine you want to allow users to filter by lifecycle status and view it at a glance during data discovery. To achieve this, you would enable **Show in Search Filters**, **Asset Badge**, and **Asset Sidebar**:

<p align="center">
  <img 
       width="90%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/structured_properties/2.2-create-structured-property-display-settings.png"
       alt="Configure Structured Property Visibility"/>
</p>

## Add a Structured Property to an Asset

Once a Structured Property has been defined, you can add it to the designated Asset Types.

From an Asset's **Properties** tab, click the `+` button to see a drop-down list of all available Structured Properties. For example, you can now see **Lifecycle Stage** as an option for the `pet_profiles` Dataset:

<p align="center">
  <img 
       width="90%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/structured_properties/3.1-add-structured-property-entity-level.png"
       alt="Add a Structured Property to an Asset"/>
</p>

Continuing with the **Lifecycle Stage** example, designate the `pet_profiles` Dataset as being in **Prod**:

<p align="center">
  <img 
       width="90%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/structured_properties/3.2-add-structured-property-entity-level-modal.png"
       alt="Select a Structured Property Value"/>
</p>

After clicking **Save**, the **Lifecycle Stage** for `pet_profiles` will appear in the following sections of the Asset Page:

1. Properties Tab  
2. Asset Badge  
3. Asset Sidebar  

<p align="center">
  <img 
       width="90%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/structured_properties/3.3-add-structured-property-entity-level-after.png"
       alt="View a Structured Property on an Asset"/>
</p>

:::info
[**DataHub Compliance Forms**](../compliance-forms/overview.md) make it easy to update values for multiple Assets at once!
:::

### Edit or Remove a Structured Property from an Asset

After a Structured Property has been added to an Asset, users can modify its value or remove the property entirely using the **More** menu:

<p align="center">
  <img 
       width="90%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/structured_properties/3.4-edit-remove-structured-property-entity-level.png"
       alt="Editing or Removing a Structured Property on an Asset"/>
</p>

### Search for Assets by a Structured Property Value

For Structured Properties that have **Show in Search Filters** enabled, users can filter search results based on allowed values.

For example, with the **Lifecycle Stage** property enabled as a filter, users can find it under the **More** dropdown in the Search interface:

<p align="center">
  <img 
       width="90%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/structured_properties/4.1-search-filter-by-structured-property.png"
       alt="Structured Property as Search Filter"/>
</p>

From here, you can quickly narrow down Search results based on the desired stage, such as **Prod**:

<p align="center">
  <img 
       width="90%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/structured_properties/4.2-search-filter-by-structured-property-results.png"
       alt="Filtered Results by Structured Property Value"/>
</p>

Notice how the **Prod** value is displayed prominently on the `pet_profiles` Asset Badge:

<p align="center">
  <img 
       width="90%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/structured_properties/4.3-search-filter-by-structured-property-results-badge.png"
       alt="View Structured Property in Asset Badge"/>
</p>

## Add a Structured Property to a Column

Structured Properties can be applied at the column level, providing deeper context for how individual dataset fields relate to business concepts or terminology. In this example, we’ll create a Structured Property called **Business Label** to help business users understand how dataset columns align with common terminology, acronyms, or key business concepts.

### Define the Business Label Property

Follow these steps to define and configure the **Business Label** Structured Property:

1. **Property Details:**  
   - **Name:** Business Label  
   - **Description:** Provide a description to explain its purpose, such as:  
     *"A user-friendly name for a dataset column, helping business users understand its meaning."*  
   - **Property Type:** Select **Text**, allowing any valid string to be entered.  
   - **Applies To:** Set this property to apply exclusively to **Columns**.  

2. **Display Preferences:**  
   - By default, column-level Structured Properties will be enabled for **all columns** on **all datasets** within DataHub, accessible via the Column Sidebar.  
   - Optionally, enable **Show in Table Columns** to make the **Business Label** visible within the Columns Table on the dataset schema.  

<p align="center">
  <img 
       width="90%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/structured_properties/5.2-create-structured-property-column-display-options.png"
       alt="Configuring Column Structured Property"/>
</p>

:::caution
While the column sidebar provides convenient access to assigned properties, adding too many Structured Properties can clutter the view. Limit the number of properties shown in the sidebar to maintain clarity and usability.
:::

Once configured, the **Business Label** Structured Property will automatically be added to all columns on dataset assets within DataHub.

For example, after assigning the property, it will appear in two key areas of the `pet_profiles` Asset Page:

1. **Columns Table:** The **Business Label** property and its populated values will be displayed directly within the Columns Table on the Dataset Schema, enabling users to view field-level metadata easily.  
2. **Column Sidebar:** Structured Properties configured for columns, including **Business Label**, will also appear in the column’s sidebar.  

By applying column-level Structured Properties like **Business Label**, you enhance data discoverability and provide business users with valuable insights while keeping the interface user-friendly.

<p align="center">
  <img 
       width="90%"
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/structured_properties/6.1-view-structured-property-column-level.png"
       alt="Column-level Structured Property in Columns Table"/>
</p>

### Update the Business Label from the Column Sidebar  

When selecting a specific column in the UI, the **Business Label** Structured Property will be visible in the column’s sidebar. Users with appropriate permissions can view or update the value directly from this interface.

<p align="center">
  <img 
       width="90%"
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/structured_properties/6.2-view-structured-property-column-level-sidebar.png"
       alt="Column-level Structured Property in Sidebar"/>
</p>

This setup ensures that column-specific metadata, such as the **Business Label**, is accessible and actionable, helping business users better understand the dataset's structure and its alignment with key business concepts.

## FAQ and Troubleshooting

### Why can’t I change a Structured Property’s definition?

Once a Structured Property has been defined, only certain aspects can be modified:

**You can change:**
- Title and description
- Add new allowed values
- Add new supported asset types
- Update display preferences

**You cannot change:**
- The type of the Structured Property
- Existing allowed values and their definitions

### Why can't I configure a Structured Property to appear as an Asset Badge?
- Only **Text** and **Number** types with allowed values can be configured as Asset Badges.
- Only one Structured Property can be displayed as a Badge for a given Asset.

### Why can't I filter Search Results by a Structured Property?
- Verify that the Structured Property has been configured to appear in search filters.
- Ensure the filter is relevant by checking if there are assets associated with the Structured Property's value in your search results. Try different search terms or relax other applied filters.

### Why can't I add a Structured Property to an Asset?
- Confirm you have the **Edit Properties** privilege.
- Ensure the Structured Property has already been created and supports the type of Asset you're trying to modify.

### API Tutorials

- [Structured Properties API Guide](/docs/api/tutorials/structured-properties.md)

### Related Features

- [DataHub Compliance Forms](/docs/features/feature-guides/compliance-forms/overview.md)