---
title: Create a Structured Property
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Create a DataHub Structured Property
<FeatureAvailability/>

This guide will walk you through creating a Structured Property via the DataHub UI, including:

1. Defining a new Structured Property
2. Configure display preferences for the Structured Property

:::note
To learn more about creating and assigning Structured Properties via CLI, please see the [Create Structured Properties](/docs/api/tutorials/structured-properties.md) tutorial.
:::

### Prerequisites

In order to create, edit, or remove Structured Properties, you must have the **View Structured Properties** and **Manage Structured Properties** Platform privileges.

## Step 1. Define a New Structured Property

From the navigation bar, head to **Govern** > **Structured Properties**.

Click **+ Create** to start defining your Property.

<p align="center">
  <img 
       width="90%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/structured_properties/1-list-structured-properties.png"
       alt="View all Structured Properties"/>
</p>

First up, provide the following details:

1. **Name and Description:** Clearly describe the purpose and meaning of the Structured Property so users understand its role and context.  
2. **Property Type:** Choose a type that best fits the metadata you want to collect. Available types include **Text**, **Number**, **Date**, **DataHub Entity**, or **Rich Text**. Choosing any of the "List" options will allow multiple entries for the Property.
3. **Allowed Values (Optional):** For **Text**, **Number**, and **DataHub Entity** types, you can define a set of allowed values to ensure consistent input across assets.
4. **Applies To:** Specify which DataHub asset types (e.g., Datasets, Dashboards, Pipelines) this Structured Property can be associated with, ensuring relevance and precision in its application.  

<p align="center">
  <img 
       width="90%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/structured_properties/2.2-create-structured-property-display-settings.png"
       alt="Create a Structured Property"/>
</p>

:::caution
Once you have saved a Srucutred Property, you **cannot** edit or remove Allowed Values. You can, however, add additional Allowed Values.
:::

## Step 2. Set Display Preferences for the Structured Property

Now that you have defined your Structured Property, you have the option to customize how it will be visible to your DataHub users. Here's how you can configure its visibility:  

1. **Hide Property:**  
   Select this option if the Structured Property contains sensitive metadata that should not be visible to regular DataHub users. This ensures that only users with the necessary permissions can view or interact with the property values.  

2. **Show in Search Filters:**  
   Enable this option to allow users to filter Assets by the values of this Structured Property. This is particularly useful for improving discoverability and facilitating searches for Assets with specific attributes or classifications.  

3. **Customize Visibility:**  
   Control where the Structured Property appears across the DataHub UI:  
   - **Asset Sidebar:** Display the property in the Asset Sidebar for quick visibility while navigating an Asset.  
   - **Asset Badge:** Showcase the property value prominently as a badge on Assets to highlight important metadata.

4. **Show in Columns Table:**  
   Use this option to display the Structured Property value in the Dataset Schema view’s Columns Table. This is especially useful for capturing field-level custom metadata and making it accessible alongside schema details.  

By tailoring these visibility settings, you can ensure that Structured Properties are displayed exactly where they are most impactful for your users while safeguarding sensitive information where necessary.  

<p align="center">
  <img 
       width="90%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/structured_properties/3-configure-visibility.png"
       alt="Configure Structured Property Visibility"/>
</p>





## Example: Creating an Asset-Level Structured Property


## Example: Creating a Column-Level Structured Property




....
