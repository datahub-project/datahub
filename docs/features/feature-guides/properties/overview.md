---
title: Overview
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# About DataHub Structured Properties
<FeatureAvailability/>

DataHub **Structured Properties** allow you to add custom, validated properties to any Entity type in DataHub. Using Structured Properties, you can enable data discovery and governance based on attributes unique to your organization.

<p align="center">
  <img width="90%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/properties/custom_and_structured_properties.png"/>
</p>

## What are Structured Properties?

**Structured Properties** are a powerful way to customize your DataHub environment, enabling you to align metadata with your organization’s unique needs. By defining specific property types—such as Date, Integer, DataHub Asset, or Text—you can apply meaningful, context-aware attributes to your Assets. Validation rules, like restricting allowed values or enforcing specific formats, ensure consistency while giving you the flexibility to reflect your business’s terminology, workflows, and priorities.

Structured Properties can be added to the following Asset Types:

- Data Assets, such as Datasets, Columns, Tasks, Pipelines, Charts, Dashboards, and more.
- DataHub Entities, such as Domains, Glossary Terms & Groups, and Data Products.

### Key Features of Structured Properties:

1. **Typed Fields:** Properties are explicitly typed, including options like Date, Integer, URN, or Text.
2. **Allowed Values:** Enforce standards by restricting values to a specific format or a pre-defined list of acceptable inputs.
3. **Targeted Application:** Structured Properties can be tailored to specific asset types—such as Datasets, Columns, or Dashboards—ensuring they align with your organization’s data management needs and usage context.

### Display Settings

Structured Properties offer several configuration options to enhance metadata management:

- **Hide Property:** For use cases where property values should not be viewable by DataHub users.
- **Show in Search Filters:** Enables users to filter for Assets based on specific property values, improving discoverability.
- **Customize Visibility:** Allows you to control where the Structured Property appears, such as in the Asset Badge, Asset Sidebar, and/or a Dataset Schema view’s Columns Table.

## Why Use Structured Properties?

Structured Properties are especially useful for organizations that require:

- **Customization:** Customize how your end-users find assets within DataHub.
- **Governance and Compliance:** Collect metadata in a way that supports compliance with internal or external standards.

<p align="center">
  <img width="90%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/structured_properties/1-list-structured-properties.png"/>
</p>

By leveraging these configurations, teams can ensure their metadata adheres to organizational policies and improves the discoverability and usability of Data Assets.

## Next Steps

Now that you understand Structured Properties, you’re ready to [Create a Structured Property](create-a-property.md).