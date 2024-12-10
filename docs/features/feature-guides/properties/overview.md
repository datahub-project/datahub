---
title: Overview
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# About DataHub Structured Properties
<FeatureAvailability/>

**DataHub Structured Properties** provide a structured and validated approach to attaching metadata to your Data Assets, enabling organizations to enforce consistency and quality in metadata collection.

<p align="center">
  <img width="90%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/properties/custom_and_structured_properties.png"/>
</p>

## What are Structured Properties?

**Structured Properties** are a configurable feature in DataHub that allow metadata to be associated with Assets in a structured format. Each Structured Property has a specific type (e.g., Date, Integer, DataHub Asset, Text) and can include validation rules, such as restricting to specific allowed values or adhering to a pre-defined format.

Structured Properties can be added to all DataHub Asset types, including Datasets, Dashboards, Pipelines, and more. This ensures flexibility in managing metadata across your entire data ecosystem.

Structured Properties can also be used within [**DataHub Compliance Forms**](../compliance-forms/overview.md) to crowdsource documentation, annotation, and classification tasks. By incorporating Structured Properties into Compliance Forms, organizations can define specific metadata collection requirements and assign them to relevant stakeholders, such as data owners, stewards, and subject matter experts. This enables large-scale governance initiatives to be efficiently managed while maintaining accuracy and accountability.

### Key Features of Structured Properties:

1. **Typed Fields:** Properties are explicitly typed, including options like Date, Integer, URN, or Text.
2. **Validation Rules:** Enforce standards by restricting values to a specific format or a pre-defined list of acceptable inputs.
3. **Custom Configuration:** Structured Properties can be tailored to meet your organization’s needs, whether for governance, compliance, or operational metadata collection.

### Configuration Options

Structured Properties offer several configuration options to enhance metadata management:

- **Hide Property:** For use cases where property values should not be viewable by DataHub users.  
- **Show in Search Filters:** Enables users to filter for Assets based on specific property values, improving discoverability.  
- **Customize Visibility:** Allows you to control where the Structured Property appears, such as in the Asset Sidebar, Asset Badge, and/or a Dataset Schema view’s Columns Table.

## Why Use Structured Properties?

Structured Properties are especially useful for organizations that require:

- **Consistency and Accuracy:** Enforce metadata standards and ensure high-quality data collection.
- **Governance and Compliance:** Collect metadata in a way that supports compliance with internal or external standards.
- **Scalability:** Simplify the process of collecting, managing, and enforcing metadata rules across a large number of Data Assets.
- **Collaboration through Compliance Forms:** Structured Properties integrate seamlessly with Compliance Forms, enabling teams to crowdsource critical metadata collection tasks and monitor compliance progress effectively.

<p align="center">
  <img width="90%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/structured_properties/1-list-structured-properties.png"/>
</p>

By leveraging these configurations, teams can ensure their metadata adheres to organizational policies and improves the discoverability and usability of Data Assets.

## Next Steps

Now that you understand Structured Properties, you’re ready to [Create a Structured Property](create-a-property.md).