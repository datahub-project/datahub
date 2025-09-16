---
title: Custom Asset Summaries
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Custom Asset Summaries

<FeatureAvailability saasOnly />

DataHub's **Custom Asset Summaries** enable organizations to create tailored, curated discovery experiences for their most important logical assets by customizing the Summary tab view that users see when browsing Domains, Data Products, Glossary Terms, and Glossary Term Groups.

<p align="center">
  <img
       width="70%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/summary/summary-overview.png"
       alt="Default custom asset summary page"/>
</p>

:::info Supported Asset Types
Custom Asset Summaries are currently supported for **Domains, Data Products, Glossary Terms, and Glossary Term Groups only**. These logical asset types help organize and group your physical data assets within DataHub.
:::

## Why Use Custom Asset Summaries?

Custom Asset Summaries transform how your team discovers and navigates key organizational assets by enabling you to:

- **Improve Asset Discoverability**: Highlight the most relevant information about your domains, data products, and glossary terms right on the Summary tab
- **Create Owner-Driven Experiences**: Asset owners can curate what information is most important for their users to see
- **Reduce Navigation Time**: Surface key details, documentation, and related assets without requiring users to click through multiple tabs
- **Standardize Asset Presentation**: Ensure consistent, professional presentation of your most important organizational constructs

Whether you're a domain owner wanting to highlight key data products, a data product manager showcasing important assets, or a data steward organizing glossary terms, Custom Asset Summaries put the most relevant information front and center.

## What's Included

Custom Asset Summaries consist of three customizable sections that you can tailor to your asset's specific needs:

### Summary Page Header

The header section showcases key properties about your asset for immediate visibility. By default, this includes:

- **Ownership information** - Who owns and manages this asset
- **Creation date** - When the asset was established
- **Tags** - Asset classifications (available for assets that support tagging)
- **Glossary terms** - Business context and definitions applied to the asset
- **Domain association** - Organizational placement
- **Structured properties** - Custom metadata fields you want to highlight

You can customize this section to show the properties most relevant to your users, including removing default properties or adding new ones. Note that property availability depends on what each asset type supports - for example, not all logical asset types currently support tags.

### Documentation and Links Section

This section brings documentation capabilities directly to the Summary tab for easy access:

- **Rich text documentation** - Add, edit, and format detailed descriptions about your asset
- **Resource links** - Include links to external documentation, dashboards, tools, or other relevant resources
- **Quick access** - Users no longer need to navigate to a separate Documentation tab

### Module Section

The module section provides contextual information and navigation for your asset through both default and custom modules:

#### Default Modules

Each asset type includes relevant default modules:

- **Assets Module** (Domains, Data Products, Glossary Terms): Shows data assets that belong to this domain, data product, or are tagged with this glossary term
- **Domains Module** (Domains only): Displays the hierarchy of child domains within this domain
- **Data Products Module** (Domains only): Lists all data products contained within this domain
- **Contents Module** (Glossary Term Groups only): Shows child glossary terms and term groups for easy hierarchy navigation
- **Related Terms Module** (Glossary Terms only): Lists all terms that share relationships with the current term

#### Custom Modules

You can also add the same custom modules available on the home page:

- **Collection**: Curated lists of specific assets you want to highlight
- **Documentation**: Pin important documentation that users should see
- **Hierarchy**: Top-down view of assets organized by domains or glossary terms

<p align="center">
  <img
       width="70%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/summary/summary-modules.png"
       alt="Summary page module examples"/>
</p>

## Customizing Asset Summaries

### Editing Summary Page Headers

To customize which properties appear in your asset's header:

1. Navigate to your asset's Summary tab
2. Click the **plus icon** in the header section to add new properties
3. Replace or remove existing properties by clicking their name

<p align="center">
  <img
       width="70%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/summary/summary-edit-properties.png"
       alt="Edit header properties"/>
</p>

### Managing Documentation and Links

Add or update documentation and links directly on the Summary tab:

1. Click **"Edit Description"** icon on the right side of the "About" section header
2. Use the rich text editor to format your content
3. Click **"Add Link"** icon on the right side of the "About" section header
4. Organize links by editing or removing them as needed where they are listed

<p align="center">
  <img
       width="70%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/summary/summary-edit-documentation.png"
       alt="Edit documentation section"/>
</p>

### Adding and Arranging Modules

Customize the module section to highlight the most important information:

1. Click the "plus" button below existing modules to browse available module types
2. Drag and drop modules to reorder them
3. Use the edit or remove options in the module menu to modify existing modules

<p align="center">
  <img
       width="70%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/summary/summary-edit-modules.png"
       alt="Add module menu"/>
</p>

:::note Global Changes
Changes you make to an asset's Summary page are **visible to all users** who view that asset. Your customizations become the default experience for everyone accessing that domain, data product, or glossary term.
:::

## Permissions and Access Control

To customize Asset Summaries, users need the **"Manage Asset Summary"** privilege for the specific asset they want to edit. This permission can be configured through DataHub's policy editor alongside other access controls. By default, Admins, Editors, and asset owners will be granted this permission.

### Setting Up Permissions

1. Navigate to **Settings > Permissions > Policies**
2. Create or edit a policy to include the "Manage Asset Summary" privilege
3. Assign the policy to appropriate users or groups
4. Apply the policy to specific assets or asset types

This granular permission system ensures that only authorized users can modify how critical organizational assets are presented to the broader team.

## Best Practices

When customizing your Asset Summaries:

- **Focus on user needs**: Include properties and modules that help users understand and navigate your assets effectively
- **Keep it relevant**: Don't overcrowd the Summary page - prioritize the most important information
- **Update regularly**: Review and refresh your customizations as your assets and organization evolve
- **Consider your audience**: Tailor the presentation to match how your team typically uses these assets
- **Leverage modules strategically**: Use default modules to show relationships and custom modules to highlight specific collections or documentation

## Next Steps

Now that you understand Custom Asset Summaries:

1. **Start with high-traffic assets**: Begin by customizing the Summary pages for your most frequently accessed domains and data products
2. **Gather feedback**: Ask users what information would be most helpful to see on Summary pages
3. **Iterate based on usage**: Monitor how users interact with your customized summaries and adjust accordingly
4. **Scale thoughtfully**: Apply learnings from your initial customizations to other assets in your organization

Custom Asset Summaries are designed to evolve with your organization's needs, providing the flexibility to create discovery experiences that truly serve your users while maintaining the structure that makes DataHub powerful.
