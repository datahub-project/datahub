# Custom Home Page

DataHub's **Custom Home Page** empowers organizations and individual users to create personalized, modular home page experiences that put the most relevant data assets and information front and center.

<p align="center">
  <img
       width="70%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/home/default-home-page.png"
       alt="Default custom home page"/>
</p>

## Why Use the Custom Home Page?

The Custom Home Page transforms how your team interacts with DataHub by enabling you to:

- **Reduce Time to Insight**: Surface the most critical data assets, domains, and resources directly on your home page for instant access
- **Personalize Your Workflow**: Create a home page experience tailored to your specific role, projects, and data needs
- **Improve Data Discoverability**: Highlight important collections, documentation, and quick links that help your team find what they need faster
- **Standardize Organization Views**: Administrators can create consistent default experiences while still allowing individual customization

Whether you're a data analyst who needs quick access to specific dashboards, a data engineer focused on particular domains, or an administrator wanting to promote data governance resources, the Custom Home Page adapts to your workflow.

## What's Included

The Custom Home Page consists of **modules** that you can arrange in rows of up to 3 modules each. You can choose from:

### Default Modules

- **Your Assets**: Data assets that you own, giving you quick access to the assets you're responsible for
- **Your Subscriptions**: Assets you've subscribed to for updates and notifications
- **Domains**: A selection of the most-used domains in your organization for easy navigation

### Custom Modules

- **Quick Link**: Links to important external references like documentation sites, dashboards, or tools
- **Collection**: Curated collections of data assets that you want to highlight for easy discoverability
- **Documentation**: Pinned documentation that DataHub users should see on their home page
- **Hierarchy**: A top-down view of assets organized by selected Domains or Glossary Terms/Term Groups

### Organization Announcements

Important announcements from your organization appear above all modules, with the ability for users to individually dismiss them.

<p align="center">
  <img
       width="70%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/home/module-examples.png"
       alt="Module examples"/>
</p>

### Default Setup

When you upgrade to this version, your default home page will automatically include:

- Your organization's existing pinned links at the top
- Row 1: Your Assets and Your Subscriptions modules
- Row 2: Domains module

## Personalizing the Home Page

When you begin customizing your home page, you'll "fork" from your organization's default to create your own personal version. This means your changes won't affect what other users see, and you can always return to the organization default if needed.

### Add New Modules

Click **"Add Module"** to browse and select from available module types. Custom module can be configured with specific settings:

<p align="center">
  <img
       width="70%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/home/add-module-menu.png"
       alt="Add module menu"/>
</p>

### Rearrange Modules

Drag and drop modules to reorder them within rows or move them between rows. You can have up to 3 modules per row.

<p align="center">
  <img
       width="70%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/home/move-module.png"
       alt="Move module example"/>
</p>

### Edit Existing Modules

Click the **edit icon** on any module to modify its settings, title, or content. Currently, editing is only available for custom modules.

<p align="center">
  <img
       width="70%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/home/edit-module-modal.png"
       alt="Edit module modal"/>
</p>

### Remove Modules

Click the **remove icon** on any module to delete it from your home page. Don't worry - you can always add default modules back later.

<p align="center">
  <img
       width="70%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/home/edit-module-menu.png"
       alt="Edit module menu"/>
</p>

### Reset to Default

If you want to return to your organization's default home page:

1. Click the **settings icon** in the bottom right corner
2. Select **"Reset to Default Home Page"**
3. Confirm that you want to remove your personal customizations

This will delete your personal home page template and return you to the organization default.

<p align="center">
  <img
       width="70%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/home/reset-to-default-menu.png"
       alt="Option to reset to default menu"/>
</p>

<p align="center">
  <img
       width="70%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/home/confirm-reset-modal.png"
       alt="Confirm reset to default modal"/>
</p>

:::tip
Even when you have a personal home page, you can always grab modules from the default home page to add to your personal view. This lets you stay up-to-date with organizational standards while maintaining your customizations.
:::

## Configuring the Default Home Page

Users with the `MANAGE_HOME_PAGE_TEMPLATES` privilege (administrators by default) can customize the default home page that all users see when they first log in.

### Editing the Organization Default

To modify the default home page for your organization:

1. Click the **settings icon** in the bottom right corner of your home page
2. Select **"Edit Organization Default"**

<p align="center">
  <img
       width="70%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/home/edit-org-default-menu.png"
       alt="Edit org default menu"/>
</p>

When you enter organization default edit mode, you'll see the current default template. Any changes you make here will:

- **Save directly to the organization default**: Changes take effect immediately for all users using the default template
- **Appear for new users**: Anyone who hasn't customized their home page will see your updates
- **Not override personal templates**: Users who have created personal home pages will continue to see their customized versions

<p align="center">
  <img
       width="70%"  
       src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/home/edit-org-default-page.png"
       alt="Edit org default page"/>
</p>

### Managing Default Modules

As an administrator editing the organization default, you can:

- Add modules that provide value to most users in your organization
- Remove modules that aren't relevant to your team's workflow
- Rearrange modules to prioritize the most important information
- Update module configurations (like which domains to feature)

### Best Practices for Default Templates

When configuring your organization's default home page:

- **Include broadly useful modules**: Choose modules that provide value to most users, like "Your Assets" and "Domains"
- **Feature organizational resources**: Use Quick Link and Documentation modules to promote important company resources
- **Keep it focused**: A cluttered default page can overwhelm new users - aim for 3-6 well-chosen modules
- **Update regularly**: Review and refresh the default template periodically to ensure it stays relevant

:::note
Administrators can have both the ability to edit the organization default AND maintain their own personal home page customizations. The two are independent.
:::

## Next Steps

Now that you understand how to customize your home page experience:

1. **Start with small changes**: Try rearranging existing modules or adding a Quick Link to see how the interface works
2. **Experiment with modules**: Each module type offers different ways to surface important information
3. **Gather feedback**: If you're an administrator, ask users what they'd find most valuable on the default home page
4. **Iterate over time**: Home page needs may change as your team and data landscape evolve

The Custom Home Page is designed to grow with your organization's needs, providing both the flexibility for individual workflows and the consistency that administrators need to promote important resources and best practices.

## Relevant APIs
