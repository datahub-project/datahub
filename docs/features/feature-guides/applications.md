import FeatureAvailability from '@site/src/components/FeatureAvailability';

# About DataHub Applications

<FeatureAvailability/>

DataHub Cloud v1.2.0 introduces **Applications**, an approach to grouping similar assets within DataHub that align with a specific business purpose.

Applications sit between Domains and Data Products in DataHub's metadata model hierarchy:

- **Domains** (largest): Business areas with multiple purposes
- **Applications** (middle): Single business purpose within the Domain
- **Data Products** (smallest): Specific data offerings within the Application

For example, an ecommerce company might have the following structure:

- **Payments Domain**: Contains everything related to processing payments across the business
- **Refund Application**: Groups all assets specifically used for handling customer refunds within the Payments Domain, including:
  - "Daily Refund Payments" **Data Product**
  - "Daily Refund Failures" **Data Product**
  - Supporting assets like payment pipelines, refund processing datasets, etc.
- **Fraud Detection Application**: Groups all assets related to identifying and preventing fraudulent transactions within the Payments Domain, including:
  - "Real-time Fraud Alerts" **Data Product**
  - "Weekly Fraud Analytics Report" **Data Product**
  - Supporting assets like ML models, risk scoring datasets, etc.

Each Application serves its specific business purpose while remaining part of the broader Payments Domain.

## Why Use Applications?

Applications work best for large organizations that already use this concept internally and need additional organization beyond Domains and Data Products. For most teams, we recommend starting with Domains and Data Products first.

Keep in mind the following:

- **Applications aren't part of Data Mesh architecture** — they're an additional organizational layer that some large organizations find helpful, but are not suitable for teams adopting Data Mesh
- **Applications differ from software services** — while a service is a single piece of software, an Application within DataHub groups multiple data assets around one business purpose

:::note
**Most teams don't need Applications**. We find that Domains and Data Products frequently provide enough organizational support for DataHub users.
:::

## Prerequisites and Permissions

Users will need the following DataHub Privileges to use Applications:

- **Edit Entity** privilege on Applications to create new Applications or edit existing ones.
- **Edit Entity Applications** privilege to assign Applications to data assets.
- **Manage Features** platform privilege to control whether Applications are visible to users.

Learn more about creating a Custom Policy to leverage these privileges [here](../../authorization/policies.md).

## Using Applications

### Enabling Applications

To enable Applications, you will need to work with your DataHub rep to configure the feature. Once this is complete, navigate to **Settings** > **Appearance** and toggle on **Show Applications**.

:::note
Applications are disabled by default. Please reach out to your DataHub rep to enable the feature.
:::

<p align="center">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/master//imgs/applications/EnableApplicationsInSettings.png"/>
</p>

### Creating an Application

To create an Application, first navigate to the **Applications** tab in the left-side navigation menu of DataHub.

Once you're on the Applications page, you'll see a list of all the Applications that have been created on DataHub.

<p align="center">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/master//imgs/applications/ManageApplicationsScreen.png"/>
</p>

To create a new Application, click '+ Create Application'.

<p align="center">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/master//imgs/applications/CreateNewApplicationModal.png"/>
</p>

Inside the form, you can choose a name for your Application, along with description and owner. Learn more about creating Applications via API in [this tutorial](../../api/tutorials/applications.md).

### Assigning Assets to an Application

You can assign assets to Applications using the UI or programmatically using the API. To assign in the UI, you can add an Application via the Entity sidebar, or from an Applications page.

<p align="center">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/master//imgs/applications/AddAssetsToApplication.png"/>
</p>

Learn more about assigning assets to Applications via API in [this tutorial](../../api/tutorials/applications.md).

### Searching by Application

Once you've created an Application, you can use the search bar to find it.

<p align="center">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/master//imgs/applications/SearchForApplications.png"/>
</p>

Clicking on the search result will take you to the Application's profile, where you can edit its description, manage owners, and view the assets inside the Application.

<p align="center">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/master//imgs/applications/ApplicationAssetsTab.png"/>
</p>

Once you've added assets to an Application, you can filter search results to limit to those assets within a particular Application using search filters.

### API Tutorials

- [Applications](../../api/tutorials/applications.md)

### Related Features

- [Domains](../../domains.md)
- [Data Products](../../dataproducts.md)
