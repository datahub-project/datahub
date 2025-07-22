import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Applications

<FeatureAvailability/>

Starting in version `1.2.0`, DataHub supports grouping data assets into collections called **Applications**. Applications reperesent the specific purpose the data assets are used for. Applications are similar to Domains in that they group data assets together based on business context.

Applications differ from Domains in that an Application is a smaller, more specific grouping for a single purpose, whereas Domains will often have many purposes within them. Applications differ from Data Products in the opposite direction, an Application would represent an entire purpose, and many Data Products would be expected to exist within that purpose. Applications are also not a core concept within Data Mesh- we observe organizations using Application as a grouping outside of Data Mesh principles. Applications are hidden by default because often times, they are not required for organizing your data- Domains and Data Products may be sufficient. For larger organizations with more complex Data Landscapes, Application can be a useful third grouping alongside Domain and Data Product that serve to fill the gap between those two concepts. An Application also differs from the Software Engineering concept of a Service- a Service is generally 1 distinct piece of software that is receiving API calls, etc. An Application in Datahub represents a purpose-based grouping that is not neccessarily scoped to a single Service.

An Example Application would be "Cancellation Refund Processing". This Application might exist in the domain Payments, under the Subdomain Cancellations. It might contain various tables, pipelines and services involved in processing refunds when a customer cancels an order. It may have peer Applications such as "Cancellations Notification Procesing", "Cancellation Fraud Detection" and "Cancellation Inventory Response". It may contain some Data Products such as "Customer Refund Daily Payments" and "Customer Refund Daily Failed Payments". It may contain a few Services involved in the processing of the cancellations, such as a service that sends payments, a service that manages refilling inventory, and so forth, depending on the architecture of the system. A DataHub user may opt for Application for categorizing the "Cancellation Refund Processing" in order to distinguish the grouping as serving a specific Business purpose. Often DataHub users who use Application already track the concept of Applications internally.

Some teams find that Domains and Data Products are sufficient to organize their metadata and may not need Applications. DataHub can be used successfully without involving this concept. As a result, Applications are hidden from the UI by default and must be enabled in the settings.

## Applications Setup, Prerequisites, and Permissions

What you need to create and add applications:

- **Edit Entity** privilege on Applications to create new Appliations, or edit existing ones.

- **Edit Entity Applications** privilege to apply Appliations to Entities.

- **Manage Features** platform privilege to control whether Applications are visible to users.

You can create these privileges by creating a new [Metadata Policy](./authorization/policies.md).

## Using Applications

### Enabling Applications

To enable Applications, navigate to **Settings**, then **Appearance**. There will be a toggle for **Show Applications**.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/master//imgs/applications/EnableApplicationsInSettings.png"/>
</p>

### Creating a Applications

To create an Application, first navigate to the **Applications** tab in the left-side navigation menu of DataHub.

Once you're on the Applications page, you'll see a list of all the Applications that have been created on DataHub.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/master//imgs/applications/ManageApplicationsScreen.png"/>
</p>

To create a new Application, click '+ Create Application'.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/master//imgs/applications/CreateNewApplicationModal.png"/>
</p>

Inside the form, you can choose a name for your Application, along with decription and owner.

### Assigning an Asset to an Application

You can assign assets to Applications using the UI or programmatically using the API. To assign in the UI, you can add an Application via the Entity sidebar, or from an Applications page.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/master//imgs/applications/AddAssetsToApplication.png"/>
</p>

### Searching by Application

Once you've created an Application, you can use the search bar to find it.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/master//imgs/applications/SearchForApplications.png"/>
</p>

Clicking on the search result will take you to the Applications's profile, where you
can edit its description, add / remove owners, and view the assets inside the Application.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/master//imgs/applications/ApplicationAssetsTab.png"/>
</p>

Once you've added assets to an Application, you can filter search results to limit to those Assets
within a particular Application using search filters.

#### Examples

**Creating an Application**

```graphql
mutation createApplication {
  createApplication(
    input: {
      properties: {
        name: "My New Application"
        description: "An optional description"
      }
    }
  )
}
```

This query will return an `urn` which you can use to fetch the Application details.

**Fetching an Application by Urn**

```graphql
query getApplication {
  application(urn: "urn:li:application:engineering") {
    urn
    properties {
      name
      description
    }
    children {
      total
    }
  }
}
```

**Adding a Dataset to an Application**

```graphql
mutation batchSetApplication {
  batchSetApplication(
    input: {
      resourceUrns: [
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,banking.public.customer,PROD)"
      ]
      applicationUrn: "urn:li:application:new-customer-signup"
    }
  )
}
```

> Pro Tip! You can try out the sample queries by visiting `<your-datahub-url>/api/graphiql`.

### Related Features

- [Domains](../../domains.md)
- [Data Products](../../dataproducts.md)
