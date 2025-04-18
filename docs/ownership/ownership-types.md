import FeatureAvailability from '@site/src/components/FeatureAvailability';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Custom Ownership Types

<FeatureAvailability/>

**🤝 Version compatibility**
> Open Source DataHub: **0.10.4** | Acryl: **0.2.8**

## What are Custom Ownership Types?
Custom Ownership Types are an improvement on the way to establish ownership relationships between users and the data assets they manage within DataHub.

## Why Custom Ownership Types?
DataHub brings a pre-defined opinion on ownership relationships. We are aware that it may not always precisely match what you may need. 
With this feature you can modify it to better match the terminology used by stakeholders.


## Benefits of Custom Ownership Types
Custom ownership types allow users to bring in their organization's ownership nomenclature straight into DataHub.
This allows stakeholders to discover what relationships an owner of an entity has using the language already in-use at organizations.

## How Can You Use Custom Ownership Types?
Custom Ownership types have been implemented as a net-new entity in DataHub's Metadata Model meaning all entity-related APIs can be used for them.
Additionally, they can be managed through DataHub's Admin UI and then used for ownership across the system in the same way pre-existing ownership types are.

## Custom Ownership Types Setup, Prerequisites, and Permissions

What you need to create and add ownership types:

* **Manage Ownership Types** metadata privilege to create/delete/update Ownership Types at the platform level. These can be granted by a [Platform Policy](./../authorization/policies.md#platform-policies).
* **Edit Owners** metadata privilege to add or remove an owner with an associated custom ownership type for a given entity.

You can create this privileges by creating a new [Metadata Policy](./../authorization/policies.md#metadata-policies).

## Using Custom Ownership Types

Custom Ownership Types can be managed using the UI, via a graphQL command or ingesting an MCP which can be managed using software engineering (GitOps) practices.

### Managing Custom Ownership Types

<Tabs>
  <TabItem value="ui" label="UI" default>

To manage a Custom Ownership type, first navigate to the DataHub Admin page:
<p></p>

<p align="center">
    <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/ownership/manage-view.png" />
</p>
    
Then navigate to the `Ownership Types` tab under the `Management` section.

To create a new type simply click '+ Create new Ownership Type'.

This will open a new modal where you can configure your Ownership Type.

Inside the form, you can choose a name for your Ownership Type. You can also add descriptions for your ownership types to help other users more easily understand their meaning. 

Don't worry, this can be changed later.
    
<p align="center">
    <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/ownership/ownership-type-create.png" />
</p>
    
Once you've chosen a name and a description, click 'Save' to create the new Ownership Type.

You can also edit and delete types in this UI by click on the ellipsis in the management view for the type you wish to change/delete.
  </TabItem>
  <TabItem value="cli" label="CLI" default>
Just like all other DataHub metadata entities DataHub ships with a JSON-based custom ownership type spec, for defining and managing Custom Ownership Types as code.


Here is an example of a custom ownership type named "Architect":

```json
{{ inline /metadata-ingestion/examples/ownership/ownership_type.json show_path_as_comment }}
```

To upload this file to DataHub, use the `datahub` cli via the `ingest` group of commands using the file-based recipe:
```yaml
# see https://datahubproject.io/docs/generated/ingestion/sources/file for complete documentation
source:
  type: "file"
  config:
    # path to json file
    path: "metadata-ingestion/examples/ownership/ownership_type.json"

# see https://datahubproject.io/docs/metadata-ingestion/sink_docs/datahub for complete documentation
sink:
  type: "datahub-rest"
  config:
    server: "http://localhost:9002/api/gms"
```

Finally running

```shell
datahub ingest -c recipe.yaml
```

For any update you wish to do, simply update the json file and re-ingest via the cli.

To delete the ownership type, simply run a [delete command](../how/delete-metadata.md#soft-delete-the-default) for the urn of the ownership type in question, in this case `urn:li:ownershipType:architect`.

  </TabItem>  
  <TabItem value="graphql" label="GraphQL" default>

You can also create/update/delete custom ownership types using DataHub's built-in [`GraphiQL` editor](../api/graphql/how-to-set-up-graphql.md#graphql-explorer-graphiql):

```json
mutation {
  createOwnershipType(
    input: {
      name: "Architect"
      description: "Technical person responsible for the asset"
    }
  ) {
    urn
    type
    info {
      name
    	description
    }
  }
}
```

If you see the following response, the operation was successful:

```json
{
  "data": {
    "createOwnershipType": {
      "urn": "urn:li:ownershipType:ccf9aa80-e3f3-4620-93a1-8d4a2ceaf5de",
      "type": "CUSTOM_OWNERSHIP_TYPE",
      "status": null,
      "info": {
        "name": "Architect",
        "description": "Technical person responsible for the asset",
        "created": null,
        "lastModified": null
      }
    }
  },
  "extensions": {}
}
```

There are also `updateOwnershipType`, `deleteOwnershipType` and `listOwnershipTypes` endpoints for CRUD operations. 

Feel free to read our [GraphQL reference documentation](../api/graphql/overview.md) on these endpoints.
  </TabItem>
</Tabs>


### Assigning a Custom Ownership Type to an Entity (UI)

You can assign an owner with a custom ownership type to an entity either using the Entity's page as the starting point.

On an Entity's profile page, use the right sidebar to locate the Owners section. 

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/ownership/ownership-type-set-part1.png" />
</p>

Click 'Add Owners', select the owner you want and then search for the Custom Ownership Type you'd like to add this asset to. When you're done, click 'Add'.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/ownership/ownership-type-set-part2.png" />
</p>

To remove ownership from an asset, click the 'x' icon on the Owner label.


> Notice: Adding or removing an Owner to an asset requires the `Edit Owners` Metadata Privilege, which can be granted
> by a [Policy](./../authorization/policies.md).



