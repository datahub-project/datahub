# Centralized Management

<FeatureAvailability saasOnly />

:::info
This feature is currently in private beta in DataHub Cloud. Reach out to your DataHub Cloud representative to learn more.
:::

## How It Works

Centralized Management allows governance of all physical children at the logical parent level. This means that metadata changes made to the logical model will be propagated to all its physical children.

<table>
<tr><th>Metadata at Logical Level</th><th>Change at Physical Level</th></tr>
<tr><td>Tags</td><td>✅ Tags are replicated on physical children.</td></tr>
<tr><td>Glossary Terms</td><td>✅ Terms are replicated on physical children.</td></tr>
<tr><td>Documentation</td><td>✅ The description shown on the logical parent is copied to physical children. If the child has its own description, that will be shown instead.</td></tr>
<tr><td>Ownership</td><td>✅️ Owners are replicated on physical children. If the same user is an owner for multiple ownership types, only one ownership type replicated.</td></tr>
<tr><td>Structured Properties</td><td>✅ Structured properties are replicated on physical children. If the child has an existing value for the same property, it will be replaced, even if that property is multi-valued.</td></tr>
<tr><td>Domains</td><td>❌ Domains are not propagated.</td></tr>
<tr><td>Data Products</td><td>❌ Data products are not propagated.</td></tr>
<tr><td>Data Quality Assertions</td><td>❌ Assertions are not propagated.</td></tr>
</table>

You can hover over propagated attributes to see when and from where this information came.

<p style={{ display: "flex", flexDirection: "row", justifyContent: "center", gap: "0px" }}>
    <img width="60%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/logical/description-propagated.png" />
    <img width="40%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/logical/tag-propagated.png" />
</p>

## Getting Started

Centralized Management requires two automations. They currently must be created manually. This can be done via the [GraphQL](../../../api/graphql) mutations below.

:::note
You can execute these mutations by visiting `<your-datahub-url>/api/graphiql`.
:::

```graphql
mutation UpsertLogicalModelsPropagationAutomation {
  upsertActionPipeline(
    urn: "urn:li:dataHubAction:logical-models"
    input: {
      name: "Logical Models"
      category: "System"
      type: "datahub_integrations.propagation.propagation_v2.propagation_v2_action.PropagationV2Action"
      description: "Propagation metadata from logical parents to their children"
      config: {
        recipe: """
        {
          "name": "logical models",
          "action": {
            "type": "datahub_integrations.propagation.propagation_v2.propagation_v2_action.PropagationV2Action",
            "config": {
              "enabled": true,
              "propagation_rule": {
                "metadata_propagated": {
                  "tags": {},
                  "terms": {},
                  "documentation": {},
                  "ownership": {},
                  "structured_properties": {}
                },
                "origin_urn_resolution": {
                  "lookup_type": "relationship",
                  "relationship_type": "PhysicalInstanceOf"
                },
                "target_urn_resolution": [
                  {
                    "lookup_type": "relationship",
                    "relationship_type": "PhysicalInstanceOf"
                  }
                ]
              }
            }
          }
        }
        """
        executorId: "default"
      }
    }
  )
}
```

:::note Logical Model Platform
If you are using a custom platform for your logical models, make sure to update the `query` field in the next mutation accordingly. If you have multiple logical platforms, you can specify multiple as so: `platform:(platformA platformB)`.
:::

```graphql
mutation UpsertSchemaFieldPropagationAutomation {
  upsertActionPipeline(
    urn: "urn:li:dataHubAction:schema-field"
    input: {
      name: "Schema Fields"
      category: "System"
      type: "datahub_integrations.propagation.propagation_v2.propagation_v2_action.PropagationV2Action"
      description: "Propagate tags, terms, and documentation on SchemaMetadata / EditableSchemaMetadata to aspects directly on schema fields"
      config: {
        recipe: """
        {
          "name": "schema fields",
          "action": {
            "type": "datahub_integrations.propagation.propagation_v2.propagation_v2_action.PropagationV2Action",
            "config": {
              "enabled": true,
              "propagation_rule": {
                "metadata_propagated": {
                  "tags": {
                    "omit_attribution_is_propagated": true
                  },
                  "terms": {
                    "omit_attribution_is_propagated": true
                  },
                  "documentation": {
                    "omit_attribution_is_propagated": true
                  }
                },
                "origin_urn_resolution": {
                  "lookup_type": "entity",
                  "entity_type": "dataset",
                  "query": "platform:logical"
                },
                "target_urn_resolution": "schema_field"
              }
            }
          }
        }
        """
        executorId: "default"
      }
    }
  )
}
```
