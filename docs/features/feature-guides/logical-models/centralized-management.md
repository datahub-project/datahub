# Centralized Management

:::info
This feature is currently in private beta in DataHub Cloud. Reach out to your DataHub Cloud representative to learn more.
:::

## How It Works

Centralized Management allows governance of all physical children at the logical parent level. This means that metadata changes made to the logical model will be propagated to all its physical children.

:::note
Centralized Management support is currently limited to documentation, tags, glossary terms, ownership, and structured properties. Domains, data products, and data quality assertions are not currently supported.
:::

You can hover over propagated attributes to see when and from where this information came.

<p align="center">
    <img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/logical/description-propagated.png" />
    <img width="80%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/logical/tag-propagated.png" />
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
        {"name": "logical models", "action": {"type": "datahub_integrations.propagation.propagation_v2.propagation_v2_action.PropagationV2Action", "config": {"enabled": true, "propagation_rule": {"metadata_propagated": {"tags": {}, "terms": {}, "documentation": {}, "ownership": {}, "structured_properties": {}}, "origin_urn_resolution": {"lookup_type": "relationship", "relationship_type": "PhysicalInstanceOf"}, "target_urn_resolution": [{"lookup_type": "relationship", "relationship_type": "PhysicalInstanceOf"}]}}}}
        """
        executorId: "default"
      }
    }
  )
}
```

```graphql
mutation UpsertSchemaFieldPropagationAutomation {
  upsertActionPipeline(
    urn: "urn:li:dataHubAction:schema-field"
    input: {
      name: "Schema Fields"
      category: "System"
      type: "datahub_integrations.propagation.propagation_v2.propagation_v2_action.PropagationV2Action"
      description: "Propagation tags and terms on SchemaMetadata / EditableSchemaMetadata to aspects directly on schema fields"
      config: {
        recipe: """
        {"name": "schema fields", "action": {"type": "datahub_integrations.propagation.propagation_v2.propagation_v2_action.PropagationV2Action", "config": {"enabled": true, "propagation_rule": {"metadata_propagated": {"tags": {"omit_attribution_is_propagated": true}, "terms": {"omit_attribution_is_propagated": true}, "documentation": {"omit_attribution_is_propagated": true}}, "origin_urn_resolution": {"lookup_type": "entity", "entity_type": "dataset", "query":"platform:logical"}, "target_urn_resolution": "schema_field"}}}}
        """
        executorId: "default"
      }
    }
  )
}
```
