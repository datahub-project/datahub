# from datahub_actions.api.action_graph import AcrylDataHubGraph
#
# from datahub_integrations.actions.oss.stats_util import ActionStageReport
# from datahub_integrations.propagation.propagation_v2.config.propagation_rule import (
#     AspectLookupConfig,
# )
# from datahub_integrations.propagation.propagation_v2.lookups.base_lookup import (
#     BaseLookup,
# )
#
#
# class AspectLookup(BaseLookup):
#     def __init__(
#         self,
#         config: AspectLookupConfig,
#         graph: AcrylDataHubGraph,
#         report: ActionStageReport,
#     ):
#         super().__init__(graph, report)
#         self.config = config
#
#     def resolve(self) -> set[str]:
#         pass
#
#     def resolve_batch(self) -> dict[str, set[str]]:
#         pass


# def get_urns_from_aspect(
#     graph: DataHubGraph, entity_urn: str, aspect_lookup: AspectLookup
# ) -> list[str]:
#     """
#     Get urn(s) from aspect field
#     """
#
#     entity_field_path: Optional[str] = None
#     urns_to_return = []
#
#     if guess_entity_type(entity_urn) == "schemaField":
#         schema_urn = SchemaFieldUrn.from_string(entity_urn)
#         parent_urn = schema_urn.parent
#         entity_field_path = schema_urn.field_path
#     else:
#         parent_urn = entity_urn
#
#     if aspect_lookup.aspect_name not in models.__SCHEMA_TYPES:
#         logger.warning(f"Aspect {aspect_lookup.aspect_name} not found.")
#         return []
#
#     logger.debug(f"Looking for aspect {aspect_lookup.aspect_name} in {parent_urn}")
#     aspect = graph.graph.get_aspect(
#         parent_urn,
#         models.__SCHEMA_TYPES.get(aspect_lookup.aspect_name),
#     )
#
#     if not aspect:
#         return []
#
#     fields = aspect_lookup.field.split(".")
#     logger.debug(f"Looking for field {fields} in {aspect}")
#
#     current = aspect.to_obj()
#     if len(fields) > 1:
#         # Navigate through all fields except the last one
#         for field in fields[:-1]:
#             if field not in current or current[field] is None:
#                 return []
#             current = current[field]
#
#         # Get the value of the last field
#         field_value = current.get(fields[-1])
#     else:
#         # If there's only one field, get it directly
#         field_value = aspect.get(aspect_lookup.field)
#
#     logger.debug(f"Found field value {field_value}")
#     if field_value:
#         if isinstance(field_value, str):
#             urns = [field_value]
#         elif isinstance(field_value, list):
#             urns = [x for x in field_value if x != parent_urn]
#         else:
#             raise ValueError(f"Unexpected field value type: {type(field_value)}")
#
#         if not entity_field_path:
#             return urns
#
#         for target_urn in urns:
#             # if entity_field_path is set and the target urn is a dataset that means we are looking for a field
#             # now we need to find the schema field
#             if guess_entity_type(target_urn) == "dataset":
#                 schema_fields = graph.graph.get_aspect(
#                     target_urn, models.SchemaMetadataClass
#                 )
#                 if schema_fields:
#                     for schema_field in schema_fields.fields:
#                         if schema_field.fieldPath == entity_field_path:
#                             schema_field_urn = make_schema_field_urn(
#                                 target_urn, schema_field.fieldPath
#                             )
#                             urns_to_return.append(schema_field_urn)
#                             break
#             else:
#                 urns_to_return.append(target_urn)
#     return urns_to_return
