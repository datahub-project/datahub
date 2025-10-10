import logging
from typing import List, Optional

import datahub.metadata.schema_classes as models
from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.metadata.urns import SchemaFieldUrn
from datahub.utilities.urns.urn import Urn, guess_entity_type
from datahub_actions.api.action_graph import AcrylDataHubGraph
from datahub_actions.plugin.action.stats_util import ActionStageReport

from datahub_integrations.propagation.propagation.propagation_rule_config import (
    AspectLookup,
    EntityLookup,
)
from datahub_integrations.propagation.propagation.propagation_strategy.base_strategy import (
    BaseStrategy,
    BaseStrategyConfig,
)
from datahub_integrations.propagation.propagation.propagation_utils import (
    PropagationDirective,
    SourceDetails,
)
from datahub_integrations.propagation.propagation.propagator import (
    EntityPropagator,
    PropagationOutput,
)

logger = logging.getLogger(__name__)


class AspectBasedStrategyConfig(BaseStrategyConfig):
    entity_types: List[str] = ["dataset", "schemaField"]


class AspectBasedStrategy(BaseStrategy):
    def __init__(
        self,
        config: AspectBasedStrategyConfig,
        graph: AcrylDataHubGraph,
        stats: ActionStageReport,
    ):
        super().__init__(graph, config, stats)

    def propagate(
        self,
        entity_lookup: EntityLookup,
        propagator: EntityPropagator,
        directive: PropagationDirective,
        context: SourceDetails,
    ) -> PropagationOutput:
        assert isinstance(entity_lookup, AspectLookup)
        assert self.graph
        assert isinstance(self.config, AspectBasedStrategyConfig)

        urns = get_urns_from_aspect(self.graph, directive.entity, entity_lookup)
        for urn in set(urns):
            if guess_entity_type(urn) in self.config.entity_types:
                maybe_mcp = propagator.create_property_change_proposal(
                    directive, Urn.from_string(urn), context
                )
                if maybe_mcp:
                    yield from maybe_mcp


def get_urns_from_aspect(
    graph: AcrylDataHubGraph, entity_urn: str, aspect_lookup: AspectLookup
) -> list[str]:
    """
    Get urn(s) from aspect field
    """

    entity_field_path: Optional[str] = None
    urns_to_return = []

    if guess_entity_type(entity_urn) == "schemaField":
        schema_urn = SchemaFieldUrn.from_string(entity_urn)
        parent_urn = schema_urn.parent
        entity_field_path = schema_urn.field_path
    else:
        parent_urn = entity_urn

    if aspect_lookup.aspect_name not in models.__SCHEMA_TYPES:  # type: ignore[attr-defined]  # Private attribute exists at runtime
        logger.warning(f"Aspect {aspect_lookup.aspect_name} not found.")
        return []

    logger.debug(f"Looking for aspect {aspect_lookup.aspect_name} in {parent_urn}")
    aspect = graph.graph.get_aspect(
        parent_urn,
        models.__SCHEMA_TYPES.get(aspect_lookup.aspect_name),  # type: ignore[attr-defined]  # Private attribute exists at runtime
    )

    if not aspect:
        return []

    fields = aspect_lookup.field.split(".")
    logger.debug(f"Looking for field {fields} in {aspect}")

    current = aspect.to_obj()
    if len(fields) > 1:
        # Navigate through all fields except the last one
        for field in fields[:-1]:
            if field not in current or current[field] is None:
                return []
            current = current[field]

        # Get the value of the last field
        field_value = current.get(fields[-1])
    else:
        # If there's only one field, get it directly
        field_value = aspect.get(aspect_lookup.field)

    logger.debug(f"Found field value {field_value}")
    if field_value:
        if isinstance(field_value, str):
            urns = [field_value]
        elif isinstance(field_value, list):
            urns = [x for x in field_value if x != parent_urn]
        else:
            raise ValueError(f"Unexpected field value type: {type(field_value)}")

        if not entity_field_path:
            return urns

        for target_urn in urns:
            # if entity_field_path is set and the target urn is a dataset that means we are looking for a field
            # now we need to find the schema field
            if guess_entity_type(target_urn) == "dataset":
                schema_fields = graph.graph.get_aspect(
                    target_urn, models.SchemaMetadataClass
                )
                if schema_fields:
                    for schema_field in schema_fields.fields:
                        if schema_field.fieldPath == entity_field_path:
                            schema_field_urn = make_schema_field_urn(
                                target_urn, schema_field.fieldPath
                            )
                            urns_to_return.append(schema_field_urn)
                            break
            else:
                urns_to_return.append(target_urn)
    return urns_to_return
