import logging
from enum import Enum
from typing import List, Optional

import cachetools
from datahub.utilities.urns.urn import Urn, guess_entity_type
from datahub_actions.api.action_graph import AcrylDataHubGraph
from datahub_actions.plugin.action.stats_util import ActionStageReport

from datahub_integrations.propagation.propagation.propagation_rule_config import (
    EntityLookup,
    PropagationRelationships,
    RelationshipLookup,
)
from datahub_integrations.propagation.propagation.propagation_strategy.base_strategy import (
    BaseStrategy,
    BaseStrategyConfig,
)
from datahub_integrations.propagation.propagation.propagation_utils import (
    DirectionType,
    PropagationDirective,
    RelationshipType,
    SourceDetails,
)
from datahub_integrations.propagation.propagation.propagator import (
    EntityPropagator,
    PropagationOutput,
)

logger = logging.getLogger(__name__)


class LineageBasedStrategyConfig(BaseStrategyConfig):
    """Configuration for the LineageBasedStrategy."""

    pass


class EntityType(str, Enum):
    """Enumeration of supported entity types for propagation."""

    SCHEMA_FIELD = "schemaField"
    DATASET = "dataset"
    CHART = "chart"


class LineageBasedStrategy(BaseStrategy):
    """
    Strategy for propagating metadata changes based on lineage relationships.
    """

    def __init__(
        self,
        graph: AcrylDataHubGraph,
        config: LineageBasedStrategyConfig,
        stats: ActionStageReport,
    ):
        super().__init__(graph, config, stats)
        self._upstream_cache: cachetools.TTLCache = cachetools.TTLCache(
            maxsize=1000, ttl=60 * 5
        )

    def propagate(
        self,
        entity_lookup: EntityLookup,
        propagator: EntityPropagator,
        directive: PropagationDirective,
        context: SourceDetails,
    ) -> PropagationOutput:
        """
        Propagate changes based on the provided directive and context.

        Args:
            propagator: The entity propagator to use for creating change proposals
            directive: The propagation directive containing the operation and entity info
            context: The source details context

        Returns:
            Iterable of metadata change proposals
        """
        assert isinstance(entity_lookup, RelationshipLookup)

        if entity_lookup.type not in [
            PropagationRelationships.DOWNSTREAM,
            PropagationRelationships.UPSTREAM,
        ]:
            return

        # Determine which directions to propagate based on current propagation state
        directions_to_process = self._get_propagation_directions(
            entity_lookup.type, context.propagation_direction
        )

        # Execute propagation for each allowed direction
        for direction in directions_to_process:
            yield from self._propagate_to_direction(
                propagator,
                directive,
                context,
                direction,
                entity_lookup.relationship_names,
            )

    def _get_propagation_directions(
        self,
        lookup_type: PropagationRelationships,
        current_direction: Optional[DirectionType],
    ) -> List[DirectionType]:
        """
        Determine which directions to propagate based on lookup type and current direction.

        Args:
            lookup_type: The type of relationship lookup (UPSTREAM or DOWNSTREAM)
            current_direction: The current propagation direction (None for first hop)

        Returns:
            List of directions to propagate to
        """
        # First hop: allow both directions based on lookup type
        if current_direction is None:
            directions = []
            if lookup_type == PropagationRelationships.DOWNSTREAM:
                directions.append(DirectionType.DOWN)
            if lookup_type == PropagationRelationships.UPSTREAM:
                directions.append(DirectionType.UP)
            return directions

        # Subsequent hops: maintain directional consistency
        if (
            lookup_type == PropagationRelationships.DOWNSTREAM
            and current_direction == DirectionType.DOWN
        ):
            return [DirectionType.DOWN]

        if (
            lookup_type == PropagationRelationships.UPSTREAM
            and current_direction == DirectionType.UP
        ):
            return [DirectionType.UP]

        # Direction mismatch: no propagation
        return []

    def _propagate_to_direction(
        self,
        propagator: EntityPropagator,
        directive: PropagationDirective,
        context: SourceDetails,
        direction: DirectionType,
        relationship_types: List[str],
    ) -> PropagationOutput:
        """
        Propagate the documentation to upstream or downstream entities.

        Args:
            propagator: The entity propagator
            directive: The propagation directive
            context: The source details
            direction: The direction (UP or DOWN)

        Returns:
            Iterable of metadata change proposals
        """
        logger.debug(
            f"Propagating to {direction} for {directive.entity}, "
            f"context: {context} with propagator {propagator.__class__.__name__}"
        )

        if not self.graph:
            logger.warning("Graph is not available for propagation")
            return

        # Get entities in the requested direction
        lineage_entities = self._get_lineage_entities(
            directive.entity, direction, relationship_types
        )
        entity_type = guess_entity_type(directive.entity)

        # Create a propagated context
        propagated_context = self._create_propagated_context(context, direction)

        if entity_type == EntityType.SCHEMA_FIELD:
            yield from self._propagate_schema_field(
                propagator,
                directive,
                propagated_context,
                lineage_entities,
                direction,
                relationship_types,
            )
        elif entity_type == EntityType.DATASET:
            yield from self._propagate_dataset(
                propagator, directive, propagated_context, lineage_entities
            )
        else:
            logger.warning(
                f"Unsupported entity type {entity_type} for {directive.entity}"
            )

    def _create_propagated_context(
        self, context: SourceDetails, direction: DirectionType
    ) -> SourceDetails:
        """Create a new context with propagation details."""
        propagated_context = SourceDetails.model_validate(context.model_dump())
        propagated_context.propagation_relationship = RelationshipType.LINEAGE

        # For first hop, set the direction; for subsequent hops, preserve original direction
        if context.propagation_direction is None:
            propagated_context.propagation_direction = direction  # First hop
        else:
            # Keep existing direction for consistency in multi-hop propagation
            propagated_context.propagation_direction = context.propagation_direction

        return propagated_context

    def _get_lineage_entities(
        self,
        entity_urn: str,
        direction_type: DirectionType,
        relationship_types: List[str],
    ) -> List[str]:
        """Get lineage entities in the specified direction."""

        direction: str
        if direction_type == DirectionType.DOWN:
            direction = "INCOMING"
        elif direction_type == DirectionType.UP:
            direction = "OUTGOING"
        else:
            raise ValueError(f"Invalid direction: {direction_type}")

        relationships = self.graph.get_relationships(
            entity_urn=entity_urn,
            direction=direction,
            relationship_types=relationship_types,
        )

        return relationships

    def _propagate_schema_field(
        self,
        propagator: EntityPropagator,
        directive: PropagationDirective,
        context: SourceDetails,
        lineage_entities: List[str],
        direction: DirectionType,
        relationship_types: List[str],
    ) -> PropagationOutput:
        """
        Propagate changes for schema field entities.

        Args:
            propagator: The entity propagator
            directive: The propagation directive
            context: The source context
            lineage_entities: List of lineage entities
            direction: Direction of propagation

        Returns:
            Iterable of metadata change proposals
        """
        # Filter schema field entities
        lineage_fields = {
            x
            for x in lineage_entities
            if guess_entity_type(x) == EntityType.SCHEMA_FIELD
        }

        # Skip propagation for upstream direction if there's not exactly one upstream field
        if direction == DirectionType.UP and len(lineage_fields) != 1:
            logger.info(
                f"Skipping upstream propagation - found {len(lineage_fields)} upstream fields"
            )
            return

        propagated_count = 0

        for field in lineage_fields:
            schema_field_urn = Urn.from_string(field)
            parent_urn = schema_field_urn.entity_ids[0]
            field_path = schema_field_urn.entity_ids[1]
            parent_entity_type = guess_entity_type(parent_urn)

            logger.debug(
                f"Will {directive.operation} directive: {directive} for {field_path} on {schema_field_urn}"
            )

            if parent_entity_type == EntityType.DATASET:
                # Check if propagation is allowed based on upstream fields
                if self._should_propagate_field(
                    propagator,
                    schema_field_urn,
                    directive.entity,
                    direction,
                    relationship_types,
                ):
                    # Check fan-out limits
                    if propagated_count >= self.config.max_propagation_fanout:
                        logger.warning(
                            f"Exceeded max propagation fanout of {self.config.max_propagation_fanout}. "
                            f"Skipping propagation to {'downstream' if direction == DirectionType.DOWN else 'upstream'} {field}"
                        )
                        return

                    # Skip self-propagation
                    if context.origin == schema_field_urn:
                        continue

                    # Create change proposal
                    yield from propagator.create_property_change_proposal(
                        directive,
                        schema_field_urn,
                        context=context,
                    )
                    propagated_count += 1
                    self._stats.increment_assets_impacted(field)
            elif parent_entity_type == EntityType.CHART:
                logger.warning(
                    "Charts are expected to have fields that are dataset schema fields. Skipping for now..."
                )

    def _propagate_dataset(
        self,
        propagator: EntityPropagator,
        directive: PropagationDirective,
        context: SourceDetails,
        lineage_entities: List[str],
    ) -> PropagationOutput:
        """
        Propagate changes for dataset entities.

        Args:
            propagator: The entity propagator
            directive: The propagation directive
            context: The source context
            lineage_entities: List of lineage entities

        Returns:
            Iterable of metadata change proposals
        """
        # Filter dataset entities
        lineage_datasets = {
            x for x in lineage_entities if guess_entity_type(x) == EntityType.DATASET
        }

        propagated_count = 0

        for dataset in lineage_datasets:
            # Check fan-out limits
            if propagated_count >= self.config.max_propagation_fanout:
                logger.warning(
                    f"Exceeded max propagation fanout of {self.config.max_propagation_fanout}. "
                    f"Skipping remaining dataset propagations."
                )
                return

            # Skip self-propagation
            if context.origin == dataset:
                continue

            dataset_urn = Urn.from_string(dataset)
            logger.debug(
                f"Will {directive.operation} directive: {directive} for {dataset_urn}"
            )

            yield from propagator.create_property_change_proposal(
                directive,
                dataset_urn,
                context=context,
            )
            self._stats.increment_assets_impacted(dataset)
            propagated_count += 1

    def _should_propagate_field(
        self,
        propagator: EntityPropagator,
        field_urn: Urn,
        entity_urn: str,
        direction: DirectionType,
        relationship_types: List[str],
    ) -> bool:
        """
        Determine if propagation should be allowed for the given field.

        Args:
            propagator: The entity propagator
            field_urn: The field URN
            entity_urn: The entity URN
            direction: Direction of propagation

        Returns:
            True if propagation is allowed, False otherwise
        """
        if not propagator.only_one_upstream_allowed:
            return True

        downstream_field = (
            str(field_urn) if direction == DirectionType.DOWN else entity_urn
        )
        upstream_field = (
            entity_urn if direction == DirectionType.DOWN else str(field_urn)
        )

        return self._only_one_upstream_field(
            downstream_field, upstream_field, relationship_types
        )

    def _only_one_upstream_field(
        self, downstream_field: str, upstream_field: str, relationship_types: List[str]
    ) -> bool:
        """
        Check if there is only one upstream field for the downstream field.

        Args:
            downstream_field: The downstream field URN
            upstream_field: The upstream field URN

        Returns:
            True if there's exactly one upstream field that matches the expected upstream, False otherwise
        """
        upstream_fields = self._get_upstream_fields(
            downstream_field, relationship_types
        )

        # Skip if no upstream fields found
        if not upstream_fields:
            logger.debug(
                f"No upstream fields found. Skipping propagation to downstream {downstream_field}"
            )
            return False

        # Check if there's exactly one upstream field and it matches the expected one
        result = len(upstream_fields) == 1 and upstream_fields[0] == upstream_field
        if not result:
            logger.warning(
                f"Failed check for single upstream: Found upstream fields {upstream_fields} for "
                f"downstream {downstream_field}. Expecting only one upstream field: {upstream_field}"
            )
        return result

    def _get_upstream_fields(
        self, entity_urn: str, relationship_types: List[str]
    ) -> List[str]:
        """
        Get the upstream schema fields for an entity.

        Args:
            entity_urn: The entity URN

        Returns:
            List of upstream schema field URNs
        """
        upstreams = self.get_upstreams_cached(
            entity_urn=entity_urn, relationship_types=relationship_types
        )
        # Filter and deduplicate schema fields
        return list(
            {x for x in upstreams if guess_entity_type(x) == EntityType.SCHEMA_FIELD}
        )

    @cachetools.cachedmethod(
        cache=lambda self: self._upstream_cache,
        key=lambda self, entity_urn, relationship_types: (
            entity_urn,
            tuple(relationship_types),
        ),
    )
    def get_upstreams_cached(
        self, entity_urn: str, relationship_types: List[str]
    ) -> List[str]:
        """Get the upstream entities from cache."""
        return self.graph.get_relationships(
            entity_urn=entity_urn,
            direction="OUTGOING",
            relationship_types=relationship_types,
        )
