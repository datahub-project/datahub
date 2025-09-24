"""Scenario builders for easily creating propagation test scenarios.

These builders use a fluent API to make test creation more readable and maintainable.
"""

import datetime
import logging
from typing import Any, Dict, List, Optional, Union

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AuditStampClass,
    EdgeClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    LogicalParentClass,
    SiblingsClass,
    StatusClass,
)
from datahub.metadata.urns import DatasetUrn
from tests.propagation.framework.core.models import PropagationTestScenario
from tests.propagation.framework.plugins.structured_properties.models import (
    DatasetStructuredProperties,
)
from tests.propagation.framework.plugins.structured_properties.templates import (
    create_structured_property_template,
)
from tests.propagation.framework.utils.graph_utils import LineageGraphBuilder

logger = logging.getLogger(__name__)


class DatasetBuilder:
    """Builder for creating datasets with metadata using explicit column names."""

    def __init__(self, graph_builder: LineageGraphBuilder, platform: str, name: str):
        self.graph_builder = graph_builder
        self.platform = platform
        self.name = name
        self.description: Optional[str] = None
        self.column_names: List[str] = []
        self.field_descriptions: Dict[str, str] = {}
        self.field_glossary_terms: Dict[str, List[str]] = {}
        self.field_tags: Dict[str, str] = {}
        self.structured_properties = DatasetStructuredProperties(name, [])
        self.subtype: Optional[str] = None
        self.sibling_urns: List[str] = []
        self.is_primary_sibling: bool = True
        self.logical_parent_urn: Optional[str] = None
        self.field_logical_parents: Dict[str, str] = {}

    def with_description(self, description: str) -> "DatasetBuilder":
        """Set dataset description."""
        self.description = description
        return self

    def with_columns(self, column_names: List[str]) -> "DatasetBuilder":
        """Set the column names for this dataset. This replaces with_field_count."""
        self.column_names = column_names[:]  # Copy the list
        return self

    def with_column_description(
        self, column_name: str, description: str
    ) -> "DatasetBuilder":
        """Set description for a specific column by name."""
        if column_name not in self.column_names:
            raise ValueError(
                f"Column '{column_name}' not found. Available columns: {self.column_names}"
            )
        self.field_descriptions[column_name] = description
        return self

    def with_column_glossary_term(
        self, column_name: str, term: str
    ) -> "DatasetBuilder":
        """Set glossary term for a specific column by name."""
        if column_name not in self.column_names:
            raise ValueError(
                f"Column '{column_name}' not found. Available columns: {self.column_names}"
            )
        # Store as single-item list to maintain consistent structure
        self.field_glossary_terms[column_name] = [term]
        return self

    def with_column_glossary_terms(
        self, column_name: str, terms: Union[str, List[str]]
    ) -> "DatasetBuilder":
        """Set multiple glossary terms for a specific column by name."""
        if column_name not in self.column_names:
            raise ValueError(
                f"Column '{column_name}' not found. Available columns: {self.column_names}"
            )
        # Handle both single term and list of terms
        if isinstance(terms, str):
            self.field_glossary_terms[column_name] = [terms]
        else:
            self.field_glossary_terms[column_name] = terms[:]  # Copy the list
        return self

    def add_column_glossary_term(self, column_name: str, term: str) -> "DatasetBuilder":
        """Add an additional glossary term to a column (append to existing terms)."""
        if column_name not in self.column_names:
            raise ValueError(
                f"Column '{column_name}' not found. Available columns: {self.column_names}"
            )
        if column_name not in self.field_glossary_terms:
            self.field_glossary_terms[column_name] = []
        self.field_glossary_terms[column_name].append(term)
        return self

    def with_column_tag(self, column_name: str, tag: str) -> "DatasetBuilder":
        """Set tag for a specific column by name."""
        if column_name not in self.column_names:
            raise ValueError(
                f"Column '{column_name}' not found. Available columns: {self.column_names}"
            )
        self.field_tags[column_name] = tag
        return self

    def with_structured_property(
        self, property_name_or_urn: str, value: Any, value_type: str = "string"
    ) -> "DatasetBuilder":
        """Set a structured property for this dataset.

        Args:
            property_name_or_urn: Name or URN of the structured property definition
            value: The value to set for the property
            value_type: Type of the value (string, number, date, urn)
        """
        self.structured_properties.add_property(property_name_or_urn, value, value_type)
        return self

    def add_structured_property(
        self, property_name_or_urn: str, value: Any, value_type: str = "string"
    ) -> "DatasetBuilder":
        """Add a structured property (alias for with_structured_property for consistency)."""
        return self.with_structured_property(property_name_or_urn, value, value_type)

    def with_subtype(self, subtype: str) -> "DatasetBuilder":
        """Set dataset subtype."""
        self.subtype = subtype
        return self

    def set_sibling(
        self, sibling_dataset_or_urn, is_primary: bool = True
    ) -> "DatasetBuilder":
        """Set sibling relationship for this dataset.

        Args:
            sibling_dataset_or_urn: Either a dataset object with .urn attribute or a URN string
            is_primary: Whether this dataset should be marked as primary in the sibling relationship
        """
        if hasattr(sibling_dataset_or_urn, "urn"):
            # It's a dataset object
            sibling_urn = sibling_dataset_or_urn.urn
        else:
            # It's already a URN string
            sibling_urn = sibling_dataset_or_urn

        self.sibling_urns.append(sibling_urn)
        self.is_primary_sibling = is_primary
        return self

    def set_logical_parent(
        self, parent_dataset_or_urn: Union[DatasetUrn, str]
    ) -> "DatasetBuilder":
        """Set logical parent relationship for this dataset.

        Args:
            parent_dataset_or_urn: Either a dataset object with .urn attribute, DatasetUrn instance, or URN string
        """
        if isinstance(parent_dataset_or_urn, DatasetUrn):
            # It's a DatasetUrn instance
            parent_urn = str(parent_dataset_or_urn)
        elif isinstance(
            parent_dataset_or_urn, str
        ) and parent_dataset_or_urn.startswith("urn:li:"):
            # It's a URN string
            parent_urn = parent_dataset_or_urn
        else:
            raise ValueError(
                f"parent_dataset_or_urn must be a DatasetUrn instance or a valid URN string starting with 'urn:li:', got: {parent_dataset_or_urn}"
            )

        self.logical_parent_urn = parent_urn
        return self

    def add_field_logical_parent(
        self, field_name: str, parent_field_urn: str
    ) -> "DatasetBuilder":
        """Add a field-level logical parent relationship.

        This creates a field URN -> field URN logical relationship where a specific field
        in this dataset has a logical parent field in another dataset.

        Args:
            field_name: The name of the field in this dataset that will have a logical parent
            parent_field_urn: The URN of the parent field (must be a field URN)
        """
        if not parent_field_urn.startswith("urn:li:schemaField:"):
            raise ValueError(
                f"parent_field_urn must be a field URN starting with 'urn:li:schemaField:', got: {parent_field_urn}"
            )

        if field_name not in self.column_names:
            raise ValueError(
                f"Field '{field_name}' not found. Available fields: {self.column_names}"
            )

        self.field_logical_parents[field_name] = parent_field_urn
        return self

    def build(self) -> Any:
        """Build the dataset."""
        if not self.column_names:
            raise ValueError(
                "No columns defined. Use with_columns() to set actual column names first."
            )

        num_fields = len(self.column_names)
        field_descriptions_list: List[str] = []
        field_glossary_terms_list: List[str] = []
        field_tags_list: List[str] = []

        for column_name in self.column_names:
            # For descriptions
            if column_name in self.field_descriptions:
                field_descriptions_list.append(self.field_descriptions[column_name])

            # For glossary terms - maintain field-term correspondence
            if column_name in self.field_glossary_terms:
                # For now, just take the first term to maintain compatibility with original Dataset API
                # The original Dataset API expects a flat list where each position corresponds to a field
                field_glossary_terms_list.append(
                    self.field_glossary_terms[column_name][0]
                )
            # Note: If we need multiple terms per field in the future, we'd need to enhance
            # the underlying Dataset creation API as well

            # For tags
            if column_name in self.field_tags:
                field_tags_list.append(self.field_tags[column_name])

        dataset = self.graph_builder.create_dataset(
            platform=self.platform,
            name=self.name,
            description=self.description or f"Dataset {self.name}",
            num_fields=num_fields,
            field_names=self.column_names,  # Pass actual field names
            field_descriptions=field_descriptions_list,
            field_glossary_terms=field_glossary_terms_list,
            field_global_tags=field_tags_list,
            subtype=self.subtype,
        )

        # Store sibling info on the dataset for the scenario builder to use
        if self.sibling_urns:
            dataset.sibling_info = (self.sibling_urns, self.is_primary_sibling)
        else:
            dataset.sibling_info = None

        # Store logical parent info on the dataset for the scenario builder to use
        dataset.logical_parent_urn = self.logical_parent_urn

        # Store field logical parent relationships on the dataset for the scenario builder to use
        dataset.field_logical_parents = self.field_logical_parents

        # Convert structured properties to DataHub Dataset format if any exist
        if self.structured_properties.has_properties():
            structured_props_dict = {}
            for prop in self.structured_properties.properties:
                # Resolve name to URN using consistent pattern
                if prop.name_or_urn.startswith("urn:li:structuredProperty:"):
                    property_urn = prop.name_or_urn
                elif prop.name_or_urn.startswith("urn:"):
                    property_urn = prop.name_or_urn
                else:
                    # Assume it's a simple name and convert to our standard URN pattern
                    property_urn = (
                        f"urn:li:structuredProperty:io.datahub.test.{prop.name_or_urn}"
                    )

                structured_props_dict[property_urn] = prop.format_value()

            dataset.structured_properties = structured_props_dict

        return dataset


class LineageBuilder:
    """Builder for creating lineage relationships."""

    def __init__(self, graph_builder: LineageGraphBuilder):
        self.graph_builder = graph_builder
        self.field_mappings: List[tuple[List[str], List[str]]] = []
        self.lineage_type: str = "TRANSFORMED"
        self.registered_datasets: Dict[str, Any] = {}

    def set_registered_datasets(self, datasets: Dict[str, Any]) -> None:
        """Set the registered datasets from the scenario builder."""
        self.registered_datasets = datasets

    def add_field_lineage(
        self,
        upstream_dataset: str,
        upstream_field: str,
        downstream_dataset: str,
        downstream_field: str,
    ) -> "LineageBuilder":
        """Add a 1:1 field lineage relationship using registered dataset names."""
        # Resolve dataset info from registered datasets
        upstream_platform, upstream_name = self._resolve_dataset_info(upstream_dataset)
        downstream_platform, downstream_name = self._resolve_dataset_info(
            downstream_dataset
        )

        upstream_urn = self.graph_builder.get_field_urn(
            upstream_platform,
            upstream_name,
            upstream_field,
        )
        downstream_urn = self.graph_builder.get_field_urn(
            downstream_platform,
            downstream_name,
            downstream_field,
        )
        self.field_mappings.append(([upstream_urn], [downstream_urn]))
        return self

    def add_many_to_one_lineage(
        self,
        upstream_dataset: str,
        upstream_fields: List[str],
        downstream_dataset: str,
        downstream_field: str,
    ) -> "LineageBuilder":
        """Add an N:1 field lineage relationship using registered dataset names."""
        # Resolve dataset info from registered datasets
        upstream_platform, upstream_name = self._resolve_dataset_info(upstream_dataset)
        downstream_platform, downstream_name = self._resolve_dataset_info(
            downstream_dataset
        )

        upstream_urns = [
            self.graph_builder.get_field_urn(
                upstream_platform,
                upstream_name,
                field,
            )
            for field in upstream_fields
        ]
        downstream_urn = self.graph_builder.get_field_urn(
            downstream_platform,
            downstream_name,
            downstream_field,
        )
        self.field_mappings.append((upstream_urns, [downstream_urn]))
        return self

    def with_lineage_type(self, lineage_type: str) -> "LineageBuilder":
        """Set the lineage type (TRANSFORMED, COPY, etc.)."""
        self.lineage_type = lineage_type
        return self

    def build(self, upstream_dataset_urn: str):
        """Build the lineage aspect."""
        return self.graph_builder.create_lineage_aspect(
            upstream_dataset_urn,
            lineage_type=self.lineage_type,
            field_mappings=self.field_mappings,
        )

    def _resolve_dataset_info(self, dataset_name: str) -> tuple[str, str]:
        """Resolve dataset info using registered datasets."""
        if dataset_name not in self.registered_datasets:
            raise ValueError(
                f"Dataset '{dataset_name}' not found in registered datasets. Available datasets: {list(self.registered_datasets.keys())}"
            )

        dataset = self.registered_datasets[dataset_name]

        if not hasattr(dataset, "platform") or not hasattr(dataset, "name"):
            raise ValueError(
                f"Dataset '{dataset_name}' is missing required 'platform' or 'name' attributes"
            )

        # Note: dataset.name contains the unprefixed name, graph_builder will add prefix
        return dataset.platform, dataset.name


class PropagationScenarioBuilder:
    """Fluent builder for creating propagation test scenarios.

    Makes test creation much more readable and maintainable.
    """

    def __init__(self, test_action_urn: str, prefix: str = "test"):
        self.test_action_urn = test_action_urn
        self.graph_builder = LineageGraphBuilder(prefix)
        self.datasets: Dict[str, Any] = {}
        self.lineage_relationships: List[
            tuple[str, str, Any]
        ] = []  # (upstream, downstream, aspect)
        self.base_expectations: List[Any] = []
        self.mutations: List[MetadataChangeProposalWrapper] = []
        self.post_mutation_expectations: List[Any] = []
        self.run_bootstrap: bool = True
        self.debug_mcps: bool = False
        self.verbose_mode: bool = True
        self.cleanup_entities: bool = True
        self.structured_property_templates: List[
            MetadataChangeProposalWrapper
        ] = []  # Property definition templates
        self.available_structured_properties: Dict[
            str, str
        ] = {}  # {property_name: property_urn}

    def add_dataset(self, name: str, platform: str = "snowflake") -> DatasetBuilder:
        """Add a dataset with fluent configuration."""
        return DatasetBuilder(self.graph_builder, platform, name)

    def register_dataset(self, name: str, dataset) -> "PropagationScenarioBuilder":
        """Register a built dataset."""
        self.datasets[name] = dataset

        # Store structured properties if provided and has properties
        # TODO: Handle structured properties via dataset.structured_properties when dataset supports it

        if self.verbose_mode:
            # Get field glossary terms from the schema_metadata if available
            field_glossary_terms_dict = {}  # Dict[str, List[str]] - proper structure
            if dataset.schema_metadata and dataset.schema_metadata.fields:
                for field in dataset.schema_metadata.fields:
                    if (
                        field.glossaryTerms
                        and isinstance(field.glossaryTerms, list)
                        and field.glossaryTerms
                    ):
                        # Store as list to preserve original structure
                        field_glossary_terms_dict[field.id] = field.glossaryTerms

            terms_with_columns = []
            for col, term_list in field_glossary_terms_dict.items():
                # Handle each term in the list for this column
                for term in term_list:
                    term_name = term.split(":")[-1]
                    terms_with_columns.append(f"{col}:{term_name}")
            terms_str = (
                f" with glossary terms {terms_with_columns}"
                if terms_with_columns
                else ""
            )
            num_columns = (
                len(dataset.schema_metadata.fields)
                if dataset.schema_metadata and dataset.schema_metadata.fields
                else 0
            )
            logger.info(
                f"📊 Registered dataset '{name}' on platform '{dataset.platform}' with {num_columns} columns{terms_str}"
            )
        return self

    def define_structured_property(
        self,
        name: str,
        display_name: str,
        value_type: str = "string",
        description: Optional[str] = None,
        allowed_values: Optional[List[str]] = None,
        cardinality: str = "SINGLE",
    ) -> "PropagationScenarioBuilder":
        """Define a structured property template that will be created during scenario build."""
        template = create_structured_property_template(
            qualified_name=f"io.datahub.test.{name}",
            display_name=display_name,
            value_type=value_type,
            description=description,
            allowed_values=allowed_values,
            cardinality=cardinality,
        )

        self.structured_property_templates.append(template)
        # Map the name to the URN for easy reference
        urn = f"urn:li:structuredProperty:io.datahub.test.{name}"
        self.available_structured_properties[name] = urn

        return self

    def register_structured_property(
        self, name: str, urn: str
    ) -> "PropagationScenarioBuilder":
        """Register a structured property that's already ingested (for external properties)."""
        self.available_structured_properties[name] = urn
        return self

    def add_lineage(
        self, upstream_dataset: str, downstream_dataset: str
    ) -> LineageBuilder:
        """Add lineage between datasets with fluent configuration."""
        lineage_builder = LineageBuilder(self.graph_builder)
        lineage_builder.set_registered_datasets(self.datasets)
        return lineage_builder

    def register_lineage(
        self, upstream_dataset: str, downstream_dataset: str, lineage_aspect
    ) -> "PropagationScenarioBuilder":
        """Register a built lineage relationship."""
        self.lineage_relationships.append(
            (upstream_dataset, downstream_dataset, lineage_aspect)
        )

        if self.verbose_mode:
            fine_grained_lineages = lineage_aspect.fineGrainedLineages or []
            logger.info(
                f"📊 Created lineage aspect with {len(fine_grained_lineages)} fine-grained lineages"
            )

            if fine_grained_lineages:
                for i, fgl in enumerate(fine_grained_lineages):
                    upstream_urn = fgl.upstreams[0]
                    downstream_urn = fgl.downstreams[0]
                    logger.info(f"  📊 FGL {i + 1}: {upstream_urn} -> {downstream_urn}")

        return self

    def add_term_mutation(
        self, dataset: str, field: str, term: str
    ) -> "PropagationScenarioBuilder":
        """Add a glossary term mutation for live testing.

        Args:
            dataset: Registered dataset name (must be previously registered with register_dataset)
            field: Field name within the dataset
            term: Glossary term URN to add
        """
        if dataset not in self.datasets:
            raise ValueError(
                f"Dataset '{dataset}' not found in registered datasets. Available datasets: {list(self.datasets.keys())}"
            )
        dataset_urn = self.datasets[dataset].urn

        mutation = MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=EditableSchemaMetadataClass(
                editableSchemaFieldInfo=[
                    EditableSchemaFieldInfoClass(
                        fieldPath=field,
                        glossaryTerms=GlossaryTermsClass(
                            auditStamp=AuditStampClass(
                                time=int(
                                    datetime.datetime.now(
                                        datetime.timezone.utc
                                    ).timestamp()
                                    * 1000
                                ),
                                actor="urn:li:corpuser:test_user",
                            ),
                            terms=[
                                GlossaryTermAssociationClass(
                                    urn=term,
                                    actor="urn:li:corpuser:test_user",
                                )
                            ],
                        ),
                    ),
                ],
                created=AuditStampClass(
                    time=int(
                        datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000
                    ),
                    actor="urn:li:corpuser:test_user",
                ),
                lastModified=AuditStampClass(
                    time=int(
                        datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000
                    ),
                    actor="urn:li:corpuser:test_user",
                ),
            ),
        )
        self.mutations.append(mutation)
        return self

    def skip_bootstrap(self, skip: bool = True) -> "PropagationScenarioBuilder":
        """Configure whether to skip bootstrap phase."""
        self.run_bootstrap = not skip
        return self

    def enable_debug_mcps(self, enabled: bool = True) -> "PropagationScenarioBuilder":
        """Enable debug printing of all MCPs during test execution."""
        self.debug_mcps = enabled
        return self

    def enable_verbose_mode(self, enabled: bool = True) -> "PropagationScenarioBuilder":
        """Enable verbose explanations during test execution."""
        self.verbose_mode = enabled
        return self

    def enable_entity_cleanup(
        self, enabled: bool = True
    ) -> "PropagationScenarioBuilder":
        """Enable automatic cleanup of test entities (enabled by default, can be disabled for debugging)."""
        self.cleanup_entities = enabled
        return self

    def build(self) -> PropagationTestScenario:
        """Build the complete test scenario."""
        # Generate MCPs for all datasets
        base_mcps: List[MetadataChangeProposalWrapper] = []

        # First add structured property templates
        base_mcps.extend(self.structured_property_templates)
        for dataset in self.datasets.values():
            # First, emit a status aspect for every dataset (must be first)
            status_mcp = MetadataChangeProposalWrapper(
                entityUrn=dataset.urn,
                aspect=StatusClass(removed=False),
            )
            base_mcps.append(status_mcp)

            # Then emit all other aspects for the dataset
            base_mcps.extend(dataset.generate_mcp())

            # Add sibling relationship MCP if defined
            if dataset.sibling_info:
                sibling_urns, is_primary = dataset.sibling_info
                sibling_mcp = MetadataChangeProposalWrapper(
                    entityUrn=dataset.urn,
                    aspect=SiblingsClass(
                        siblings=sibling_urns,
                        primary=is_primary,
                    ),
                )
                base_mcps.append(sibling_mcp)

            # Add logical parent relationship MCP if defined
            if dataset.logical_parent_urn:
                timestamp = int(
                    datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000
                )
                logical_parent_mcp = MetadataChangeProposalWrapper(
                    entityUrn=dataset.urn,
                    aspect=LogicalParentClass(
                        parent=EdgeClass(
                            destinationUrn=dataset.logical_parent_urn,
                            created=AuditStampClass(
                                time=timestamp,
                                actor="urn:li:corpuser:test_user",
                            ),
                            lastModified=AuditStampClass(
                                time=timestamp,
                                actor="urn:li:corpuser:test_user",
                            ),
                        )
                    ),
                )
                base_mcps.append(logical_parent_mcp)

            # Add field-level logical parent relationships if defined
            if dataset.field_logical_parents:
                from datahub.emitter.mce_builder import make_schema_field_urn

                timestamp = int(
                    datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000
                )

                for (
                    field_name,
                    parent_field_urn,
                ) in dataset.field_logical_parents.items():
                    # Create field URN for the child field
                    child_field_urn = make_schema_field_urn(dataset.urn, field_name)

                    # Create LogicalParentClass aspect for the field
                    field_logical_parent_mcp = MetadataChangeProposalWrapper(
                        entityUrn=child_field_urn,
                        aspect=LogicalParentClass(
                            parent=EdgeClass(
                                sourceUrn=child_field_urn,
                                destinationUrn=parent_field_urn,
                                created=AuditStampClass(
                                    time=timestamp,
                                    actor="urn:li:corpuser:test_user",
                                ),
                                lastModified=AuditStampClass(
                                    time=timestamp,
                                    actor="urn:li:corpuser:test_user",
                                ),
                            )
                        ),
                    )
                    base_mcps.append(field_logical_parent_mcp)

        # Structured property value assignments are now handled during dataset materialization
        # via dataset.generate_mcp() - no separate handling needed here

        # Add lineage relationships - combine multiple upstream lineages to the same downstream
        # Group lineage relationships by downstream dataset
        lineage_by_downstream: Dict[
            str, tuple[Any, List[Any]]
        ] = {}  # downstream_name -> (downstream_dataset, lineage_aspects)

        for _upstream, downstream, lineage_aspect in self.lineage_relationships:
            downstream_dataset = self.datasets[downstream]

            if downstream not in lineage_by_downstream:
                lineage_by_downstream[downstream] = (downstream_dataset, [])
            lineage_by_downstream[downstream][1].append(lineage_aspect)

        # Create merged lineage MCPs
        from datahub.metadata.schema_classes import UpstreamLineageClass

        for _downstream_name, (
            downstream_dataset,
            lineage_aspects,
        ) in lineage_by_downstream.items():
            if len(lineage_aspects) == 1:
                # Single lineage - use as-is
                base_mcps.append(
                    MetadataChangeProposalWrapper(
                        entityUrn=downstream_dataset.urn, aspect=lineage_aspects[0]
                    )
                )
            else:
                # Multiple lineages - merge them
                all_upstreams = []
                all_fine_grained_lineages = []

                for lineage_aspect in lineage_aspects:
                    all_upstreams.extend(lineage_aspect.upstreams)
                    if lineage_aspect.fineGrainedLineages:
                        all_fine_grained_lineages.extend(
                            lineage_aspect.fineGrainedLineages
                        )

                merged_lineage = UpstreamLineageClass(
                    upstreams=all_upstreams,
                    fineGrainedLineages=all_fine_grained_lineages
                    if all_fine_grained_lineages
                    else None,
                )

                base_mcps.append(
                    MetadataChangeProposalWrapper(
                        entityUrn=downstream_dataset.urn, aspect=merged_lineage
                    )
                )

        return PropagationTestScenario(
            base_graph=base_mcps,
            base_expectations=self.base_expectations,
            pre_bootstrap_mutations=[],  # Empty by default, can be added via scenario methods
            mutations=self.mutations,
            post_mutation_expectations=self.post_mutation_expectations,
            run_bootstrap=self.run_bootstrap,
            debug_mcps=self.debug_mcps,
            verbose_mode=self.verbose_mode,
            cleanup_entities=self.cleanup_entities,
        )
