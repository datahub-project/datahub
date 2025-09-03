"""Scenario builders for easily creating propagation test scenarios.

These builders use a fluent API to make test creation more readable and maintainable.
"""

import datetime
import logging
from typing import Any, Dict, List, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AuditStampClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
)
from tests.propagation.framework.core.models import PropagationTestScenario
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
        self.field_glossary_terms: Dict[str, str] = {}
        self.field_tags: Dict[str, str] = {}
        self.subtype: Optional[str] = None

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
        self.field_glossary_terms[column_name] = term
        return self

    def with_column_tag(self, column_name: str, tag: str) -> "DatasetBuilder":
        """Set tag for a specific column by name."""
        if column_name not in self.column_names:
            raise ValueError(
                f"Column '{column_name}' not found. Available columns: {self.column_names}"
            )
        self.field_tags[column_name] = tag
        return self

    def with_subtype(self, subtype: str) -> "DatasetBuilder":
        """Set dataset subtype."""
        self.subtype = subtype
        return self

    def build(self):
        """Build the dataset."""
        if not self.column_names:
            raise ValueError(
                "No columns defined. Use with_columns() to set actual column names first."
            )

        num_fields = len(self.column_names)
        field_descriptions_list = []
        field_glossary_terms_list = []
        field_tags_list = []

        for column_name in self.column_names:
            # For descriptions
            if column_name in self.field_descriptions:
                field_descriptions_list.append(self.field_descriptions[column_name])

            # For glossary terms
            if column_name in self.field_glossary_terms:
                field_glossary_terms_list.append(self.field_glossary_terms[column_name])

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

    def add_dataset(self, name: str, platform: str = "snowflake") -> DatasetBuilder:
        """Add a dataset with fluent configuration."""
        return DatasetBuilder(self.graph_builder, platform, name)

    def register_dataset(self, name: str, dataset) -> "PropagationScenarioBuilder":
        """Register a built dataset."""
        self.datasets[name] = dataset
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
        """Add a glossary term mutation for live testing."""
        dataset_urn = self.graph_builder.get_dataset_urn(
            self._extract_platform(dataset), self._extract_name(dataset)
        )

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
        base_mcps = []
        for dataset in self.datasets.values():
            base_mcps.extend(dataset.generate_mcp())

        # Add lineage relationships
        for _upstream, downstream, lineage_aspect in self.lineage_relationships:
            downstream_dataset = self.datasets[downstream]
            base_mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=downstream_dataset.urn, aspect=lineage_aspect
                )
            )

        return PropagationTestScenario(
            base_graph=base_mcps,
            base_expectations=self.base_expectations,
            mutations=self.mutations,
            post_mutation_expectations=self.post_mutation_expectations,
            run_bootstrap=self.run_bootstrap,
            debug_mcps=self.debug_mcps,
            verbose_mode=self.verbose_mode,
            cleanup_entities=self.cleanup_entities,
        )

    def _extract_platform(self, dataset_name: str) -> str:
        """Extract platform from dataset name."""
        if "." in dataset_name:
            return dataset_name.split(".")[0]
        return "snowflake"  # default

    def _extract_name(self, dataset_name: str) -> str:
        """Extract name from dataset name."""
        if "." in dataset_name:
            return dataset_name.split(".", 1)[1]
        return dataset_name
