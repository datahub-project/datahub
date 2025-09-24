"""Graph generation utilities for propagation tests."""

from typing import Dict, List, Optional, Tuple

from pydantic import Field

import datahub.metadata.schema_classes as models
from datahub.api.entities.dataset.dataset import (
    Dataset as BaseDataset,
    SchemaFieldSpecification,
    SchemaSpecification,
)
from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
from datahub.metadata.schema_classes import (
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)


class Dataset(BaseDataset):
    """Extended Dataset class with additional fields for propagation testing."""

    sibling_info: Optional[Tuple[List[str], bool]] = Field(
        default=None,
        description="Tuple of (sibling_urns, is_primary) for sibling relationships",
    )

    logical_parent_urn: Optional[str] = Field(
        default=None, description="URN of the logical parent dataset"
    )

    field_logical_parents: Dict[str, str] = Field(
        default_factory=dict,
        description="Dictionary mapping field names to their logical parent field URNs",
    )


class LineageGraphBuilder:
    """Utility for building standardized lineage graphs."""

    def __init__(self, prefix: str = "test"):
        self.prefix = prefix

    def create_dataset(
        self,
        platform: str,
        name: str,
        description: str,
        num_fields: int = 5,
        field_names: Optional[List[str]] = None,
        field_descriptions: Optional[List[str]] = None,
        field_glossary_terms: Optional[List[str]] = None,
        field_global_tags: Optional[List[str]] = None,
        subtype: Optional[str] = None,
    ) -> Dataset:
        """Create a dataset with configurable fields."""
        dataset_urn = make_dataset_urn(platform, f"{self.prefix}.{name}")

        # Use the provided field names
        if not field_names:
            raise ValueError(
                "field_names must be provided - indexed field creation is no longer supported"
            )
        actual_field_names = field_names

        fields = []
        for i, field_name in enumerate(actual_field_names):
            field_spec = SchemaFieldSpecification(
                id=field_name,
                type="string",
                description=field_descriptions[i]
                if field_descriptions and i < len(field_descriptions)
                else None,
                urn=make_schema_field_urn(dataset_urn, field_name),
                glossaryTerms=[field_glossary_terms[i]]
                if field_glossary_terms
                and i < len(field_glossary_terms)
                and field_glossary_terms[i] is not None
                else None,
                globalTags=[field_global_tags[i]]
                if field_global_tags
                and i < len(field_global_tags)
                and field_global_tags[i] is not None
                else None,
            )
            fields.append(field_spec)

        return Dataset(
            id=name,
            urn=dataset_urn,
            platform=platform,
            name=name,
            description=description,
            subtype=subtype,
            subtypes=None,
            schema=SchemaSpecification(
                file=None,
                fields=fields,
            ),
            downstreams=None,
            properties=None,
        )

    def create_lineage_aspect(
        self,
        upstream_dataset_urn: str,
        lineage_type: str = "TRANSFORMED",
        field_mappings: Optional[List[tuple[List[str], List[str]]]] = None,
    ) -> UpstreamLineageClass:
        """Create lineage aspect with field mappings."""
        fine_grained_lineages = []

        if field_mappings:
            for upstream_fields, downstream_fields in field_mappings:
                fine_grained_lineages.append(
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
                        upstreams=upstream_fields,
                        downstreams=downstream_fields,
                    )
                )

        return UpstreamLineageClass(
            upstreams=[
                UpstreamClass(
                    dataset=upstream_dataset_urn,
                    type=lineage_type,
                )
            ],
            fineGrainedLineages=fine_grained_lineages,
        )

    def create_sibling_relationship(
        self, sibling_urns: List[str], primary: bool = True
    ) -> models.SiblingsClass:
        """Create sibling relationship aspect."""
        return models.SiblingsClass(
            siblings=sibling_urns,
            primary=primary,
        )

    def get_dataset_urn(self, platform: str, name: str) -> str:
        """Get dataset URN."""
        return make_dataset_urn(platform, f"{self.prefix}.{name}")

    def get_field_urn(self, platform: str, dataset_name: str, field_name: str) -> str:
        """Get field URN."""
        dataset_urn = self.get_dataset_urn(platform, dataset_name)
        return make_schema_field_urn(dataset_urn, field_name)

    def get_field_urns_for_dataset(
        self, platform: str, dataset_name: str, field_names: List[str]
    ) -> List[str]:
        """Get all field URNs for a dataset using actual field names."""
        return [
            self.get_field_urn(platform, dataset_name, field_name)
            for field_name in field_names
        ]
