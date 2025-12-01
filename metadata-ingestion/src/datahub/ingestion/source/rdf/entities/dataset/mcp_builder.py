"""
Dataset MCP Builder

Creates DataHub MCPs (Metadata Change Proposals) for datasets.
"""

import logging
from typing import Any, Dict, List

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.rdf.entities.base import EntityMCPBuilder
from datahub.ingestion.source.rdf.entities.dataset.ast import DataHubDataset
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DatasetPropertiesClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    SchemalessClass,
    SchemaMetadataClass,
)

logger = logging.getLogger(__name__)


class DatasetMCPBuilder(EntityMCPBuilder[DataHubDataset]):
    """
    Creates MCPs for datasets.

    Creates:
    - DatasetProperties MCP for basic metadata
    - SchemaMetadata MCP for schema fields
    - GlossaryTerms MCP for field-to-term associations
    """

    @property
    def entity_type(self) -> str:
        return "dataset"

    def build_mcps(
        self, dataset: DataHubDataset, context: Dict[str, Any] = None
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Build MCPs for a single dataset.

        Args:
            dataset: The DataHub dataset
            context: Optional context
        """
        mcps = []

        try:
            # Dataset properties MCP
            properties_mcp = self._create_properties_mcp(dataset)
            mcps.append(properties_mcp)

            # Schema metadata MCP if schema fields exist
            if dataset.schema_fields:
                schema_mcp = self._create_schema_mcp(dataset)
                if schema_mcp:
                    mcps.append(schema_mcp)

                # Field-to-glossary-term MCPs
                field_mcps = self._create_field_glossary_mcps(dataset)
                mcps.extend(field_mcps)

        except Exception as e:
            logger.error(f"Failed to create MCPs for dataset {dataset.name}: {e}")

        return mcps

    def build_all_mcps(
        self, datasets: List[DataHubDataset], context: Dict[str, Any] = None
    ) -> List[MetadataChangeProposalWrapper]:
        """Build MCPs for all datasets."""
        mcps = []

        for dataset in datasets:
            dataset_mcps = self.build_mcps(dataset, context)
            mcps.extend(dataset_mcps)

        logger.info(f"Built {len(mcps)} MCPs for {len(datasets)} datasets")
        return mcps

    def _create_properties_mcp(
        self, dataset: DataHubDataset
    ) -> MetadataChangeProposalWrapper:
        """Create DatasetProperties MCP."""
        properties_aspect = DatasetPropertiesClass(
            name=dataset.name,
            description=dataset.description or f"Dataset: {dataset.name}",
            customProperties=dataset.custom_properties or {},
        )

        return MetadataChangeProposalWrapper(
            entityUrn=str(dataset.urn), aspect=properties_aspect
        )

    def _create_schema_mcp(
        self, dataset: DataHubDataset
    ) -> MetadataChangeProposalWrapper:
        """Create SchemaMetadata MCP.

        Platform is embedded in the dataset URN at this stage (DataHub AST).
        Extract it from the URN - no need to check dataset.platform.
        """
        dataset_urn_str = str(dataset.urn)

        # Extract platform from dataset URN: urn:li:dataset:(urn:li:dataPlatform:postgres,name,env)
        # Platform is always the first part inside the parentheses
        if "," not in dataset_urn_str or "(" not in dataset_urn_str:
            raise ValueError(
                f"Invalid dataset URN format: {dataset_urn_str}. "
                f"Expected format: urn:li:dataset:(urn:li:dataPlatform:platform,path,env). "
                f"This should have been set during RDF to DataHub AST conversion."
            )

        # Extract platform URN from dataset URN
        platform_part = dataset_urn_str.split("(")[1].split(",")[0]
        platform_urn = platform_part

        schema_metadata = SchemaMetadataClass(
            schemaName=dataset.name.replace(" ", "_"),
            platform=platform_urn,
            version=0,
            hash="",
            platformSchema=SchemalessClass(),
            fields=dataset.schema_fields,
        )

        return MetadataChangeProposalWrapper(
            entityUrn=str(dataset.urn), aspect=schema_metadata
        )

    def _create_field_glossary_mcps(
        self, dataset: DataHubDataset
    ) -> List[MetadataChangeProposalWrapper]:
        """Create MCPs for field-to-glossary-term associations."""
        mcps = []

        if not dataset.field_glossary_relationships:
            return mcps

        import time

        audit_stamp = AuditStampClass(
            time=int(time.time() * 1000), actor="urn:li:corpuser:datahub"
        )

        for field_name, term_urns in dataset.field_glossary_relationships.items():
            if not term_urns:
                continue

            # Create field URN
            field_urn = make_schema_field_urn(str(dataset.urn), field_name)

            # Create glossary term associations
            associations = [
                GlossaryTermAssociationClass(urn=term_urn) for term_urn in term_urns
            ]

            glossary_terms = GlossaryTermsClass(
                terms=associations, auditStamp=audit_stamp
            )

            mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=field_urn, aspect=glossary_terms
                )
            )

        return mcps

    @staticmethod
    def create_dataset_domain_association_mcp(
        dataset_urn: str, domain_urn: str
    ) -> MetadataChangeProposalWrapper:
        """Create MCP to associate a dataset with a domain."""
        from datahub.metadata.schema_classes import DomainsClass

        domains_aspect = DomainsClass(domains=[domain_urn])

        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=domains_aspect,
        )

    def build_post_processing_mcps(
        self, datahub_graph: Any, context: Dict[str, Any] = None
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Build MCPs for dataset-domain associations.

        This handles the cross-entity dependency where datasets need to be
        associated with domains after both have been created.

        Args:
            datahub_graph: The complete DataHubGraph AST
            context: Optional context

        Returns:
            List of MCPs for dataset-domain associations
        """
        mcps = []

        # Build a map of datasets to their domains
        dataset_to_domain_map = {}
        for domain in datahub_graph.domains:
            for dataset in domain.datasets:
                dataset_urn_str = str(dataset.urn) if dataset.urn else None
                domain_urn_str = str(domain.urn) if domain.urn else None
                if dataset_urn_str and domain_urn_str:
                    dataset_to_domain_map[dataset_urn_str] = domain_urn_str

        # Add domain association MCPs for datasets that belong to domains
        for dataset_urn_str, domain_urn_str in dataset_to_domain_map.items():
            try:
                domain_mcp = self.create_dataset_domain_association_mcp(
                    dataset_urn_str, domain_urn_str
                )
                mcps.append(domain_mcp)
                logger.debug(
                    f"Assigned dataset {dataset_urn_str} to domain {domain_urn_str}"
                )
            except Exception as e:
                logger.warning(
                    f"Failed to create domain association MCP for dataset {dataset_urn_str}: {e}"
                )

        logger.debug(f"Created {len(mcps)} dataset-domain association MCPs")
        return mcps
