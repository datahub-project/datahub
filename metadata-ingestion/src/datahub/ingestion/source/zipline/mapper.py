import logging
from typing import Dict, Iterable, List, Optional

from datahub.emitter.mce_builder import (
    make_ml_feature_table_urn,
    make_ml_feature_urn,
    make_ml_primary_key_urn,
    make_tag_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import MLAssetSubTypes
from datahub.ingestion.source.zipline.config import ZiplineConfig
from datahub.ingestion.source.zipline.constants import (
    CUSTOM_JSON_GROUPBY_TAGS_KEY,
    PLATFORM_NAME,
    operation_to_feature_data_type,
)
from datahub.ingestion.source.zipline.lineage import SourceResolver
from datahub.ingestion.source.zipline.models import GroupBy, MetaData
from datahub.ingestion.source.zipline.report import ZiplineSourceReport
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    GlobalTagsClass,
    MLFeatureDataTypeClass as DataType,
    MLFeaturePropertiesClass,
    MLFeatureTablePropertiesClass,
    MLPrimaryKeyPropertiesClass,
    OwnerClass,
    OwnershipClass,
    StatusClass,
    SubTypesClass,
    TagAssociationClass,
)

logger = logging.getLogger(__name__)


class ZiplineMapper:
    """Maps compiled Chronon GroupBys to DataHub ML feature entities."""

    def __init__(
        self,
        config: ZiplineConfig,
        report: ZiplineSourceReport,
        source_resolver: SourceResolver,
    ) -> None:
        self.config = config
        self.report = report
        self.source_resolver = source_resolver

    def map_group_by(self, group_by: GroupBy) -> Iterable[MetadataWorkUnit]:
        table_name = group_by.meta_data.name
        if table_name is None:
            # extra="ignore" tolerates schema drift, so a renamed/absent name key
            # surfaces here as None. Without a name there is no feature-table URN,
            # so the whole GroupBy (and its features) would vanish silently.
            self.report.warning(
                title="GroupBy missing name",
                message="Skipped a compiled GroupBy with no metaData.name — cannot form a feature-table URN",
                context=group_by.source_file,
            )
            return

        if (
            self.config.enable_tag_extraction
            and group_by.meta_data.has_malformed_custom_json()
        ):
            self.report.warning(
                title="Unparseable customJson",
                message="Could not decode MetaData.customJson; tags for this feature table were dropped",
                context=table_name,
            )

        table_urn = make_ml_feature_table_urn(PLATFORM_NAME, table_name)
        source_urns = self._resolve_sources(group_by)
        column_tags = group_by.meta_data.column_tags()

        feature_specs = self._feature_specs(group_by)
        feature_urns: List[str] = []
        for feature_name, data_type in feature_specs.items():
            feature_urn = make_ml_feature_urn(table_name, feature_name)
            feature_urns.append(feature_urn)
            yield from self._emit_feature(
                feature_urn=feature_urn,
                data_type=data_type,
                source_urns=source_urns,
                tags=column_tags.get(feature_name, {}),
            )
            self.report.report_feature_scanned()

        primary_key_urns: List[str] = []
        for key in group_by.key_columns:
            key_urn = make_ml_primary_key_urn(table_name, key)
            primary_key_urns.append(key_urn)
            yield from self._emit_primary_key(
                key_urn=key_urn,
                source_urns=source_urns,
                tags=column_tags.get(key, {}),
            )
            self.report.report_primary_key_scanned()

        yield from self._emit_feature_table(
            table_urn=table_urn,
            meta_data=group_by.meta_data,
            feature_urns=feature_urns,
            primary_key_urns=primary_key_urns,
            tags_key=CUSTOM_JSON_GROUPBY_TAGS_KEY,
        )
        self.report.report_feature_table_scanned()

    def _resolve_sources(self, group_by: GroupBy) -> List[str]:
        urns: List[str] = []
        for source in group_by.sources:
            if source.join_source is not None:
                self.report.join_sources_skipped += 1
                continue
            urns.extend(self.source_resolver.resolve_source_urns(source))
        # De-duplicate while preserving order for stable golden output.
        return list(dict.fromkeys(urns))

    def _feature_specs(self, group_by: GroupBy) -> Dict[str, str]:
        """Feature name -> MLFeatureDataType.

        Derivations rename/compose columns the compiled config never types, so
        they fall back to UNKNOWN; otherwise the type is inferred per operation.
        """
        specs: Dict[str, str] = {}
        if group_by.derivations or not group_by.aggregations:
            for name in group_by.feature_names():
                specs[name] = DataType.UNKNOWN
            return specs

        for aggregation in group_by.aggregations:
            data_type = operation_to_feature_data_type(aggregation.operation or -1)
            for name in aggregation.output_column_names():
                specs[name] = data_type
        return specs

    def _emit_feature(
        self,
        feature_urn: str,
        data_type: str,
        source_urns: List[str],
        tags: Dict[str, str],
    ) -> Iterable[MetadataWorkUnit]:
        yield MetadataChangeProposalWrapper(
            entityUrn=feature_urn,
            aspect=MLFeaturePropertiesClass(
                dataType=data_type,
                sources=source_urns or None,
            ),
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=feature_urn, aspect=StatusClass(removed=False)
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=feature_urn,
            aspect=SubTypesClass(typeNames=[MLAssetSubTypes.FEATURE]),
        ).as_workunit()
        tags_aspect = self._tags_aspect(tags)
        if tags_aspect is not None:
            yield MetadataChangeProposalWrapper(
                entityUrn=feature_urn, aspect=tags_aspect
            ).as_workunit()

    def _emit_primary_key(
        self,
        key_urn: str,
        source_urns: List[str],
        tags: Dict[str, str],
    ) -> Iterable[MetadataWorkUnit]:
        yield MetadataChangeProposalWrapper(
            entityUrn=key_urn,
            aspect=MLPrimaryKeyPropertiesClass(
                # The compiled config does not carry key column types.
                dataType=DataType.UNKNOWN,
                sources=source_urns,
            ),
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=key_urn, aspect=StatusClass(removed=False)
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=key_urn,
            aspect=SubTypesClass(typeNames=[MLAssetSubTypes.PRIMARY_KEY]),
        ).as_workunit()
        tags_aspect = self._tags_aspect(tags)
        if tags_aspect is not None:
            yield MetadataChangeProposalWrapper(
                entityUrn=key_urn, aspect=tags_aspect
            ).as_workunit()

    def _emit_feature_table(
        self,
        table_urn: str,
        meta_data: MetaData,
        feature_urns: List[str],
        primary_key_urns: List[str],
        tags_key: str,
    ) -> Iterable[MetadataWorkUnit]:
        yield MetadataChangeProposalWrapper(
            entityUrn=table_urn,
            aspect=MLFeatureTablePropertiesClass(
                description=meta_data.description,
                mlFeatures=feature_urns,
                mlPrimaryKeys=primary_key_urns,
            ),
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=table_urn, aspect=StatusClass(removed=False)
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=table_urn,
            aspect=SubTypesClass(typeNames=[MLAssetSubTypes.FEATURE_TABLE]),
        ).as_workunit()
        if meta_data.team:
            yield MetadataChangeProposalWrapper(
                entityUrn=table_urn,
                aspect=BrowsePathsClass(paths=[f"/{PLATFORM_NAME}/{meta_data.team}"]),
            ).as_workunit()

        tags_aspect = self._tags_aspect(meta_data.tags(tags_key))
        if tags_aspect is not None:
            yield MetadataChangeProposalWrapper(
                entityUrn=table_urn, aspect=tags_aspect
            ).as_workunit()

        ownership = self._ownership_aspect(meta_data.team)
        if ownership is not None:
            yield MetadataChangeProposalWrapper(
                entityUrn=table_urn, aspect=ownership
            ).as_workunit()

    def _tags_aspect(self, tags: Dict[str, str]) -> Optional[GlobalTagsClass]:
        if not self.config.enable_tag_extraction or not tags:
            return None
        associations = [
            TagAssociationClass(tag=make_tag_urn(f"{key}:{value}" if value else key))
            for key, value in sorted(tags.items())
        ]
        return GlobalTagsClass(tags=associations)

    def _ownership_aspect(self, team: Optional[str]) -> Optional[OwnershipClass]:
        if not self.config.enable_owner_extraction or not team:
            return None
        if not self.config.owner_mappings:
            return None
        for mapping in self.config.owner_mappings:
            if mapping.team_name != team:
                continue
            return OwnershipClass(
                owners=[
                    OwnerClass(
                        owner=mapping.datahub_owner_urn,
                        type=mapping.datahub_ownership_type,
                    )
                ]
            )
        return None
