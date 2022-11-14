import json
import logging
from typing import Dict, Iterable, List, Set, Union

import pandas as pd

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import config_class, platform_name
from datahub.ingestion.api.workunit import MetadataWorkUnit, UsageStatsWorkUnit
from datahub.ingestion.source.ib.ib_common import IBRedashSource, IBRedashSourceConfig
from datahub.ingestion.source.ib.utils.dataset_utils import (
    DatasetUtils,
    IBGenericPathElements,
)
from datahub.ingestion.source.state.stateful_ingestion_base import JobId
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
)
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)

logger = logging.getLogger(__name__)


class IBLineagesSourceConfig(IBRedashSourceConfig):
    pass


@platform_name("IBLineages")
@config_class(IBLineagesSourceConfig)
class IBLineagesSource(IBRedashSource):
    def __init__(self, config: IBLineagesSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)

    def fetch_workunits(self) -> Iterable[Union[MetadataWorkUnit, UsageStatsWorkUnit]]:
        lineages_grouped = pd.read_json(
            json.dumps(self.query_get(self.config.query_id))
        ).groupby(
            [
                "dstType",
                "dstLocationCode",
                "dstParent1",
                "dstParent2",
                "dstParent3",
                "dstObjectName",
            ],
            dropna=False,
        )

        for key_tuple, lineages in lineages_grouped:
            row = lineages.iloc[0]
            dst_platform = row.dstType.lower()
            dst_dataset_path = DatasetUtils.map_path(
                dst_platform,
                None,
                IBGenericPathElements(
                    location_code=row.dstLocationCode,
                    parent1=row.dstParent1,
                    parent2=row.dstParent2,
                    parent3=row.dstParent3,
                    object_name=row.dstObjectName,
                ),
            )
            dst_dataset_urn = DatasetUtils.build_dataset_urn(
                dst_platform, *dst_dataset_path
            )

            upstream_tables_urns: Set[str] = set()
            finegrained_lineages_container = FineGrainedLineagesContainer()
            # TODO for to map()/transform()
            for index, row in lineages.iterrows():
                src_platform = row.srcType.lower()
                src_dataset_path = DatasetUtils.map_path(
                    src_platform,
                    None,
                    IBGenericPathElements(
                        location_code=row.srcLocationCode,
                        parent1=row.srcParent1,
                        parent2=row.srcParent2,
                        parent3=row.srcParent3,
                        object_name=row.srcObjectName,
                    ),
                )
                src_dataset_urn = DatasetUtils.build_dataset_urn(
                    src_platform, *src_dataset_path
                )
                upstream_tables_urns.add(src_dataset_urn)

                # TODO unify schema field urn generation between ingesters
                if pd.notna(row.srcFieldName) and pd.notna(row.dstFieldName):
                    finegrained_lineages_container.add_column(
                        dst_column_urn=builder.make_schema_field_urn(
                            dst_dataset_urn, row.dstFieldName
                        ),
                        src_column_urn=builder.make_schema_field_urn(
                            src_dataset_urn, row.srcFieldName
                        ),
                    )

            upstream_tables: List[UpstreamClass] = []
            for urn in upstream_tables_urns:
                upstream_tables.append(
                    UpstreamClass(
                        dataset=urn,
                        type=DatasetLineageTypeClass.TRANSFORMED,
                    )
                )

            finegrained_lineages: List[FineGrainedLineage] = []
            for (
                dst_column_urn,
                src_column_urns,
            ) in finegrained_lineages_container.get_columns().items():
                finegrained_lineages.append(
                    FineGrainedLineage(
                        upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                        upstreams=list(src_column_urns),
                        downstreamType=FineGrainedLineageDownstreamType.FIELD,
                        downstreams=[dst_column_urn],
                    )
                )

            yield MetadataWorkUnit(
                id=f"{dst_dataset_urn}-lineage",
                mcp=MetadataChangeProposalWrapper(
                    entityType="dataset",
                    changeType=ChangeTypeClass.UPSERT,
                    entityUrn=dst_dataset_urn,
                    aspectName="upstreamLineage",
                    aspect=UpstreamLineageClass(
                        upstreams=upstream_tables,
                        fineGrainedLineages=finegrained_lineages,
                    ),
                ),
            )

    def get_default_ingestion_job_id_prefix(self) -> JobId:
        return JobId("ingest_lineages_from_redash_source_")


class FineGrainedLineagesContainer:
    _column_urns: Dict[str, Set[str]]

    def __init__(self):
        self._column_urns = dict()

    def add_column(self, dst_column_urn: str, src_column_urn: str):
        src_columns = self._column_urns.get(dst_column_urn, set())
        src_columns.add(src_column_urn)
        self._column_urns[dst_column_urn] = src_columns

    def get_columns(self) -> Dict[str, Set[str]]:
        return self._column_urns
