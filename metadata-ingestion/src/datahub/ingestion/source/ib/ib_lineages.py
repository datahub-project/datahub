import json

import pandas as pd
from datahub.ingestion.api.decorators import config_class, platform_name
from datahub.ingestion.api.workunit import UsageStatsWorkUnit
from datahub.metadata.schema_classes import DatasetLineageTypeClass

from datahub.ingestion.source.ib.ib_common import *

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
                "DstType",
                "DstLocationCode",
                "DstDBName",
                "DstSchemaName",
                "DstTableName",
            ],
            dropna=False,
        )

        for key_tuple, lineages in lineages_grouped:
            first = lineages.iloc[0]
            dst_dataset_urn = build_dataset_urn(
                first.DstType,
                first.DstLocationCode,
                first.DstDBName,
                first.DstSchemaName,
                first.DstTableName,
            )

            src_urns = []
            # TODO for to map()/transform()
            for index, row in lineages.iterrows():
                src_urns.append(
                    build_dataset_urn(
                        row.SrcType,
                        row.SrcLocationCode,
                        row.SrcDBName,
                        row.SrcSchemaName,
                        row.SrcTableName,
                    )
                )

            yield MetadataWorkUnit(
                dst_dataset_urn,
                mce=builder.make_lineage_mce(src_urns, dst_dataset_urn, DatasetLineageTypeClass.COPY),
            )

    def get_default_ingestion_job_id(self) -> JobId:
        return JobId("ingest_lineages_from_redash_source")
