from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional

import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.urns import DatasetUrn, QueryUrn
from datahub.sdk._shared import DatasetUrnOrStr
from datahub.sdk._utils import DEFAULT_ACTOR_URN
from datahub.sdk.dataset import ColumnLineageMapping, parse_cll_mapping
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.sql_parsing.fingerprint_utils import generate_hash
from datahub.sql_parsing.sql_parsing_aggregator import make_query_subjects
from datahub.utilities.ordered_set import OrderedSet

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient

logger = logging.getLogger(__name__)

_empty_audit_stamp = models.AuditStampClass(
    time=0,
    actor=DEFAULT_ACTOR_URN,
)


class LineageClient:
    def __init__(self, client: DataHubClient):
        self._client = client

    def add_dataset_transform_lineage(
        self,
        *,
        upstream: DatasetUrnOrStr,
        downstream: DatasetUrnOrStr,
        column_lineage: Optional[ColumnLineageMapping] = None,
        query_text: Optional[str] = None,
    ) -> None:
        upstream = DatasetUrn.from_string(upstream)
        downstream = DatasetUrn.from_string(downstream)

        cll = None
        if column_lineage is not None:
            cll = parse_cll_mapping(
                upstream=upstream,
                downstream=downstream,
                cll_mapping=column_lineage,
            )

        fields_involved = OrderedSet([str(upstream), str(downstream)])
        if cll is not None:
            for c in cll:
                for field in c.upstreams or []:
                    fields_involved.add(field)
                for field in c.downstreams or []:
                    fields_involved.add(field)

        query_urn = None
        query_entity = None
        if query_text:
            # Eventually we might want to use our regex-based fingerprinting instead.
            fingerprint = generate_hash(query_text)
            query_urn = QueryUrn(fingerprint).urn()

            query_entity = MetadataChangeProposalWrapper.construct_many(
                query_urn,
                aspects=[
                    models.QueryPropertiesClass(
                        statement=models.QueryStatementClass(
                            value=query_text,
                            language=models.QueryLanguageClass.SQL,
                        ),
                        source=models.QuerySourceClass.SYSTEM,
                        created=_empty_audit_stamp,
                        lastModified=_empty_audit_stamp,
                    ),
                    make_query_subjects(list(fields_involved)),
                ],
            )

            for c in cll or []:
                c.query = query_urn

        updater = DatasetPatchBuilder(str(downstream))
        updater.add_upstream_lineage(
            models.UpstreamClass(
                dataset=str(upstream),
                type=models.DatasetLineageTypeClass.TRANSFORMED,
                query=query_urn,
            )
        )
        for cl in cll or []:
            updater.add_fine_grained_upstream_lineage(cl)

        # Will throw if the dataset does not exist.
        self._client.entities.update(updater)
        if query_entity:
            self._client._graph.emit_mcps(query_entity)
