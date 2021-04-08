"""Convenience functions for creating MCEs"""

import time
from typing import List, Union

from datahub.metadata import (
    AuditStampClass,
    DatasetLineageTypeClass,
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    UpstreamClass,
    UpstreamLineageClass,
)

DEFAULT_ENV = "PROD"
DEFAULT_FLOW_CLUSTER = "prod"


def make_dataset_urn(platform: str, name: str, env: str = DEFAULT_ENV):
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},{env})"


def make_user_urn(username: str):
    return f"urn:li:corpuser:{username}"


def make_data_flow_urn(
    orchestrator: str, flow_id: str, cluster: str = DEFAULT_FLOW_CLUSTER
):
    return f"urn:li:dataFlow:({orchestrator},{flow_id},{cluster})"


def make_data_job_urn_with_flow(flow_urn: str, job_id: str):
    return f"urn:li:dataJob:({flow_urn},{job_id})"


def make_data_job_urn(
    orchestrator: str, flow_id: str, job_id: str, cluster: str = DEFAULT_FLOW_CLUSTER
):
    return make_data_job_urn_with_flow(
        make_data_flow_urn(orchestrator, flow_id, cluster), job_id
    )


def make_lineage_mce(
    upstream_urns: Union[str, List[str]],
    downstream_urn: str,
    actor: str = make_user_urn("datahub"),
    lineage_type: str = DatasetLineageTypeClass.TRANSFORMED,
) -> MetadataChangeEventClass:
    sys_time = int(time.time() * 1000)
    if not isinstance(upstream_urns, list):
        upstream_urns = [upstream_urns]

    mce = MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
            urn=downstream_urn,
            aspects=[
                UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
                            auditStamp=AuditStampClass(
                                time=sys_time,
                                actor=actor,
                            ),
                            dataset=upstream_urn,
                            type=lineage_type,
                        )
                        for upstream_urn in upstream_urns
                    ]
                )
            ],
        )
    )
    return mce
