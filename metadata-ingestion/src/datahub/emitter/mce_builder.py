"""Convenience functions for creating MCEs"""

from typing import List, Union
import time
from datahub.metadata import (
    MetadataChangeEventClass,
    DatasetSnapshotClass,
    AuditStampClass,
    UpstreamLineageClass,
    UpstreamClass,
    DatasetLineageTypeClass,
)


def make_dataset_urn(platform: str, name: str, env: str = "PROD"):
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},{env})"


def make_user_urn(username: str):
    return f"urn:li:corpuser:{username}"


def make_lineage_mce(
    upstream_urns: Union[str, List[str]],
    downstream_urns: Union[str, List[str]],
    actor: str = make_user_urn("datahub"),
    lineage_type: str = DatasetLineageTypeClass.TRANSFORMED,
):
    sys_time = int(time.time() * 1000)
    if not isinstance(upstream_urns, list):
        upstream_urns = [upstream_urns]
    if not isinstance(downstream_urns, list):
        downstream_urns = [downstream_urns]

    mces = [
        MetadataChangeEventClass(
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
        for downstream_urn in downstream_urns
    ]
    return mces
