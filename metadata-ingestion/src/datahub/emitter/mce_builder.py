"""Convenience functions for creating MCEs"""

import time
from typing import List, Optional, Type, TypeVar

from datahub.metadata.schema_classes import (
    AuditStampClass,
    DatasetLineageTypeClass,
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    UpstreamClass,
    UpstreamLineageClass,
)

DEFAULT_ENV = "PROD"
DEFAULT_FLOW_CLUSTER = "prod"

T = TypeVar("T")


def get_sys_time() -> int:
    return int(time.time() * 1000)


def make_dataset_urn(platform: str, name: str, env: str = DEFAULT_ENV) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},{env})"


def make_user_urn(username: str) -> str:
    return f"urn:li:corpuser:{username}"


def make_tag_urn(tag: str) -> str:
    return f"urn:li:tag:{tag}"


def make_data_flow_urn(
    orchestrator: str, flow_id: str, cluster: str = DEFAULT_FLOW_CLUSTER
) -> str:
    return f"urn:li:dataFlow:({orchestrator},{flow_id},{cluster})"


def make_data_job_urn_with_flow(flow_urn: str, job_id: str) -> str:
    return f"urn:li:dataJob:({flow_urn},{job_id})"


def make_data_job_urn(
    orchestrator: str, flow_id: str, job_id: str, cluster: str = DEFAULT_FLOW_CLUSTER
) -> str:
    return make_data_job_urn_with_flow(
        make_data_flow_urn(orchestrator, flow_id, cluster), job_id
    )


def make_lineage_mce(
    upstream_urns: List[str],
    downstream_urn: str,
    actor: str = make_user_urn("datahub"),
    lineage_type: str = DatasetLineageTypeClass.TRANSFORMED,
) -> MetadataChangeEventClass:
    sys_time = get_sys_time()

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


def get_aspect_if_available(
    mce: MetadataChangeEventClass, type: Type[T]
) -> Optional[T]:
    all_aspects = mce.proposedSnapshot.aspects
    aspects: List[T] = [aspect for aspect in all_aspects if isinstance(aspect, type)]

    if len(aspects) > 1:
        raise ValueError(f"MCE contains multiple aspects of type {type}: {aspects}")
    if aspects:
        return aspects[0]
    return None


def get_or_add_aspect(mce: MetadataChangeEventClass, default: T) -> T:
    existing = get_aspect_if_available(mce, type(default))
    if existing is not None:
        return existing
    mce.proposedSnapshot.aspects.append(default)
    return default
