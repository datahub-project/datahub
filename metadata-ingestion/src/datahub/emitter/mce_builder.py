"""Convenience functions for creating MCEs"""

import time
from typing import List, Optional, Type, TypeVar

from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    UpstreamClass,
    UpstreamLineageClass,
)

DEFAULT_ENV = "PROD"
DEFAULT_FLOW_CLUSTER = "prod"
UNKNOWN_USER = "urn:li:corpuser:unknown"

T = TypeVar("T")


def get_sys_time() -> int:
    # TODO deprecate this
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


def make_ml_primary_key_urn(feature_table_name: str, primary_key_name: str) -> str:

    return f"urn:li:mlPrimaryKey:({feature_table_name},{primary_key_name})"


def make_ml_feature_urn(
    feature_table_name: str,
    feature_name: str,
) -> str:

    return f"urn:li:mlFeature:({feature_table_name},{feature_name})"


def make_ml_feature_table_urn(platform: str, feature_table_name: str) -> str:

    return (
        f"urn:li:mlFeatureTable:(urn:li:dataPlatform:{platform},{feature_table_name})"
    )


def make_ml_model_urn(platform: str, model_name: str, env: str) -> str:

    return f"urn:li:mlModel:(urn:li:dataPlatform:{platform},{model_name},{env})"


def make_lineage_mce(
    upstream_urns: List[str],
    downstream_urn: str,
    lineage_type: str = DatasetLineageTypeClass.TRANSFORMED,
) -> MetadataChangeEventClass:
    mce = MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
            urn=downstream_urn,
            aspects=[
                UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
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
