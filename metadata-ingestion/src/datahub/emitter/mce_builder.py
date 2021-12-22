"""Convenience functions for creating MCEs"""
import logging
import re
import time
from typing import Any, List, Optional, Type, TypeVar, cast, get_type_hints

import typing_inspect
from avrogen.dict_wrapper import DictWrapper

from datahub.metadata.schema_classes import (
    DatasetKeyClass,
    DatasetLineageTypeClass,
    DatasetSnapshotClass,
    GlobalTagsClass,
    MetadataChangeEventClass,
    OwnershipTypeClass,
    TagAssociationClass,
    UpstreamClass,
    UpstreamLineageClass,
)

DEFAULT_ENV = "PROD"
DEFAULT_FLOW_CLUSTER = "prod"
UNKNOWN_USER = "urn:li:corpuser:unknown"


logger = logging.getLogger(__name__)


def get_sys_time() -> int:
    # TODO deprecate this
    return int(time.time() * 1000)


def make_data_platform_urn(platform: str) -> str:
    if platform.startswith("urn:li:dataPlatform:"):
        return platform
    return f"urn:li:dataPlatform:{platform}"


def make_dataset_urn(platform: str, name: str, env: str = DEFAULT_ENV) -> str:
    return f"urn:li:dataset:({make_data_platform_urn(platform)},{name},{env})"


def dataset_urn_to_key(dataset_urn: str) -> Optional[DatasetKeyClass]:
    pattern = r"urn:li:dataset:\(urn:li:dataPlatform:(.*),(.*),(.*)\)"
    results = re.search(pattern, dataset_urn)
    if results is not None:
        return DatasetKeyClass(
            platform=results.group(1), name=results.group(2), origin=results.group(3)
        )
    return None


def make_user_urn(username: str) -> str:
    return f"urn:li:corpuser:{username}"


def make_group_urn(groupname: str) -> str:
    return f"urn:li:corpGroup:{groupname}"


def make_tag_urn(tag: str) -> str:
    return f"urn:li:tag:{tag}"


def make_term_urn(term: str) -> str:
    return f"urn:li:glossaryTerm:{term}"


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


def make_dashboard_urn(platform: str, name: str) -> str:
    # FIXME: dashboards don't currently include data platform urn prefixes.
    return f"urn:li:dashboard:({platform},{name})"


def make_chart_urn(platform: str, name: str) -> str:
    # FIXME: charts don't currently include data platform urn prefixes.
    return f"urn:li:chart:({platform},{name})"


def make_ml_primary_key_urn(feature_table_name: str, primary_key_name: str) -> str:

    return f"urn:li:mlPrimaryKey:({feature_table_name},{primary_key_name})"


def make_ml_feature_urn(
    feature_table_name: str,
    feature_name: str,
) -> str:

    return f"urn:li:mlFeature:({feature_table_name},{feature_name})"


def make_ml_feature_table_urn(platform: str, feature_table_name: str) -> str:
    return f"urn:li:mlFeatureTable:({make_data_platform_urn(platform)},{feature_table_name})"


def make_ml_model_urn(platform: str, model_name: str, env: str) -> str:
    return f"urn:li:mlModel:({make_data_platform_urn(platform)},{model_name},{env})"


def make_ml_model_deployment_urn(platform: str, deployment_name: str, env: str) -> str:
    return f"urn:li:mlModelDeployment:({make_data_platform_urn(platform)},{deployment_name},{env})"


def make_ml_model_group_urn(platform: str, group_name: str, env: str) -> str:
    return (
        f"urn:li:mlModelGroup:({make_data_platform_urn(platform)},{group_name},{env})"
    )


def is_valid_ownership_type(ownership_type: Optional[str]) -> bool:
    return ownership_type is not None and ownership_type in [
        OwnershipTypeClass.DEVELOPER,
        OwnershipTypeClass.DATAOWNER,
        OwnershipTypeClass.DELEGATE,
        OwnershipTypeClass.PRODUCER,
        OwnershipTypeClass.CONSUMER,
        OwnershipTypeClass.STAKEHOLDER,
    ]


def validate_ownership_type(ownership_type: Optional[str]) -> str:
    if is_valid_ownership_type(ownership_type):
        return cast(str, ownership_type)
    else:
        raise ValueError(f"Unexpected ownership type: {ownership_type}")


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


# This bound isn't tight, but it's better than nothing.
Aspect = TypeVar("Aspect", bound=DictWrapper)


def can_add_aspect(mce: MetadataChangeEventClass, AspectType: Type[Aspect]) -> bool:
    SnapshotType = type(mce.proposedSnapshot)

    constructor_annotations = get_type_hints(SnapshotType.__init__)
    aspect_list_union = typing_inspect.get_args(constructor_annotations["aspects"])[0]
    if not isinstance(aspect_list_union, tuple):
        supported_aspect_types = typing_inspect.get_args(aspect_list_union)
    else:
        # On Python 3.6, the union type is represented as a tuple, where
        # the first item is typing.Union and the subsequent elements are
        # the types within the union.
        supported_aspect_types = aspect_list_union[1:]

    return issubclass(AspectType, supported_aspect_types)


def get_aspect_if_available(
    mce: MetadataChangeEventClass, AspectType: Type[Aspect]
) -> Optional[Aspect]:
    assert can_add_aspect(mce, AspectType)

    all_aspects = mce.proposedSnapshot.aspects
    aspects: List[Aspect] = [
        aspect for aspect in all_aspects if isinstance(aspect, AspectType)
    ]

    if len(aspects) > 1:
        raise ValueError(
            f"MCE contains multiple aspects of type {AspectType}: {aspects}"
        )
    if aspects:
        return aspects[0]
    return None


def remove_aspect_if_available(
    mce: MetadataChangeEventClass, aspect_type: Type[Aspect]
) -> bool:
    assert can_add_aspect(mce, aspect_type)
    # loose type annotations since we checked before
    aspects: List[Any] = [
        aspect
        for aspect in mce.proposedSnapshot.aspects
        if not isinstance(aspect, aspect_type)
    ]
    removed = len(aspects) != len(mce.proposedSnapshot.aspects)
    mce.proposedSnapshot.aspects = aspects
    return removed


def get_or_add_aspect(mce: MetadataChangeEventClass, default: Aspect) -> Aspect:
    existing = get_aspect_if_available(mce, type(default))
    if existing is not None:
        return existing
    mce.proposedSnapshot.aspects.append(default)  # type: ignore
    return default


def make_global_tag_aspect_with_tag_list(tags: List[str]) -> GlobalTagsClass:
    return GlobalTagsClass(
        tags=[TagAssociationClass(f"urn:li:tag:{tag}") for tag in tags]
    )


def set_aspect(
    mce: MetadataChangeEventClass, aspect: Optional[Aspect], aspect_type: Type[Aspect]
) -> None:
    """Sets the aspect to the provided aspect, overwriting any previous aspect value that might have existed before.
    If passed in aspect is None, then the existing aspect value will be removed"""
    remove_aspect_if_available(mce, aspect_type)
    if aspect is not None:
        mce.proposedSnapshot.aspects.append(aspect)  # type: ignore
