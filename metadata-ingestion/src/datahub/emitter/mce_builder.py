"""Convenience functions for creating MCEs"""
import json
import logging
import os
import re
import time
from enum import Enum
from hashlib import md5
from typing import Any, List, Optional, Type, TypeVar, Union, cast, get_type_hints

import typing_inspect

from datahub.configuration.source_common import DEFAULT_ENV as DEFAULT_ENV_CONFIGURATION
from datahub.emitter.serialization_helper import pre_json_transform
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ContainerKeyClass,
    DatasetKeyClass,
    DatasetLineageTypeClass,
    DatasetSnapshotClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass as GlossaryTerms,
    MetadataChangeEventClass,
    OwnerClass,
    OwnershipClass,
    OwnershipSourceClass,
    OwnershipSourceTypeClass,
    OwnershipTypeClass,
    SchemaFieldKeyClass,
    TagAssociationClass,
    UpstreamClass,
    UpstreamLineageClass,
    _Aspect as AspectAbstract,
)
from datahub.utilities.urns.dataset_urn import DatasetUrn

logger = logging.getLogger(__name__)
Aspect = TypeVar("Aspect", bound=AspectAbstract)

DEFAULT_ENV = DEFAULT_ENV_CONFIGURATION
DEFAULT_FLOW_CLUSTER = "prod"
UNKNOWN_USER = "urn:li:corpuser:unknown"
DATASET_URN_TO_LOWER: bool = (
    os.getenv("DATAHUB_DATASET_URN_TO_LOWER", "false") == "true"
)


# TODO: Delete this once lower-casing is the standard.
def set_dataset_urn_to_lower(value: bool) -> None:
    global DATASET_URN_TO_LOWER
    DATASET_URN_TO_LOWER = value


class OwnerType(Enum):
    USER = "corpuser"
    GROUP = "corpGroup"


def get_sys_time() -> int:
    # TODO deprecate this
    return int(time.time() * 1000)


def make_data_platform_urn(platform: str) -> str:
    if platform.startswith("urn:li:dataPlatform:"):
        return platform
    return f"urn:li:dataPlatform:{platform}"


def make_dataset_urn(platform: str, name: str, env: str = DEFAULT_ENV) -> str:
    return make_dataset_urn_with_platform_instance(
        platform=platform, name=name, platform_instance=None, env=env
    )


def make_dataplatform_instance_urn(platform: str, instance: str) -> str:
    if instance.startswith("urn:li:dataPlatformInstance"):
        return instance
    else:
        return f"urn:li:dataPlatformInstance:({make_data_platform_urn(platform)},{instance})"


def make_dataset_urn_with_platform_instance(
    platform: str, name: str, platform_instance: Optional[str], env: str = DEFAULT_ENV
) -> str:
    if DATASET_URN_TO_LOWER:
        name = name.lower()
    return str(
        DatasetUrn.create_from_ids(
            platform_id=platform,
            table_name=name,
            env=env,
            platform_instance=platform_instance,
        )
    )


def make_schema_field_urn(parent_urn: str, field_path: str) -> str:
    assert parent_urn.startswith("urn:li:"), "Schema field's parent must be an urn"
    return f"urn:li:schemaField:({parent_urn},{field_path})"


def schema_field_urn_to_key(schema_field_urn: str) -> Optional[SchemaFieldKeyClass]:
    pattern = r"urn:li:schemaField:\((.*),(.*)\)"
    results = re.search(pattern, schema_field_urn)
    if results is not None:
        dataset_urn: str = results[1]
        field_path: str = results[2]
        return SchemaFieldKeyClass(parent=dataset_urn, fieldPath=field_path)
    return None


def dataset_urn_to_key(dataset_urn: str) -> Optional[DatasetKeyClass]:
    pattern = r"urn:li:dataset:\(urn:li:dataPlatform:(.*),(.*),(.*)\)"
    results = re.search(pattern, dataset_urn)
    if results is not None:
        return DatasetKeyClass(platform=results[1], name=results[2], origin=results[3])
    return None


def make_container_new_urn(guid: str) -> str:
    return f"urn:dh:container:0:({guid})"


def container_new_urn_to_key(dataset_urn: str) -> Optional[ContainerKeyClass]:
    pattern = r"urn:dh:container:0:\((.*)\)"
    results = re.search(pattern, dataset_urn)
    if results is not None:
        return ContainerKeyClass(guid=results[1])
    return None


# def make_container_urn(platform: str, name: str, env: str = DEFAULT_ENV) -> str:
#    return f"urn:li:container:({make_data_platform_urn(platform)},{env},{name})"


def make_container_urn(guid: str) -> str:
    return f"urn:li:container:{guid}"


def container_urn_to_key(guid: str) -> Optional[ContainerKeyClass]:
    pattern = r"urn:li:container:(.*)"
    results = re.search(pattern, guid)
    if results is not None:
        return ContainerKeyClass(guid=results[1])
    return None


def datahub_guid(obj: dict) -> str:
    obj_str = json.dumps(
        pre_json_transform(obj), separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    return md5(obj_str).hexdigest()


def make_assertion_urn(assertion_id: str) -> str:
    return f"urn:li:assertion:{assertion_id}"


def make_user_urn(username: str) -> str:
    return f"urn:li:corpuser:{username}"


def make_group_urn(groupname: str) -> str:
    return f"urn:li:corpGroup:{groupname}"


def make_tag_urn(tag: str) -> str:
    return f"urn:li:tag:{tag}"


def make_owner_urn(owner: str, owner_type: OwnerType) -> str:
    return f"urn:li:{owner_type.value}:{owner}"


def make_term_urn(term: str) -> str:
    return f"urn:li:glossaryTerm:{term}"


def make_data_flow_urn(
    orchestrator: str, flow_id: str, cluster: str = DEFAULT_FLOW_CLUSTER
) -> str:
    return f"urn:li:dataFlow:({orchestrator},{flow_id},{cluster})"


def make_data_job_urn_with_flow(flow_urn: str, job_id: str) -> str:
    return f"urn:li:dataJob:({flow_urn},{job_id})"


def make_data_process_instance_urn(dataProcessInstanceId: str) -> str:
    return f"urn:li:dataProcessInstance:{dataProcessInstanceId}"


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


def make_domain_urn(domain: str) -> str:
    if domain.startswith("urn:li:domain:"):
        return domain
    return f"urn:li:domain:{domain}"


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
        OwnershipTypeClass.TECHNICAL_OWNER,
        OwnershipTypeClass.BUSINESS_OWNER,
        OwnershipTypeClass.DATA_STEWARD,
        OwnershipTypeClass.NONE,
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


def make_ownership_aspect_from_urn_list(
    owner_urns: List[str],
    source_type: Optional[Union[str, OwnershipSourceTypeClass]],
    owner_type: Union[str, OwnershipTypeClass] = OwnershipTypeClass.DATAOWNER,
) -> OwnershipClass:
    for owner_urn in owner_urns:
        assert owner_urn.startswith("urn:li:corpuser:") or owner_urn.startswith(
            "urn:li:corpGroup:"
        )
    ownership_source_type: Union[None, OwnershipSourceClass] = None
    if source_type:
        ownership_source_type = OwnershipSourceClass(type=source_type)

    owners_list = [
        OwnerClass(
            owner=owner_urn,
            type=owner_type,
            source=ownership_source_type,
        )
        for owner_urn in owner_urns
    ]
    return OwnershipClass(
        owners=owners_list,
    )


def make_glossary_terms_aspect_from_urn_list(term_urns: List[str]) -> GlossaryTerms:
    for term_urn in term_urns:
        assert term_urn.startswith("urn:li:glossaryTerm:")
    glossary_terms = GlossaryTerms(
        [GlossaryTermAssociationClass(term_urn) for term_urn in term_urns],
        AuditStampClass(
            time=int(time.time() * 1000),
            actor="urn:li:corpuser:datahub",
        ),
    )
    return glossary_terms


def set_aspect(
    mce: MetadataChangeEventClass, aspect: Optional[Aspect], aspect_type: Type[Aspect]
) -> None:
    """Sets the aspect to the provided aspect, overwriting any previous aspect value that might have existed before.
    If passed in aspect is None, then the existing aspect value will be removed"""
    remove_aspect_if_available(mce, aspect_type)
    if aspect is not None:
        mce.proposedSnapshot.aspects.append(aspect)  # type: ignore
