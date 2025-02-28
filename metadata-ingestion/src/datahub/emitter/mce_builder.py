"""Convenience functions for creating MCEs"""

import hashlib
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    get_type_hints,
    overload,
)

import typing_inspect
from avrogen.dict_wrapper import DictWrapper
from typing_extensions import assert_never

from datahub.emitter.enum_helpers import get_enum_options
from datahub.metadata.schema_classes import (
    AssertionKeyClass,
    AuditStampClass,
    ChartKeyClass,
    ContainerKeyClass,
    DashboardKeyClass,
    DatasetKeyClass,
    DatasetLineageTypeClass,
    DatasetSnapshotClass,
    FabricTypeClass,
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
from datahub.metadata.urns import DataFlowUrn, DatasetUrn, TagUrn
from datahub.utilities.urn_encoder import UrnEncoder

logger = logging.getLogger(__name__)
Aspect = TypeVar("Aspect", bound=AspectAbstract)

DEFAULT_ENV = FabricTypeClass.PROD
ALL_ENV_TYPES: Set[str] = set(get_enum_options(FabricTypeClass))

DEFAULT_FLOW_CLUSTER = "prod"
UNKNOWN_USER = "urn:li:corpuser:unknown"
DATASET_URN_TO_LOWER: bool = (
    os.getenv("DATAHUB_DATASET_URN_TO_LOWER", "false") == "true"
)

if TYPE_CHECKING:
    from datahub.emitter.mcp_builder import DatahubKey


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


@overload
def make_ts_millis(ts: None) -> None: ...


@overload
def make_ts_millis(ts: datetime) -> int: ...


def make_ts_millis(ts: Optional[datetime]) -> Optional[int]:
    # TODO: This duplicates the functionality of datetime_to_ts_millis
    if ts is None:
        return None
    return int(ts.timestamp() * 1000)


@overload
def parse_ts_millis(ts: float) -> datetime: ...


@overload
def parse_ts_millis(ts: None) -> None: ...


def parse_ts_millis(ts: Optional[float]) -> Optional[datetime]:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)


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


# Schema Field Urns url-encode reserved characters.
# TODO: This needs to be handled on consumer (UI) side well.
def make_schema_field_urn(parent_urn: str, field_path: str) -> str:
    assert parent_urn.startswith("urn:li:"), "Schema field's parent must be an urn"
    return f"urn:li:schemaField:({parent_urn},{UrnEncoder.encode_string(field_path)})"


def schema_field_urn_to_key(schema_field_urn: str) -> Optional[SchemaFieldKeyClass]:
    pattern = r"urn:li:schemaField:\((.*),(.*)\)"
    results = re.search(pattern, schema_field_urn)
    if results is not None:
        dataset_urn: str = results[1]
        field_path: str = results[2]
        return SchemaFieldKeyClass(parent=dataset_urn, fieldPath=field_path)
    return None


def dataset_urn_to_key(dataset_urn: str) -> Optional[DatasetKeyClass]:
    pattern = r"urn:li:dataset:\((.*),(.*),(.*)\)"
    results = re.search(pattern, dataset_urn)
    if results is not None:
        return DatasetKeyClass(platform=results[1], name=results[2], origin=results[3])
    return None


def dataset_key_to_urn(key: DatasetKeyClass) -> str:
    return f"urn:li:dataset:({key.platform},{key.name},{key.origin})"


def make_container_urn(guid: Union[str, "DatahubKey"]) -> str:
    if isinstance(guid, str) and guid.startswith("urn:li:container"):
        return guid
    else:
        from datahub.emitter.mcp_builder import DatahubKey

        if isinstance(guid, DatahubKey):
            guid = guid.guid()

        return f"urn:li:container:{guid}"


def container_urn_to_key(guid: str) -> Optional[ContainerKeyClass]:
    pattern = r"urn:li:container:(.*)"
    results = re.search(pattern, guid)
    if results is not None:
        return ContainerKeyClass(guid=results[1])
    return None


class _DatahubKeyJSONEncoder(json.JSONEncoder):
    # overload method default
    def default(self, obj: Any) -> Any:
        if hasattr(obj, "guid"):
            return obj.guid()
        # Call the default method for other types
        return json.JSONEncoder.default(self, obj)


def datahub_guid(obj: dict) -> str:
    json_key = json.dumps(
        obj,
        separators=(",", ":"),
        sort_keys=True,
        cls=_DatahubKeyJSONEncoder,
    )
    md5_hash = hashlib.md5(json_key.encode("utf-8"))
    return str(md5_hash.hexdigest())


def make_assertion_urn(assertion_id: str) -> str:
    return f"urn:li:assertion:{assertion_id}"


def assertion_urn_to_key(assertion_urn: str) -> Optional[AssertionKeyClass]:
    pattern = r"urn:li:assertion:(.*)"
    results = re.search(pattern, assertion_urn)
    if results is not None:
        return AssertionKeyClass(assertionId=results[1])
    return None


def make_user_urn(username: str) -> str:
    """
    Makes a user urn if the input is not a user or group urn already
    """
    return (
        f"urn:li:corpuser:{username}"
        if not username.startswith(("urn:li:corpuser:", "urn:li:corpGroup:"))
        else username
    )


def make_group_urn(groupname: str) -> str:
    """
    Makes a group urn if the input is not a user or group urn already
    """
    if groupname and groupname.startswith(("urn:li:corpGroup:", "urn:li:corpuser:")):
        return groupname
    else:
        return f"urn:li:corpGroup:{groupname}"


def make_tag_urn(tag: str) -> str:
    """
    Makes a tag urn if the input is not a tag urn already
    """
    if tag and tag.startswith("urn:li:tag:"):
        return tag
    return str(TagUrn(tag))


def make_owner_urn(owner: str, owner_type: OwnerType) -> str:
    if owner_type == OwnerType.USER:
        return make_user_urn(owner)
    elif owner_type == OwnerType.GROUP:
        return make_group_urn(owner)
    else:
        assert_never(owner_type)


def make_ownership_type_urn(type: str) -> str:
    return f"urn:li:ownershipType:{type}"


def make_term_urn(term: str) -> str:
    """
    Makes a term urn if the input is not a term urn already
    """
    if term and term.startswith("urn:li:glossaryTerm:"):
        return term
    else:
        return f"urn:li:glossaryTerm:{term}"


def make_data_flow_urn(
    orchestrator: str,
    flow_id: str,
    cluster: str = DEFAULT_FLOW_CLUSTER,
    platform_instance: Optional[str] = None,
) -> str:
    return str(
        DataFlowUrn.create_from_ids(
            orchestrator=orchestrator,
            flow_id=flow_id,
            env=cluster,
            platform_instance=platform_instance,
        )
    )


def make_data_job_urn_with_flow(flow_urn: str, job_id: str) -> str:
    return f"urn:li:dataJob:({flow_urn},{job_id})"


def make_data_process_instance_urn(dataProcessInstanceId: str) -> str:
    return f"urn:li:dataProcessInstance:{dataProcessInstanceId}"


def make_data_job_urn(
    orchestrator: str,
    flow_id: str,
    job_id: str,
    cluster: str = DEFAULT_FLOW_CLUSTER,
    platform_instance: Optional[str] = None,
) -> str:
    return make_data_job_urn_with_flow(
        make_data_flow_urn(orchestrator, flow_id, cluster, platform_instance), job_id
    )


def make_dashboard_urn(
    platform: str, name: str, platform_instance: Optional[str] = None
) -> str:
    # FIXME: dashboards don't currently include data platform urn prefixes.
    if platform_instance:
        return f"urn:li:dashboard:({platform},{platform_instance}.{name})"
    else:
        return f"urn:li:dashboard:({platform},{name})"


def dashboard_urn_to_key(dashboard_urn: str) -> Optional[DashboardKeyClass]:
    pattern = r"urn:li:dashboard:\((.*),(.*)\)"
    results = re.search(pattern, dashboard_urn)
    if results is not None:
        return DashboardKeyClass(dashboardTool=results[1], dashboardId=results[2])
    return None


def make_chart_urn(
    platform: str, name: str, platform_instance: Optional[str] = None
) -> str:
    # FIXME: charts don't currently include data platform urn prefixes.
    if platform_instance:
        return f"urn:li:chart:({platform},{platform_instance}.{name})"
    else:
        return f"urn:li:chart:({platform},{name})"


def chart_urn_to_key(chart_urn: str) -> Optional[ChartKeyClass]:
    pattern = r"urn:li:chart:\((.*),(.*)\)"
    results = re.search(pattern, chart_urn)
    if results is not None:
        return ChartKeyClass(dashboardTool=results[1], chartId=results[2])
    return None


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


def validate_ownership_type(ownership_type: str) -> Tuple[str, Optional[str]]:
    if ownership_type.startswith("urn:li:"):
        return OwnershipTypeClass.CUSTOM, ownership_type
    ownership_type = ownership_type.upper()
    if ownership_type in get_enum_options(OwnershipTypeClass):
        return ownership_type, None
    raise ValueError(f"Unexpected ownership type: {ownership_type}")


def make_lineage_mce(
    upstream_urns: List[str],
    downstream_urn: str,
    lineage_type: str = DatasetLineageTypeClass.TRANSFORMED,
) -> MetadataChangeEventClass:
    """
    Note: this function only supports lineage for dataset aspects. It will not
    update lineage for any other aspect types.
    """
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


def can_add_aspect_to_snapshot(
    SnapshotType: Type[DictWrapper], AspectType: Type[Aspect]
) -> bool:
    constructor_annotations = get_type_hints(SnapshotType.__init__)
    aspect_list_union = typing_inspect.get_args(constructor_annotations["aspects"])[0]

    supported_aspect_types = typing_inspect.get_args(aspect_list_union)

    return issubclass(AspectType, supported_aspect_types)


def can_add_aspect(mce: MetadataChangeEventClass, AspectType: Type[Aspect]) -> bool:
    # TODO: This is specific to snapshot types. We have a more general method
    # in `entity_supports_aspect`, which should be used instead. This method
    # should be deprecated, and all usages should be replaced.

    SnapshotType = type(mce.proposedSnapshot)

    return can_add_aspect_to_snapshot(SnapshotType, AspectType)


def assert_can_add_aspect(
    mce: MetadataChangeEventClass, AspectType: Type[Aspect]
) -> None:
    if not can_add_aspect(mce, AspectType):
        raise AssertionError(
            f"Cannot add aspect {AspectType} to {type(mce.proposedSnapshot)}"
        )


def get_aspect_if_available(
    mce: MetadataChangeEventClass, AspectType: Type[Aspect]
) -> Optional[Aspect]:
    assert_can_add_aspect(mce, AspectType)

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
    assert_can_add_aspect(mce, aspect_type)
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
        tags=[TagAssociationClass(make_tag_urn(tag)) for tag in tags]
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
