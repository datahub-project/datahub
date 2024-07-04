import datetime
import itertools
import logging
import os
import re
from contextlib import contextmanager
from dataclasses import dataclass, field as dataclasses_field
from enum import Enum
from functools import lru_cache
from typing import (
    TYPE_CHECKING,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
    cast,
)

from looker_sdk.error import SDKError
from looker_sdk.sdk.api40.models import (
    LookmlModelExplore,
    LookmlModelExploreField,
    User,
    WriteQuery,
)
from pydantic.class_validators import validator

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ContainerKey, create_embed_mcp
from datahub.ingestion.api.report import Report
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.looker.looker_config import (
    LookerCommonConfig,
    LookerDashboardSourceConfig,
    NamingPatternMapping,
    ViewNamingPatternMapping,
)
from datahub.ingestion.source.looker.looker_constant import IMPORTED_PROJECTS
from datahub.ingestion.source.looker.looker_lib_wrapper import LookerAPI
from datahub.ingestion.source.sql.sql_types import (
    POSTGRES_TYPES_MAP,
    SNOWFLAKE_TYPES_MAP,
    resolve_postgres_modified_type,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import AuditStamp
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageTypeClass,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    UpstreamClass,
    UpstreamLineage,
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    OtherSchema,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringTypeClass,
    TimeTypeClass,
    UnionTypeClass,
)
from datahub.metadata.schema_classes import (
    BrowsePathEntryClass,
    BrowsePathsClass,
    BrowsePathsV2Class,
    ContainerClass,
    DatasetPropertiesClass,
    EnumTypeClass,
    FineGrainedLineageClass,
    GlobalTagsClass,
    SchemaMetadataClass,
    StatusClass,
    SubTypesClass,
    TagAssociationClass,
    TagPropertiesClass,
    TagSnapshotClass,
)
from datahub.metadata.urns import TagUrn
from datahub.utilities.lossy_collections import LossyList, LossySet
from datahub.utilities.url_util import remove_port_from_url

CORPUSER_DATAHUB = "urn:li:corpuser:datahub"

if TYPE_CHECKING:
    from datahub.ingestion.source.looker.lookml_source import (
        LookerViewFileLoader,
        LookMLSourceReport,
    )

logger = logging.getLogger(__name__)


@dataclass
class LookerFolder:
    id: str
    name: str
    parent_id: Optional[str]


class LookMLProjectKey(ContainerKey):
    project_name: str


class LookMLModelKey(ContainerKey):
    model_name: str


class LookerFolderKey(ContainerKey):
    folder_id: str


def remove_suffix(original: str, suffix: str) -> str:
    # This can be removed in favour of original.removesuffix for python>3.8
    if original.endswith(suffix):
        return original[: -len(suffix)]
    return original


@dataclass
class LookerViewId:
    project_name: str
    model_name: str
    view_name: str
    file_path: str

    def get_mapping(self, config: LookerCommonConfig) -> ViewNamingPatternMapping:
        return ViewNamingPatternMapping(
            platform=config.platform_name,
            env=config.env.lower(),
            project=self.project_name,
            model=self.model_name,
            name=self.view_name,
            file_path=remove_suffix(self.file_path, ".view.lkml"),
            folder_path=os.path.dirname(self.file_path),
        )

    @validator("view_name")
    def remove_quotes(cls, v):
        # Sanitize the name.
        v = v.replace('"', "").replace("`", "")
        return v

    def preprocess_file_path(self, file_path: str) -> str:
        new_file_path: str = str(file_path)

        str_to_remove: List[str] = [
            "\\.view\\.lkml$",  # escape the . using \
        ]

        for pattern in str_to_remove:
            new_file_path = re.sub(pattern, "", new_file_path)

        str_to_replace: Dict[str, str] = {
            f"^imported_projects/{re.escape(self.project_name)}/": "",  # escape any special regex character present in project-name
            "/": ".",  # / is not urn friendly
        }

        for pattern in str_to_replace:
            new_file_path = re.sub(pattern, str_to_replace[pattern], new_file_path)

        logger.debug(f"Original file path {file_path}")
        logger.debug(f"After preprocessing file path {new_file_path}")

        return new_file_path

    def get_urn(self, config: LookerCommonConfig) -> str:
        n_mapping: ViewNamingPatternMapping = self.get_mapping(config)

        n_mapping.file_path = self.preprocess_file_path(n_mapping.file_path)

        dataset_name = config.view_naming_pattern.replace_variables(n_mapping)

        return builder.make_dataset_urn_with_platform_instance(
            platform=config.platform_name,
            name=dataset_name,
            platform_instance=config.platform_instance,
            env=config.env,
        )

    def get_browse_path(self, config: LookerCommonConfig) -> str:
        browse_path = config.view_browse_pattern.replace_variables(
            self.get_mapping(config)
        )
        return browse_path

    def get_browse_path_v2(self, config: LookerCommonConfig) -> BrowsePathsV2Class:
        project_key = gen_project_key(config, self.project_name)
        view_path = (
            remove_suffix(self.file_path, ".view.lkml")
            if "{file_path}" in config.view_browse_pattern.pattern
            else os.path.dirname(self.file_path)
        )
        if view_path:
            path_entries = [
                BrowsePathEntryClass(id=path) for path in view_path.split("/")
            ]
        else:
            path_entries = []
        return BrowsePathsV2Class(
            path=[
                BrowsePathEntryClass(id="Develop"),
                BrowsePathEntryClass(id=project_key.as_urn(), urn=project_key.as_urn()),
                *path_entries,
            ],
        )


class ViewFieldType(Enum):
    DIMENSION = "Dimension"
    DIMENSION_GROUP = "Dimension Group"
    MEASURE = "Measure"
    UNKNOWN = "Unknown"


class ViewFieldValue(Enum):
    NOT_AVAILABLE = "NotAvailable"


@dataclass
class ViewField:
    name: str
    label: Optional[str]
    type: str
    description: str
    field_type: ViewFieldType
    project_name: Optional[str] = None
    view_name: Optional[str] = None
    is_primary_key: bool = False
    upstream_fields: List[str] = dataclasses_field(default_factory=list)
    tags: List[str] = dataclasses_field(default_factory=list)


@dataclass
class ExploreUpstreamViewField:
    explore: LookmlModelExplore
    field: LookmlModelExploreField

    def _form_field_name(self):
        assert self.field.name is not None

        if len(self.field.name.split(".")) != 2:
            return self.field.name  # Inconsistent info received

        view_name: Optional[str] = self.explore.name

        if (
            self.field.original_view is not None
        ):  # if `from` is used in explore then original_view is pointing to
            # lookml view
            view_name = self.field.original_view

        field_name = self.field.name.split(".")[1]

        return f"{view_name}.{field_name}"

    def upstream(self) -> str:
        assert self.field.name is not None

        if self.field.dimension_group is None:  # It is not part of Dimensional Group
            return self._form_field_name()

        if self.field.field_group_variant is None:
            return (
                self._form_field_name()
            )  # Variant i.e. Month, Day, Year ... is not available

        if self.field.type is None or not self.field.type.startswith("date_"):
            return (
                self._form_field_name()
            )  # for Dimensional Group the type is always start with date_[time|date]

        if not self.field.name.endswith(f"_{self.field.field_group_variant.lower()}"):
            return (
                self._form_field_name()
            )  # if the explore field is generated because of  Dimensional Group in View
            # then the field_name should ends with field_group_variant

        return self._form_field_name()[
            : -(len(self.field.field_group_variant.lower()) + 1)
        ]  # remove variant at the end. +1 for "_"


def create_view_project_map(view_fields: List[ViewField]) -> Dict[str, str]:
    """
    Each view in a model has unique name.
    Use this function in scope of a model.
    """
    view_project_map: Dict[str, str] = {}
    for view_field in view_fields:
        if view_field.view_name is not None and view_field.project_name is not None:
            view_project_map[view_field.view_name] = view_field.project_name

    return view_project_map


def get_view_file_path(
    lkml_fields: List[LookmlModelExploreField], view_name: str
) -> Optional[str]:
    """
    Search for the view file path on field, if found then return the file path
    """
    logger.debug("Entered")

    for field in lkml_fields:
        if field.view == view_name:
            # This path is relative to git clone directory
            logger.debug(f"Found view({view_name}) file-path {field.source_file}")
            return field.source_file

    logger.debug(f"Failed to find view({view_name}) file-path")

    return None


def create_upstream_views_file_path_map(
    view_names: Set[str], lkml_fields: List[LookmlModelExploreField]
) -> Dict[str, Optional[str]]:
    """
    Create a map of view-name v/s view file path, so that later we can fetch view's file path via view-name
    """

    upstream_views_file_path: Dict[str, Optional[str]] = {}

    for view_name in view_names:
        file_path: Optional[str] = get_view_file_path(
            lkml_fields=lkml_fields, view_name=view_name
        )

        upstream_views_file_path[view_name] = file_path

    return upstream_views_file_path


def explore_field_set_to_lkml_fields(
    explore: LookmlModelExplore,
) -> List[LookmlModelExploreField]:
    """
    explore.fields has three variables i.e. dimensions, measures, parameters of same type i.e. LookmlModelExploreField.
    This method creating a list by adding all field instance to lkml_fields
    """
    lkml_fields: List[LookmlModelExploreField] = []

    if explore.fields is None:
        logger.debug(f"Explore({explore.name}) doesn't have any field")
        return lkml_fields

    def empty_list(
        fields: Optional[Sequence[LookmlModelExploreField]],
    ) -> List[LookmlModelExploreField]:
        return list(fields) if fields is not None else []

    lkml_fields.extend(empty_list(explore.fields.dimensions))
    lkml_fields.extend(empty_list(explore.fields.measures))
    lkml_fields.extend(empty_list(explore.fields.parameters))

    return lkml_fields


class LookerUtil:
    field_type_mapping = {
        **POSTGRES_TYPES_MAP,
        **SNOWFLAKE_TYPES_MAP,
        "date": DateTypeClass,
        "date_date": DateTypeClass,
        "date_day_of_month": NumberTypeClass,
        "date_day_of_week": EnumTypeClass,
        "date_day_of_week_index": EnumTypeClass,
        "date_fiscal_month_num": NumberTypeClass,
        "date_fiscal_quarter": DateTypeClass,
        "date_fiscal_quarter_of_year": EnumTypeClass,
        "date_hour": TimeTypeClass,
        "date_hour_of_day": NumberTypeClass,
        "date_month": DateTypeClass,
        "date_month_num": NumberTypeClass,
        "date_month_name": EnumTypeClass,
        "date_quarter": DateTypeClass,
        "date_quarter_of_year": EnumTypeClass,
        "date_time": TimeTypeClass,
        "date_time_of_day": TimeTypeClass,
        "date_microsecond": TimeTypeClass,
        "date_millisecond": TimeTypeClass,
        "date_minute": TimeTypeClass,
        "date_raw": TimeTypeClass,
        "date_second": TimeTypeClass,
        "date_week": TimeTypeClass,
        "date_year": DateTypeClass,
        "date_day_of_year": NumberTypeClass,
        "date_week_of_year": NumberTypeClass,
        "date_fiscal_year": DateTypeClass,
        "duration_day": StringTypeClass,
        "duration_hour": StringTypeClass,
        "duration_minute": StringTypeClass,
        "duration_month": StringTypeClass,
        "duration_quarter": StringTypeClass,
        "duration_second": StringTypeClass,
        "duration_week": StringTypeClass,
        "duration_year": StringTypeClass,
        "distance": NumberTypeClass,
        "duration": NumberTypeClass,
        "location": UnionTypeClass,
        "number": NumberTypeClass,
        "string": StringTypeClass,
        "tier": EnumTypeClass,
        "time": TimeTypeClass,
        "unquoted": StringTypeClass,
        "yesno": BooleanTypeClass,
        "zipcode": EnumTypeClass,
        "int": NumberTypeClass,
        "average": NumberTypeClass,
        "average_distinct": NumberTypeClass,
        "count": NumberTypeClass,
        "count_distinct": NumberTypeClass,
        "list": ArrayTypeClass,
        "max": NumberTypeClass,
        "median": NumberTypeClass,
        "median_distinct": NumberTypeClass,
        "min": NumberTypeClass,
        "percent_of_previous": NumberTypeClass,
        "percent_of_total": NumberTypeClass,
        "percentile": NumberTypeClass,
        "percentile_distinct": NumberTypeClass,
        "running_total": NumberTypeClass,
        "sum": NumberTypeClass,
        "sum_distinct": NumberTypeClass,
        "unknown": NullTypeClass,
    }

    @staticmethod
    def _extract_view_from_field(field: str) -> str:
        assert (
            field.count(".") == 1
        ), f"Error: A field must be prefixed by a view name, field is: {field}"
        return field.split(".")[0]

    @staticmethod
    def extract_view_name_from_lookml_model_explore_field(
        field: LookmlModelExploreField,
    ) -> Optional[str]:
        """
        View name is either present in original_view or view property
        """
        if field.original_view is not None:
            return field.original_view

        return field.view

    @staticmethod
    def extract_project_name_from_source_file(
        source_file: Optional[str],
    ) -> Optional[str]:
        """
        source_file is a key inside explore.fields. This key point to relative path of included views.
        if view is included from another project then source_file is starts with "imported_projects".
        Example: imported_projects/datahub-demo/views/datahub-demo/datasets/faa_flights.view.lkml
        """
        if source_file is None:
            return None

        if source_file.startswith(IMPORTED_PROJECTS):
            tokens: List[str] = source_file.split("/")
            if len(tokens) >= 2:
                return tokens[1]  # second index is project-name

        return None

    @staticmethod
    def _get_field_type(
        native_type: str, reporter: SourceReport
    ) -> SchemaFieldDataType:
        type_class = LookerUtil.field_type_mapping.get(native_type)

        if type_class is None:
            # attempt Postgres modified type
            type_class = resolve_postgres_modified_type(native_type)

        # if still not found, log and continue
        if type_class is None:
            logger.debug(
                f"The type '{native_type}' is not recognized for field type, setting as NullTypeClass.",
            )
            type_class = NullTypeClass

        return SchemaFieldDataType(type=type_class())

    @staticmethod
    def _get_schema(
        platform_name: str,
        schema_name: str,
        view_fields: List[ViewField],
        reporter: SourceReport,
    ) -> Optional[SchemaMetadataClass]:
        if not view_fields:
            return None
        fields, primary_keys = LookerUtil._get_fields_and_primary_keys(
            view_fields=view_fields, reporter=reporter
        )
        schema_metadata = SchemaMetadata(
            schemaName=schema_name,
            platform=f"urn:li:dataPlatform:{platform_name}",
            version=0,
            fields=fields,
            primaryKeys=primary_keys,
            hash="",
            platformSchema=OtherSchema(rawSchema=""),
        )
        return schema_metadata

    DIMENSION_TAG_URN = "urn:li:tag:Dimension"
    TEMPORAL_TAG_URN = "urn:li:tag:Temporal"
    MEASURE_TAG_URN = "urn:li:tag:Measure"

    type_to_tag_map: Dict[ViewFieldType, List[str]] = {
        ViewFieldType.DIMENSION: [DIMENSION_TAG_URN],
        ViewFieldType.DIMENSION_GROUP: [
            DIMENSION_TAG_URN,
            TEMPORAL_TAG_URN,
        ],
        ViewFieldType.MEASURE: [MEASURE_TAG_URN],
        ViewFieldType.UNKNOWN: [],
    }

    tag_definitions: Dict[str, TagPropertiesClass] = {
        DIMENSION_TAG_URN: TagPropertiesClass(
            name=DIMENSION_TAG_URN.split(":")[-1],
            description="A tag that is applied to all dimension fields.",
        ),
        TEMPORAL_TAG_URN: TagPropertiesClass(
            name=TEMPORAL_TAG_URN.split(":")[-1],
            description="A tag that is applied to all time-based (temporal) fields such as timestamps or durations.",
        ),
        MEASURE_TAG_URN: TagPropertiesClass(
            name=MEASURE_TAG_URN.split(":")[-1],
            description="A tag that is applied to all measures (metrics). Measures are typically the columns that you aggregate on",
        ),
    }

    @staticmethod
    def _get_tag_mce_for_urn(tag_urn: str) -> MetadataChangeEvent:
        assert tag_urn in LookerUtil.tag_definitions
        return MetadataChangeEvent(
            proposedSnapshot=TagSnapshotClass(
                urn=tag_urn, aspects=[LookerUtil.tag_definitions[tag_urn]]
            )
        )

    @staticmethod
    def _get_tags_from_field_type(
        field: ViewField, reporter: SourceReport
    ) -> Optional[GlobalTagsClass]:
        schema_field_tags: List[TagAssociationClass] = [
            TagAssociationClass(tag=builder.make_tag_urn(tag_name))
            for tag_name in field.tags
        ]

        if field.field_type in LookerUtil.type_to_tag_map:
            schema_field_tags.extend(
                [
                    TagAssociationClass(tag=tag_name)
                    for tag_name in LookerUtil.type_to_tag_map[field.field_type]
                ]
            )
        else:
            reporter.report_warning(
                title="Failed to Map View Field Type",
                message=f"Failed to map view field type {field.field_type}. Won't emit tags for measure and dimension",
            )

        if schema_field_tags:
            return GlobalTagsClass(tags=schema_field_tags)

        return None

    @staticmethod
    def get_tag_mces() -> Iterable[MetadataChangeEvent]:
        # Emit tag MCEs for measures and dimensions:
        return [
            LookerUtil._get_tag_mce_for_urn(tag_urn)
            for tag_urn in LookerUtil.tag_definitions
        ]

    @staticmethod
    def view_field_to_schema_field(
        field: ViewField,
        reporter: SourceReport,
        tag_measures_and_dimensions: bool = True,
    ) -> SchemaField:
        return SchemaField(
            fieldPath=field.name,
            type=LookerUtil._get_field_type(field.type, reporter),
            nativeDataType=field.type,
            label=field.label,
            description=(
                field.description
                if tag_measures_and_dimensions is True
                else f"{field.field_type.value}. {field.description}"
            ),
            globalTags=(
                LookerUtil._get_tags_from_field_type(field, reporter)
                if tag_measures_and_dimensions is True
                else None
            ),
            isPartOfKey=field.is_primary_key,
        )

    @staticmethod
    def _get_fields_and_primary_keys(
        view_fields: List[ViewField],
        reporter: SourceReport,
        tag_measures_and_dimensions: bool = True,
    ) -> Tuple[List[SchemaField], List[str]]:
        primary_keys: List = []
        fields = []
        for field in view_fields:
            schema_field = LookerUtil.view_field_to_schema_field(
                field, reporter, tag_measures_and_dimensions
            )
            fields.append(schema_field)
            if field.is_primary_key:
                primary_keys.append(schema_field.fieldPath)
        return fields, primary_keys

    @staticmethod
    def _display_name(name: str) -> str:
        """Returns a display name that corresponds to the Looker conventions"""
        return name.replace("_", " ").title() if name else name

    @staticmethod
    def create_query_request(q: dict, limit: Optional[str] = None) -> WriteQuery:
        return WriteQuery(
            model=q["model"],
            view=q["view"],
            fields=q.get("fields"),
            filters=q.get("filters"),
            filter_expression=q.get("filter_expressions"),
            sorts=q.get("sorts"),
            limit=q.get("limit") or limit,
            column_limit=q.get("column_limit"),
            vis_config={"type": "looker_column"},
            filter_config=q.get("filter_config"),
            query_timezone="UTC",
        )


@dataclass(frozen=True, order=True)
class ProjectInclude:
    project: str
    include: str


@dataclass
class LookerExplore:
    name: str
    model_name: str
    project_name: Optional[str] = None
    label: Optional[str] = None
    description: Optional[str] = None
    upstream_views: Optional[
        List[ProjectInclude]
    ] = None  # captures the view name(s) this explore is derived from
    upstream_views_file_path: Dict[str, Optional[str]] = dataclasses_field(
        default_factory=dict
    )  # view_name is key and file_path is value. A single file may contains multiple views
    joins: Optional[List[str]] = None
    fields: Optional[List[ViewField]] = None  # the fields exposed in this explore
    source_file: Optional[str] = None
    tags: List[str] = dataclasses_field(default_factory=list)

    @validator("name")
    def remove_quotes(cls, v):
        # Sanitize the name.
        v = v.replace('"', "").replace("`", "")
        return v

    @staticmethod
    def _get_fields_from_sql_equality(sql_fragment: str) -> List[str]:
        field_match = re.compile(r"\${([^}]+)}")
        return field_match.findall(sql_fragment)

    @classmethod
    def from_dict(
        cls,
        model_name: str,
        dict: Dict,
        resolved_includes: List[ProjectInclude],
        looker_viewfile_loader: "LookerViewFileLoader",
        reporter: "LookMLSourceReport",
        model_explores_map: Dict[str, dict],
    ) -> "LookerExplore":
        view_names: Set[str] = set()
        joins = None
        assert "name" in dict, "Explore doesn't have a name field, this isn't allowed"

        # The view name that the explore refers to is resolved in the following order of priority:
        # 1. view_name: https://cloud.google.com/looker/docs/reference/param-explore-view-name
        # 2. from: https://cloud.google.com/looker/docs/reference/param-explore-from
        # 3. default to the name of the explore
        view_names.add(dict.get("view_name") or dict.get("from") or dict["name"])

        if dict.get("joins", {}) != {}:
            # additionally for join-based explores, pull in the linked views
            assert "joins" in dict
            for join in dict["joins"]:
                join_from = join.get("from")
                view_names.add(join_from or join["name"])
                sql_on = join.get("sql_on", None)
                if sql_on is not None:
                    fields = cls._get_fields_from_sql_equality(sql_on)
                    joins = fields

        # HACK: We shouldn't be doing imports here. We also have
        # circular imports that don't belong.
        from datahub.ingestion.source.looker.lookml_source import (
            _find_view_from_resolved_includes,
        )

        upstream_views: List[ProjectInclude] = []
        # create the list of extended explores
        extends = list(
            itertools.chain.from_iterable(
                dict.get("extends", dict.get("extends__all", []))
            )
        )
        if extends:
            for extended_explore in extends:
                if extended_explore in model_explores_map:
                    parsed_explore = LookerExplore.from_dict(
                        model_name,
                        model_explores_map[extended_explore],
                        resolved_includes,
                        looker_viewfile_loader,
                        reporter,
                        model_explores_map,
                    )
                    upstream_views.extend(parsed_explore.upstream_views or [])
                else:
                    logger.warning(
                        f'Could not find extended explore {extended_explore} for explore {dict["name"]} in model {model_name}'
                    )
        else:
            # we only fallback to the view_names list if this is not an extended explore
            for view_name in view_names:
                info = _find_view_from_resolved_includes(
                    None,
                    resolved_includes,
                    looker_viewfile_loader,
                    view_name,
                    reporter,
                )
                if not info:
                    logger.warning(
                        f'Could not resolve view {view_name} for explore {dict["name"]} in model {model_name}'
                    )
                else:
                    upstream_views.append(
                        ProjectInclude(project=info[0].project, include=view_name)
                    )

        return LookerExplore(
            model_name=model_name,
            name=dict["name"],
            label=dict.get("label"),
            description=dict.get("description"),
            upstream_views=upstream_views,
            joins=joins,
            # This method is getting called from lookml_source's get_internal_workunits method
            # & upstream_views_file_path is not in use in that code flow
            upstream_views_file_path={},
            tags=cast(List, dict.get("tags")) if dict.get("tags") is not None else [],
        )

    @classmethod  # noqa: C901
    def from_api(  # noqa: C901
        cls,
        model: str,
        explore_name: str,
        client: LookerAPI,
        reporter: SourceReport,
        source_config: LookerDashboardSourceConfig,
    ) -> Optional["LookerExplore"]:  # noqa: C901
        from datahub.ingestion.source.looker.lookml_source import _BASE_PROJECT_NAME

        try:
            explore = client.lookml_model_explore(model, explore_name)
            views: Set[str] = set()
            lkml_fields: List[
                LookmlModelExploreField
            ] = explore_field_set_to_lkml_fields(explore)

            if explore.view_name is not None and explore.view_name != explore.name:
                # explore is not named after a view and is instead using a from field, which is modeled as view_name.
                aliased_explore = True
                views.add(explore.view_name)
            else:
                # otherwise, the explore name is a view, so add it to the set.
                aliased_explore = False
                if explore.name is not None:
                    views.add(explore.name)

            if explore.joins is not None and explore.joins != []:
                join_to_orig_name_map = {}
                potential_views = []
                for e in explore.joins:
                    if e.from_ is not None:
                        potential_views.append(e.from_)
                        join_to_orig_name_map[e.name] = e.from_
                    elif e.name is not None:
                        potential_views.append(e.name)
                for e_join in [
                    e for e in explore.joins if e.dependent_fields is not None
                ]:
                    assert e_join.dependent_fields is not None
                    for field_name in e_join.dependent_fields:
                        try:
                            view_name = LookerUtil._extract_view_from_field(field_name)
                            orig_name = join_to_orig_name_map.get(e_join.name)
                            if orig_name is not None:
                                view_name = orig_name
                            potential_views.append(view_name)
                        except AssertionError:
                            reporter.report_warning(
                                title="Missing View Name",
                                message="The field was not prefixed by a view name. This can happen when the field references another dynamic field.",
                                context=view_name,
                            )
                            continue

                for view_name in potential_views:
                    if (view_name == explore.name) and aliased_explore:
                        # if the explore is aliased, then the joins could be referring to views using the aliased name.
                        # this needs to be corrected by switching to the actual view name of the explore
                        assert explore.view_name is not None
                        view_name = explore.view_name
                    views.add(view_name)

            view_fields: List[ViewField] = []
            if explore.fields is not None:
                if explore.fields.dimensions is not None:
                    for dim_field in explore.fields.dimensions:
                        if dim_field.name is None:
                            continue
                        else:
                            dimension_upstream_field: ExploreUpstreamViewField = (
                                ExploreUpstreamViewField(
                                    explore=explore,
                                    field=dim_field,
                                )
                            )

                            view_fields.append(
                                ViewField(
                                    name=dim_field.name,
                                    label=dim_field.label_short,
                                    description=(
                                        dim_field.description
                                        if dim_field.description
                                        else ""
                                    ),
                                    type=(
                                        dim_field.type
                                        if dim_field.type is not None
                                        else ""
                                    ),
                                    field_type=(
                                        ViewFieldType.DIMENSION_GROUP
                                        if dim_field.dimension_group is not None
                                        else ViewFieldType.DIMENSION
                                    ),
                                    project_name=LookerUtil.extract_project_name_from_source_file(
                                        dim_field.source_file
                                    ),
                                    view_name=LookerUtil.extract_view_name_from_lookml_model_explore_field(
                                        dim_field
                                    ),
                                    is_primary_key=(
                                        dim_field.primary_key
                                        if dim_field.primary_key
                                        else False
                                    ),
                                    upstream_fields=[
                                        dimension_upstream_field.upstream()
                                    ],
                                )
                            )
                if explore.fields.measures is not None:
                    for measure_field in explore.fields.measures:
                        if measure_field.name is None:
                            continue
                        else:
                            measure_upstream_field: ExploreUpstreamViewField = (
                                ExploreUpstreamViewField(
                                    explore=explore,
                                    field=measure_field,
                                )
                            )

                            view_fields.append(
                                ViewField(
                                    name=measure_field.name,
                                    label=measure_field.label_short,
                                    description=(
                                        measure_field.description
                                        if measure_field.description
                                        else ""
                                    ),
                                    type=(
                                        measure_field.type
                                        if measure_field.type is not None
                                        else ""
                                    ),
                                    field_type=ViewFieldType.MEASURE,
                                    project_name=LookerUtil.extract_project_name_from_source_file(
                                        measure_field.source_file
                                    ),
                                    view_name=LookerUtil.extract_view_name_from_lookml_model_explore_field(
                                        measure_field
                                    ),
                                    is_primary_key=(
                                        measure_field.primary_key
                                        if measure_field.primary_key
                                        else False
                                    ),
                                    upstream_fields=[measure_upstream_field.upstream()],
                                )
                            )

            view_project_map: Dict[str, str] = create_view_project_map(view_fields)
            if view_project_map:
                logger.debug(f"views and their projects: {view_project_map}")

            upstream_views_file_path: Dict[
                str, Optional[str]
            ] = create_upstream_views_file_path_map(
                lkml_fields=lkml_fields,
                view_names=views,
            )
            if upstream_views_file_path:
                logger.debug(f"views and their file-paths: {upstream_views_file_path}")

            return cls(
                name=explore_name,
                model_name=model,
                project_name=explore.project_name,
                label=explore.label,
                description=explore.description,
                fields=view_fields,
                upstream_views=list(
                    ProjectInclude(
                        project=view_project_map.get(view_name, _BASE_PROJECT_NAME),
                        include=view_name,
                    )
                    for view_name in views
                ),
                upstream_views_file_path=upstream_views_file_path,
                source_file=explore.source_file,
                tags=list(explore.tags) if explore.tags is not None else [],
            )
        except SDKError as e:
            if "<title>Looker Not Found (404)</title>" in str(e):
                logger.info(
                    f"Explore {explore_name} in model {model} is referred to, but missing. Continuing..."
                )
            else:
                logger.warning(
                    f"Failed to extract explore {explore_name} from model {model}: {e}"
                )

        except AssertionError:
            reporter.report_warning(
                title="Unable to find Views",
                message="Encountered exception while attempting to find dependent views for this chart",
                context=f"Explore: {explore_name}, Mode: {model}, Views: {views}",
            )
        return None

    def get_mapping(self, config: LookerCommonConfig) -> NamingPatternMapping:
        return NamingPatternMapping(
            platform=config.platform_name,
            project=self.project_name,  # type: ignore
            model=self.model_name,
            name=self.name,
            env=config.env.lower(),
        )

    def get_explore_urn(self, config: LookerCommonConfig) -> str:
        dataset_name = config.explore_naming_pattern.replace_variables(
            self.get_mapping(config)
        )

        return builder.make_dataset_urn_with_platform_instance(
            platform=config.platform_name,
            name=dataset_name,
            platform_instance=config.platform_instance,
            env=config.env,
        )

    def get_explore_browse_path(self, config: LookerCommonConfig) -> str:
        browse_path = config.explore_browse_pattern.replace_variables(
            self.get_mapping(config)
        )
        return browse_path

    def _get_url(self, base_url):
        base_url = remove_port_from_url(base_url)
        return f"{base_url}/explore/{self.model_name}/{self.name}"

    def _get_embed_url(self, base_url: str) -> str:
        base_url = remove_port_from_url(base_url)
        return f"{base_url}/embed/explore/{self.model_name}/{self.name}"

    def _to_metadata_events(  # noqa: C901
        self,
        config: LookerCommonConfig,
        reporter: SourceReport,
        base_url: str,
        extract_embed_urls: bool,
    ) -> Optional[List[Union[MetadataChangeEvent, MetadataChangeProposalWrapper]]]:
        # We only generate MCE-s for explores that contain from clauses and do NOT contain joins
        # All other explores (passthrough explores and joins) end in correct resolution of lineage, and don't need additional nodes in the graph.
        from datahub.ingestion.source.looker.lookml_source import _BASE_PROJECT_NAME

        dataset_snapshot = DatasetSnapshot(
            urn=self.get_explore_urn(config),
            aspects=[],  # we append to this list later on
        )

        model_key = gen_model_key(config, self.model_name)
        browse_paths = BrowsePathsClass(paths=[self.get_explore_browse_path(config)])
        container = ContainerClass(container=model_key.as_urn())
        dataset_snapshot.aspects.append(browse_paths)
        dataset_snapshot.aspects.append(StatusClass(removed=False))

        custom_properties = {}
        if self.label is not None:
            custom_properties["looker.explore.label"] = str(self.label)
        if self.source_file is not None:
            custom_properties["looker.explore.file"] = str(self.source_file)
        dataset_props = DatasetPropertiesClass(
            name=str(self.label) if self.label else LookerUtil._display_name(self.name),
            description=self.description,
            customProperties=custom_properties,
        )
        dataset_props.externalUrl = self._get_url(base_url)

        dataset_snapshot.aspects.append(dataset_props)
        view_name_to_urn_map: Dict[str, str] = {}
        if self.upstream_views is not None:
            assert self.project_name is not None
            upstreams = []
            observed_lineage_ts = datetime.datetime.now(tz=datetime.timezone.utc)
            for view_ref in sorted(self.upstream_views):
                # set file_path to ViewFieldType.UNKNOWN if file_path is not available to keep backward compatibility
                # if we raise error on file_path equal to None then existing test-cases will fail as mock data doesn't have required attributes.
                file_path: str = (
                    cast(str, self.upstream_views_file_path[view_ref.include])
                    if self.upstream_views_file_path[view_ref.include] is not None
                    else ViewFieldValue.NOT_AVAILABLE.value
                )
                view_urn = LookerViewId(
                    project_name=(
                        view_ref.project
                        if view_ref.project != _BASE_PROJECT_NAME
                        else self.project_name
                    ),
                    model_name=self.model_name,
                    view_name=view_ref.include,
                    file_path=file_path,
                ).get_urn(config)

                upstreams.append(
                    UpstreamClass(
                        dataset=view_urn,
                        type=DatasetLineageTypeClass.VIEW,
                        auditStamp=AuditStamp(
                            time=int(observed_lineage_ts.timestamp() * 1000),
                            actor=CORPUSER_DATAHUB,
                        ),
                    )
                )
                view_name_to_urn_map[view_ref.include] = view_urn

            fine_grained_lineages = []
            if config.extract_column_level_lineage:
                for field in self.fields or []:
                    for upstream_field in field.upstream_fields:
                        if len(upstream_field.split(".")) >= 2:
                            (view_name, field_path) = upstream_field.split(".")[
                                0
                            ], ".".join(upstream_field.split(".")[1:])
                            assert view_name
                            view_urn = view_name_to_urn_map.get(view_name, "")
                            if view_urn:
                                fine_grained_lineages.append(
                                    FineGrainedLineageClass(
                                        upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                                        downstreamType=FineGrainedLineageDownstreamType.FIELD,
                                        upstreams=[
                                            builder.make_schema_field_urn(
                                                view_urn, field_path
                                            )
                                        ],
                                        downstreams=[
                                            builder.make_schema_field_urn(
                                                self.get_explore_urn(config), field.name
                                            )
                                        ],
                                    )
                                )

            upstream_lineage = UpstreamLineage(
                upstreams=upstreams, fineGrainedLineages=fine_grained_lineages or None
            )
            dataset_snapshot.aspects.append(upstream_lineage)
        if self.fields is not None:
            schema_metadata = LookerUtil._get_schema(
                platform_name=config.platform_name,
                schema_name=self.name,
                view_fields=self.fields,
                reporter=reporter,
            )
            if schema_metadata is not None:
                dataset_snapshot.aspects.append(schema_metadata)

        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        mcp = MetadataChangeProposalWrapper(
            entityUrn=dataset_snapshot.urn,
            aspect=SubTypesClass(typeNames=[DatasetSubTypes.LOOKER_EXPLORE]),
        )

        proposals: List[Union[MetadataChangeEvent, MetadataChangeProposalWrapper]] = [
            mce,
            mcp,
        ]

        # Add tags
        explore_tag_urns: List[TagAssociationClass] = [
            TagAssociationClass(tag=TagUrn(tag).urn()) for tag in self.tags
        ]
        if explore_tag_urns:
            dataset_snapshot.aspects.append(GlobalTagsClass(explore_tag_urns))

        # If extracting embeds is enabled, produce an MCP for embed URL.
        if extract_embed_urls:
            embed_mcp = create_embed_mcp(
                dataset_snapshot.urn, self._get_embed_url(base_url)
            )
            proposals.append(embed_mcp)

        proposals.append(
            MetadataChangeProposalWrapper(
                entityUrn=dataset_snapshot.urn,
                aspect=container,
            )
        )

        return proposals


def gen_project_key(config: LookerCommonConfig, project_name: str) -> LookMLProjectKey:
    return LookMLProjectKey(
        platform=config.platform_name,
        instance=config.platform_instance,
        env=config.env,
        project_name=project_name,
    )


def gen_model_key(config: LookerCommonConfig, model_name: str) -> LookMLModelKey:
    return LookMLModelKey(
        platform=config.platform_name,
        instance=config.platform_instance,
        env=config.env,
        model_name=model_name,
    )


class LookerExploreRegistry:
    """A LRU caching registry of Looker Explores"""

    def __init__(
        self,
        looker_api: LookerAPI,
        report: SourceReport,
        source_config: LookerDashboardSourceConfig,
    ):
        self.client = looker_api
        self.report = report
        self.source_config = source_config

    @lru_cache(maxsize=200)
    def get_explore(self, model: str, explore: str) -> Optional[LookerExplore]:
        looker_explore = LookerExplore.from_api(
            model,
            explore,
            self.client,
            self.report,
            self.source_config,
        )
        return looker_explore

    def compute_stats(self) -> Dict:
        return {
            "cache_info": self.get_explore.cache_info(),
        }


class StageLatency(Report):
    name: str
    start_time: Optional[datetime.datetime]
    end_time: Optional[datetime.datetime] = None

    def __init__(self, name: str, start_time: datetime.datetime):
        self.name = name
        self.start_time = start_time

    def compute_stats(self) -> None:
        if self.end_time and self.start_time:
            self.latency = self.end_time - self.start_time
            # clear out start and end times to keep logs clean
            self.start_time = None
            self.end_time = None


@dataclass
class LookerDashboardSourceReport(StaleEntityRemovalSourceReport):
    total_dashboards: int = 0
    dashboards_scanned: int = 0
    looks_scanned: int = 0
    filtered_dashboards: LossyList[str] = dataclasses_field(default_factory=LossyList)
    filtered_looks: LossyList[str] = dataclasses_field(default_factory=LossyList)
    dashboards_scanned_for_usage: int = 0
    charts_scanned_for_usage: int = 0
    charts_with_activity: LossySet[str] = dataclasses_field(default_factory=LossySet)
    accessed_dashboards: int = 0
    dashboards_with_activity: LossySet[str] = dataclasses_field(
        default_factory=LossySet
    )
    stage_latency: List[StageLatency] = dataclasses_field(default_factory=list)
    _looker_explore_registry: Optional[LookerExploreRegistry] = None
    total_explores: int = 0
    explores_scanned: int = 0

    resolved_user_ids: int = 0
    email_ids_missing: int = 0  # resolved users with missing email addresses

    _looker_api: Optional[LookerAPI] = None
    query_latency: Dict[str, datetime.timedelta] = dataclasses_field(
        default_factory=dict
    )
    user_resolution_latency: Dict[str, datetime.timedelta] = dataclasses_field(
        default_factory=dict
    )

    def report_total_dashboards(self, total_dashboards: int) -> None:
        self.total_dashboards = total_dashboards

    def report_dashboards_scanned(self) -> None:
        self.dashboards_scanned += 1

    def report_charts_scanned(self) -> None:
        self.looks_scanned += 1

    def report_dashboards_dropped(self, model: str) -> None:
        self.filtered_dashboards.append(model)

    def report_charts_dropped(self, view: str) -> None:
        self.filtered_looks.append(view)

    def report_dashboards_scanned_for_usage(self, num_dashboards: int) -> None:
        self.dashboards_scanned_for_usage += num_dashboards

    def report_charts_scanned_for_usage(self, num_charts: int) -> None:
        self.charts_scanned_for_usage += num_charts

    def report_upstream_latency(
        self, start_time: datetime.datetime, end_time: datetime.datetime
    ) -> None:
        # recording total combined latency is not very useful, keeping this method as a placeholder
        # for future implementation of min / max / percentiles etc.
        pass

    def report_query_latency(
        self, query_type: str, latency: datetime.timedelta
    ) -> None:
        self.query_latency[query_type] = latency

    def report_user_resolution_latency(
        self, generator_type: str, latency: datetime.timedelta
    ) -> None:
        self.user_resolution_latency[generator_type] = latency

    def report_stage_start(self, stage_name: str) -> None:
        self.stage_latency.append(
            StageLatency(name=stage_name, start_time=datetime.datetime.now())
        )

    def report_stage_end(self, stage_name: str) -> None:
        if self.stage_latency[-1].name == stage_name:
            self.stage_latency[-1].end_time = datetime.datetime.now()

    @contextmanager
    def report_stage(self, stage_name: str) -> Iterator[None]:
        try:
            self.report_stage_start(stage_name)
            yield
        finally:
            self.report_stage_end(stage_name)

    def compute_stats(self) -> None:
        if self.total_dashboards:
            self.dashboard_process_percentage_completion = round(
                100 * self.dashboards_scanned / self.total_dashboards, 2
            )
        if self._looker_explore_registry:
            self.explore_registry_stats = self._looker_explore_registry.compute_stats()

        if self.total_explores:
            self.explores_process_percentage_completion = round(
                100 * self.explores_scanned / self.total_explores, 2
            )

        if self._looker_api:
            self.looker_api_stats = self._looker_api.compute_stats()
        return super().compute_stats()


@dataclass
class LookerUser:
    id: str
    email: Optional[str]
    display_name: Optional[str]
    first_name: Optional[str]
    last_name: Optional[str]

    @classmethod
    def create_looker_user(cls, raw_user: User) -> "LookerUser":
        assert raw_user.id is not None
        return LookerUser(
            raw_user.id,
            raw_user.email,
            raw_user.display_name,
            raw_user.first_name,
            raw_user.last_name,
        )

    def get_urn(self, strip_user_ids_from_email: bool) -> Optional[str]:
        user = self.email
        if user and strip_user_ids_from_email:
            user = user.split("@")[0]

        if not user:
            return None
        return builder.make_user_urn(user)


@dataclass
class InputFieldElement:
    name: str
    view_field: Optional[ViewField]
    model: str = ""
    explore: str = ""


@dataclass
class LookerDashboardElement:
    id: str
    title: str
    query_slug: str
    upstream_explores: List[LookerExplore]
    look_id: Optional[str]
    type: Optional[str] = None
    description: Optional[str] = None
    input_fields: Optional[List[InputFieldElement]] = None
    folder_path: Optional[str] = None  # for independent looks.
    folder: Optional[LookerFolder] = None
    owner: Optional[LookerUser] = None

    def url(self, base_url: str) -> str:
        # A dashboard element can use a look or just a raw query against an explore
        base_url = remove_port_from_url(base_url)
        if self.look_id is not None:
            return f"{base_url}/looks/{self.look_id}"
        else:
            return f"{base_url}/x/{self.query_slug}"

    def embed_url(self, base_url: str) -> Optional[str]:
        # A dashboard element can use a look or just a raw query against an explore
        base_url = remove_port_from_url(base_url)
        if self.look_id is not None:
            return f"{base_url}/embed/looks/{self.look_id}"
        else:
            # No embeddable URL
            return None

    def get_urn_element_id(self):
        # A dashboard element can use a look or just a raw query against an explore
        return f"dashboard_elements.{self.id}"

    def get_view_urns(self, config: LookerCommonConfig) -> List[str]:
        return [v.get_explore_urn(config) for v in self.upstream_explores]


# These function will avoid to create LookerDashboard object to get the Looker Dashboard urn id part
def get_urn_looker_dashboard_id(id_: str) -> str:
    return f"dashboards.{id_}"


def get_urn_looker_element_id(id_: str) -> str:
    return f"dashboard_elements.{id_}"


@dataclass
class LookerDashboard:
    id: str
    title: str
    dashboard_elements: List[LookerDashboardElement]
    created_at: Optional[datetime.datetime]
    description: Optional[str] = None
    folder_path: Optional[str] = None
    folder: Optional[LookerFolder] = None
    is_deleted: bool = False
    is_hidden: bool = False
    owner: Optional[LookerUser] = None
    strip_user_ids_from_email: Optional[bool] = True
    last_updated_at: Optional[datetime.datetime] = None
    last_updated_by: Optional[LookerUser] = None
    deleted_at: Optional[datetime.datetime] = None
    deleted_by: Optional[LookerUser] = None
    favorite_count: Optional[int] = None
    view_count: Optional[int] = None
    last_viewed_at: Optional[datetime.datetime] = None

    def url(self, base_url):
        base_url = remove_port_from_url(base_url)
        return f"{base_url}/dashboards/{self.id}"

    def embed_url(self, base_url: str) -> str:
        base_url = remove_port_from_url(base_url)
        return f"{base_url}/embed/dashboards/{self.id}"

    def get_urn_dashboard_id(self):
        return get_urn_looker_dashboard_id(self.id)


class LookerUserRegistry:
    looker_api_wrapper: LookerAPI
    fields: str = ",".join(["id", "email", "display_name", "first_name", "last_name"])

    def __init__(self, looker_api: LookerAPI):
        self.looker_api_wrapper = looker_api

    def get_by_id(self, id_: str) -> Optional[LookerUser]:
        if not id_:
            return None

        logger.debug(f"Will get user {id_}")

        raw_user: Optional[User] = self.looker_api_wrapper.get_user(
            str(id_), user_fields=self.fields
        )
        if raw_user is None:
            return None

        looker_user = LookerUser.create_looker_user(raw_user)
        return looker_user
