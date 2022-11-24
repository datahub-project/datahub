from __future__ import print_function

import dataclasses
import datetime
import logging
import re
from dataclasses import dataclass, field as dataclasses_field
from enum import Enum
from functools import lru_cache
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

import pydantic
from looker_sdk.error import SDKError
from looker_sdk.sdk.api31.models import User, WriteQuery
from pydantic import Field
from pydantic.class_validators import validator

import datahub.emitter.mce_builder as builder
from datahub.configuration import ConfigModel
from datahub.configuration.common import ConfigurationError
from datahub.configuration.github import GitHubInfo
from datahub.configuration.source_common import DatasetSourceConfigBase
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.report import Report
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.looker.looker_lib_wrapper import LookerAPI
from datahub.ingestion.source.sql.sql_types import (
    POSTGRES_TYPES_MAP,
    SNOWFLAKE_TYPES_MAP,
    resolve_postgres_modified_type,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
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
    BrowsePathsClass,
    ChangeTypeClass,
    DatasetPropertiesClass,
    EnumTypeClass,
    FineGrainedLineageClass,
    GlobalTagsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaMetadataClass,
    StatusClass,
    SubTypesClass,
    TagAssociationClass,
    TagPropertiesClass,
    TagSnapshotClass,
)
from datahub.utilities.lossy_collections import LossyList, LossySet

if TYPE_CHECKING:
    from datahub.ingestion.source.looker.lookml_source import (
        LookerViewFileLoader,
        LookMLSourceReport,
    )

logger = logging.getLogger(__name__)


class NamingPattern(ConfigModel):
    ALLOWED_VARS: ClassVar[List[str]] = []
    REQUIRE_AT_LEAST_ONE_VAR: ClassVar[bool] = True

    pattern: str

    @classmethod
    def __get_validators__(cls):
        yield cls.pydantic_accept_raw_pattern
        yield cls.validate
        yield cls.pydantic_validate_pattern

    @classmethod
    def pydantic_accept_raw_pattern(cls, v):
        if isinstance(v, (NamingPattern, dict)):
            return v
        assert isinstance(v, str), "pattern must be a string"
        return {"pattern": v}

    @classmethod
    def pydantic_validate_pattern(cls, v):
        assert isinstance(v, NamingPattern)
        assert v.validate_pattern(cls.REQUIRE_AT_LEAST_ONE_VAR)
        return v

    @classmethod
    def allowed_docstring(cls) -> str:
        return f"Allowed variables are {cls.ALLOWED_VARS}"

    def validate_pattern(self, at_least_one: bool) -> bool:
        variables = re.findall("({[^}{]+})", self.pattern)

        variables = [v[1:-1] for v in variables]  # remove the {}

        for v in variables:
            if v not in self.ALLOWED_VARS:
                raise ConfigurationError(
                    f"Failed to find {v} in allowed_variables {self.ALLOWED_VARS}"
                )
        if at_least_one and len(variables) == 0:
            raise ConfigurationError(
                f"Failed to find any variable assigned to pattern {self.pattern}. Must have at least one. {self.allowed_docstring()}"
            )
        return True

    def replace_variables(self, values: Union[Dict[str, Optional[str]], object]) -> str:
        if not isinstance(values, dict):
            assert dataclasses.is_dataclass(values)
            values = dataclasses.asdict(values)
        values = {k: v for k, v in values.items() if v is not None}
        return self.pattern.format(**values)


@dataclass
class NamingPatternMapping:
    platform: str
    env: str
    project: str
    model: str
    name: str


class LookerNamingPattern(NamingPattern):
    ALLOWED_VARS = [field.name for field in dataclasses.fields(NamingPatternMapping)]


class LookerCommonConfig(DatasetSourceConfigBase):
    explore_naming_pattern: LookerNamingPattern = pydantic.Field(
        description=f"Pattern for providing dataset names to explores. {LookerNamingPattern.allowed_docstring()}",
        default=LookerNamingPattern(pattern="{model}.explore.{name}"),
    )

    explore_browse_pattern: LookerNamingPattern = pydantic.Field(
        description=f"Pattern for providing browse paths to explores. {LookerNamingPattern.allowed_docstring()}",
        default=LookerNamingPattern(pattern="/{env}/{platform}/{project}/explores"),
    )

    view_naming_pattern: LookerNamingPattern = Field(
        LookerNamingPattern(pattern="{project}.view.{name}"),
        description=f"Pattern for providing dataset names to views. {LookerNamingPattern.allowed_docstring()}",
    )
    view_browse_pattern: LookerNamingPattern = Field(
        LookerNamingPattern(pattern="/{env}/{platform}/{project}/views"),
        description=f"Pattern for providing browse paths to views. {LookerNamingPattern.allowed_docstring()}",
    )

    tag_measures_and_dimensions: bool = Field(
        True,
        description="When enabled, attaches tags to measures, dimensions and dimension groups to make them more discoverable. When disabled, adds this information to the description of the column.",
    )
    platform_name: str = Field(
        "looker", description="Default platform name. Don't change."
    )
    github_info: Optional[GitHubInfo] = Field(
        None,
        description="Reference to your github location. If present, supplies handy links to your lookml on the dataset entity page.",
    )
    extract_column_level_lineage: bool = Field(
        True,
        description="When enabled, extracts column-level lineage from Views and Explores",
    )


@dataclass
class LookerViewId:
    project_name: str
    model_name: str
    view_name: str

    def get_mapping(self, config: LookerCommonConfig) -> NamingPatternMapping:
        return NamingPatternMapping(
            platform=config.platform_name,
            env=config.env.lower(),
            project=self.project_name,
            model=self.model_name,
            name=self.view_name,
        )

    @validator("view_name")
    def remove_quotes(cls, v):
        # Sanitize the name.
        v = v.replace('"', "").replace("`", "")
        return v

    def get_urn(self, config: LookerCommonConfig) -> str:
        dataset_name = config.view_naming_pattern.replace_variables(
            self.get_mapping(config)
        )

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


class ViewFieldType(Enum):
    DIMENSION = "Dimension"
    DIMENSION_GROUP = "Dimension Group"
    MEASURE = "Measure"
    UNKNOWN = "Unknown"


@dataclass
class ViewField:
    name: str
    label: Optional[str]
    type: str
    description: str
    field_type: ViewFieldType
    is_primary_key: bool = False
    upstream_fields: List[str] = dataclasses_field(default_factory=list)


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
    def _get_field_type(
        native_type: str, reporter: SourceReport
    ) -> SchemaFieldDataType:

        type_class = LookerUtil.field_type_mapping.get(native_type)

        if type_class is None:

            # attempt Postgres modified type
            type_class = resolve_postgres_modified_type(native_type)

        # if still not found, log and continue
        if type_class is None:
            logger.info(
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
        ownership = OwnershipClass(
            owners=[
                OwnerClass(
                    owner="urn:li:corpuser:datahub",
                    type=OwnershipTypeClass.DATAOWNER,
                )
            ]
        )
        return MetadataChangeEvent(
            proposedSnapshot=TagSnapshotClass(
                urn=tag_urn, aspects=[ownership, LookerUtil.tag_definitions[tag_urn]]
            )
        )

    @staticmethod
    def _get_tags_from_field_type(
        field_type: ViewFieldType, reporter: SourceReport
    ) -> Optional[GlobalTagsClass]:
        if field_type in LookerUtil.type_to_tag_map:
            return GlobalTagsClass(
                tags=[
                    TagAssociationClass(tag=tag_name)
                    for tag_name in LookerUtil.type_to_tag_map[field_type]
                ]
            )
        else:
            reporter.report_warning(
                "lookml",
                f"Failed to map view field type {field_type}. Won't emit tags for it",
            )
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
            description=field.description
            if tag_measures_and_dimensions is True
            else f"{field.field_type.value}. {field.description}",
            globalTags=LookerUtil._get_tags_from_field_type(field.field_type, reporter)
            if tag_measures_and_dimensions is True
            else None,
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
    joins: Optional[List[str]] = None
    fields: Optional[List[ViewField]] = None  # the fields exposed in this explore
    source_file: Optional[str] = None

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
    ) -> "LookerExplore":
        view_names = set()
        joins = None
        # always add the explore's name or the name from the from clause as the view on which this explore is built
        view_names.add(dict.get("from", dict.get("name")))

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

        upstream_views = []
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
        )

    @classmethod  # noqa: C901
    def from_api(  # noqa: C901
        cls,
        model: str,
        explore_name: str,
        client: LookerAPI,
        reporter: SourceReport,
    ) -> Optional["LookerExplore"]:  # noqa: C901
        from datahub.ingestion.source.looker.lookml_source import _BASE_PROJECT_NAME

        try:
            explore = client.lookml_model_explore(model, explore_name)
            views: Set[str] = set()

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
                                key=f"chart-field-{field_name}",
                                reason="The field was not prefixed by a view name. This can happen when the field references another dynamic field.",
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
                            view_fields.append(
                                ViewField(
                                    name=dim_field.name,
                                    label=dim_field.label_short,
                                    description=dim_field.description
                                    if dim_field.description
                                    else "",
                                    type=dim_field.type
                                    if dim_field.type is not None
                                    else "",
                                    field_type=ViewFieldType.DIMENSION_GROUP
                                    if dim_field.dimension_group is not None
                                    else ViewFieldType.DIMENSION,
                                    is_primary_key=dim_field.primary_key
                                    if dim_field.primary_key
                                    else False,
                                    upstream_fields=[dim_field.name],
                                )
                            )
                if explore.fields.measures is not None:
                    for measure_field in explore.fields.measures:
                        if measure_field.name is None:
                            continue
                        else:
                            view_fields.append(
                                ViewField(
                                    name=measure_field.name,
                                    label=measure_field.label_short,
                                    description=measure_field.description
                                    if measure_field.description
                                    else "",
                                    type=measure_field.type
                                    if measure_field.type is not None
                                    else "",
                                    field_type=ViewFieldType.MEASURE,
                                    is_primary_key=measure_field.primary_key
                                    if measure_field.primary_key
                                    else False,
                                    upstream_fields=[measure_field.name],
                                )
                            )

            return cls(
                name=explore_name,
                model_name=model,
                project_name=explore.project_name,
                label=explore.label,
                description=explore.description,
                fields=view_fields,
                upstream_views=list(
                    ProjectInclude(
                        project=_BASE_PROJECT_NAME,
                        include=view_name,
                    )
                    for view_name in views
                ),
                source_file=explore.source_file,
            )
        except SDKError as e:
            if "<title>Looker Not Found (404)</title>" in str(e):
                logger.info(
                    f"Explore {explore_name} in model {model} is referred to, but missing. Continuing..."
                )
            else:
                logger.warning(
                    f"Failed to extract explore {explore_name} from model {model}.", e
                )

        except AssertionError:
            reporter.report_warning(
                key="chart-",
                reason="Was unable to find dependent views for this chart",
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
        # If the base_url contains a port number (like https://company.looker.com:19999) remove the port number
        m = re.match("^(.*):([0-9]+)$", base_url)
        if m is not None:
            base_url = m[1]
        return f"{base_url}/explore/{self.model_name}/{self.name}"

    def _to_metadata_events(  # noqa: C901
        self, config: LookerCommonConfig, reporter: SourceReport, base_url: str
    ) -> Optional[List[Union[MetadataChangeEvent, MetadataChangeProposalWrapper]]]:
        # We only generate MCE-s for explores that contain from clauses and do NOT contain joins
        # All other explores (passthrough explores and joins) end in correct resolution of lineage, and don't need additional nodes in the graph.
        from datahub.ingestion.source.looker.lookml_source import _BASE_PROJECT_NAME

        dataset_snapshot = DatasetSnapshot(
            urn=self.get_explore_urn(config),
            aspects=[],  # we append to this list later on
        )
        browse_paths = BrowsePathsClass(paths=[self.get_explore_browse_path(config)])
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
            for view_ref in sorted(self.upstream_views):
                view_urn = LookerViewId(
                    project_name=view_ref.project
                    if view_ref.project != _BASE_PROJECT_NAME
                    else self.project_name,
                    model_name=self.model_name,
                    view_name=view_ref.include,
                ).get_urn(config)

                upstreams.append(
                    UpstreamClass(
                        dataset=view_urn,
                        type=DatasetLineageTypeClass.VIEW,
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
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=dataset_snapshot.urn,
            aspectName="subTypes",
            aspect=SubTypesClass(typeNames=["explore"]),
        )

        return [mce, mcp]


class LookerExploreRegistry:
    """A LRU caching registry of Looker Explores"""

    def __init__(
        self,
        looker_api: LookerAPI,
        report: SourceReport,
    ):
        self.client = looker_api
        self.report = report

    @lru_cache()
    def get_explore(self, model: str, explore: str) -> Optional[LookerExplore]:
        looker_explore = LookerExplore.from_api(
            model,
            explore,
            self.client,
            self.report,
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
    dashboards_with_activity: LossySet[str] = dataclasses_field(
        default_factory=LossySet
    )
    stage_latency: List[StageLatency] = dataclasses_field(default_factory=list)
    _looker_explore_registry: Optional[LookerExploreRegistry] = None
    total_explores: int = 0
    explores_scanned: int = 0
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
    id: int
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
        if self.email is None:
            return None
        if strip_user_ids_from_email:
            return builder.make_user_urn(self.email.split("@")[0])
        else:
            return builder.make_user_urn(self.email)


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

    def url(self, base_url: str) -> str:
        # A dashboard element can use a look or just a raw query against an explore
        # If the base_url contains a port number (like https://company.looker.com:19999) remove the port number
        m = re.match("^(.*):([0-9]+)$", base_url)
        if m is not None:
            base_url = m[1]
        if self.look_id is not None:
            return f"{base_url}/looks/{self.look_id}"
        else:
            return f"{base_url}/x/{self.query_slug}"

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
        # If the base_url contains a port number (like https://company.looker.com:19999) remove the port number
        m = re.match("^(.*):([0-9]+)$", base_url)
        if m is not None:
            base_url = m[1]
        return f"{base_url}/dashboards/{self.id}"

    def get_urn_dashboard_id(self):
        return get_urn_looker_dashboard_id(self.id)


class LookerUserRegistry:
    looker_api_wrapper: LookerAPI
    fields: str = ",".join(["id", "email", "display_name", "first_name", "last_name"])

    def __init__(self, looker_api: LookerAPI):
        self.looker_api_wrapper = looker_api

    def get_by_id(self, id_: int) -> Optional[LookerUser]:
        logger.debug(f"Will get user {id_}")

        raw_user: Optional[User] = self.looker_api_wrapper.get_user(
            id_, user_fields=self.fields
        )
        if raw_user is None:
            return None

        looker_user = LookerUser.create_looker_user(raw_user)
        return looker_user
