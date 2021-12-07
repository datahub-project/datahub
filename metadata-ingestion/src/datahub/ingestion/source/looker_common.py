import logging
import re
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Iterable, List, Optional, Tuple, Union

from looker_sdk.error import SDKError
from looker_sdk.sdk.api31.methods import Looker31SDK
from pydantic.class_validators import validator

import datahub.emitter.mce_builder as builder
from datahub.configuration import ConfigModel
from datahub.configuration.common import ConfigurationError
from datahub.configuration.github import GitHubInfo
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.sql.sql_types import (
    POSTGRES_TYPES_MAP,
    SNOWFLAKE_TYPES_MAP,
    resolve_postgres_modified_type,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageTypeClass,
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

logger = logging.getLogger(__name__)


@dataclass
class NamingPattern:
    allowed_vars: List[str]
    pattern: str
    variables: Optional[List[str]] = None

    def validate(self, at_least_one: bool) -> bool:
        variables = re.findall("({[^}{]+})", self.pattern)
        self.variables = [v[1:-1] for v in variables]
        for v in variables:
            if v[1:-1] not in self.allowed_vars:
                raise ConfigurationError(
                    f"Failed to find {v} in allowed_variables {self.allowed_vars}"
                )
        if at_least_one and len(variables) == 0:
            raise ConfigurationError(
                f"Failed to find any variable assigned to pattern {self.pattern}. Must have at least one. Allowed variables are {self.allowed_vars}"
            )
        return True


naming_pattern_variables: List[str] = ["platform", "env", "project", "model", "name"]


class LookerExploreNamingConfig(ConfigModel):
    explore_naming_pattern: NamingPattern = NamingPattern(
        allowed_vars=naming_pattern_variables, pattern="{model}.explore.{name}"
    )
    explore_browse_pattern: NamingPattern = NamingPattern(
        allowed_vars=naming_pattern_variables,
        pattern="/{env}/{platform}/{project}/explores/{model}.{name}",
    )

    @validator("explore_naming_pattern", "explore_browse_pattern", pre=True)
    def init_naming_pattern(cls, v):
        if isinstance(v, NamingPattern):
            return v
        else:
            assert isinstance(v, str), "pattern must be a string"
            naming_pattern = NamingPattern(
                allowed_vars=naming_pattern_variables, pattern=v
            )
            return naming_pattern

    @validator("explore_naming_pattern", "explore_browse_pattern", always=True)
    def validate_naming_pattern(cls, v):
        assert isinstance(v, NamingPattern)
        v.validate(at_least_one=True)
        return v


class LookerViewNamingConfig(ConfigModel):
    view_naming_pattern: NamingPattern = NamingPattern(
        allowed_vars=naming_pattern_variables, pattern="{project}.view.{name}"
    )
    view_browse_pattern: NamingPattern = NamingPattern(
        allowed_vars=naming_pattern_variables,
        pattern="/{env}/{platform}/{project}/views/{name}",
    )

    @validator("view_naming_pattern", "view_browse_pattern", pre=True)
    def init_naming_pattern(cls, v):
        if isinstance(v, NamingPattern):
            return v
        else:
            assert isinstance(v, str), "pattern must be a string"
            naming_pattern = NamingPattern(
                allowed_vars=naming_pattern_variables, pattern=v
            )
            return naming_pattern

    @validator("view_naming_pattern", "view_browse_pattern", always=True)
    def validate_naming_pattern(cls, v):
        assert isinstance(v, NamingPattern)
        v.validate(at_least_one=True)
        return v


class LookerCommonConfig(LookerViewNamingConfig, LookerExploreNamingConfig):
    tag_measures_and_dimensions: bool = True
    platform_name: str = "looker"
    env: str = builder.DEFAULT_ENV
    github_info: Optional[GitHubInfo] = None


@dataclass
class LookerViewId:
    project_name: str
    model_name: str
    view_name: str

    def get_mapping(self, variable: str, config: LookerCommonConfig) -> str:
        assert variable in naming_pattern_variables
        if variable == "project":
            return self.project_name
        if variable == "model":
            return self.model_name
        if variable == "name":
            return self.view_name
        if variable == "env":
            return config.env.lower()
        if variable == "platform":
            return config.platform_name
        assert False, "Unreachable code"

    @validator("view_name")
    def remove_quotes(cls, v):
        # Sanitize the name.
        v = v.replace('"', "").replace("`", "")
        return v

    def get_urn(self, config: LookerCommonConfig) -> str:
        dataset_name = config.view_naming_pattern.pattern
        assert config.view_naming_pattern.variables is not None
        for v in config.view_naming_pattern.variables:
            dataset_name = dataset_name.replace(
                "{" + v + "}", self.get_mapping(v, config)
            )

        return builder.make_dataset_urn(config.platform_name, dataset_name, config.env)

    def get_browse_path(self, config: LookerCommonConfig) -> str:
        browse_path = config.view_browse_pattern.pattern
        assert config.view_browse_pattern.variables is not None
        for v in config.view_browse_pattern.variables:
            browse_path = browse_path.replace(
                "{" + v + "}", self.get_mapping(v, config)
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
    type: str
    description: str
    field_type: ViewFieldType
    is_primary_key: bool = False


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
        view_name = field.split(".")[0]
        return view_name

    @staticmethod
    def _get_field_type(
        native_type: str, reporter: SourceReport
    ) -> SchemaFieldDataType:

        type_class = LookerUtil.field_type_mapping.get(native_type)

        if type_class is None:

            # attempt Postgres modified type
            type_class = resolve_postgres_modified_type(native_type)

        # if still not found, report a warning
        if type_class is None:
            reporter.report_warning(
                native_type,
                f"The type '{native_type}' is not recognized for field type, setting as NullTypeClass.",
            )
            type_class = NullTypeClass

        data_type = SchemaFieldDataType(type=type_class())
        return data_type

    @staticmethod
    def _get_schema(
        platform_name: str,
        schema_name: str,
        view_fields: List[ViewField],
        reporter: SourceReport,
    ) -> Optional[SchemaMetadataClass]:
        if view_fields == []:
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
    def _get_fields_and_primary_keys(
        view_fields: List[ViewField],
        reporter: SourceReport,
        tag_measures_and_dimensions: bool = True,
    ) -> Tuple[List[SchemaField], List[str]]:
        primary_keys: List = []
        fields = []
        for field in view_fields:
            schema_field = SchemaField(
                fieldPath=field.name,
                type=LookerUtil._get_field_type(field.type, reporter),
                nativeDataType=field.type,
                description=f"{field.description}"
                if tag_measures_and_dimensions is True
                else f"{field.field_type.value}. {field.description}",
                globalTags=LookerUtil._get_tags_from_field_type(
                    field.field_type, reporter
                )
                if tag_measures_and_dimensions is True
                else None,
                isPartOfKey=field.is_primary_key,
            )
            fields.append(schema_field)
            if field.is_primary_key:
                primary_keys.append(schema_field.fieldPath)
        return fields, primary_keys


@dataclass
class LookerExplore:
    name: str
    model_name: str
    project_name: Optional[str] = None
    label: Optional[str] = None
    description: Optional[str] = None
    upstream_views: Optional[
        List[str]
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
    def __from_dict(cls, model_name: str, dict: Dict) -> "LookerExplore":
        if dict.get("joins", {}) != {}:
            assert "joins" in dict
            view_names = set()
            for join in dict["joins"]:
                sql_on = join.get("sql_on", None)
                if sql_on is not None:
                    fields = cls._get_fields_from_sql_equality(sql_on)
                    joins = fields
                    for f in fields:
                        view_names.add(LookerUtil._extract_view_from_field(f))
        else:
            # non-join explore, get view_name from `from` field if possible, default to explore name
            view_names = set(dict.get("from", dict.get("name")))
        return LookerExplore(
            model_name=model_name,
            name=dict["name"],
            label=dict.get("label"),
            description=dict.get("description"),
            upstream_views=list(view_names),
            joins=joins,
        )

    @classmethod  # noqa: C901
    def from_api(  # noqa: C901
        cls,
        model: str,
        explore_name: str,
        client: Looker31SDK,
        reporter: SourceReport,
    ) -> Optional["LookerExplore"]:  # noqa: C901
        try:
            explore = client.lookml_model_explore(model, explore_name)
            views = set()
            if explore.joins is not None and explore.joins != []:
                if explore.view_name is not None and explore.view_name != explore.name:
                    # explore is renaming the view name, we will need to swap references to explore.name with explore.view_name
                    aliased_explore = True
                    views.add(explore.view_name)
                else:
                    aliased_explore = False

                for e_join in [
                    e for e in explore.joins if e.dependent_fields is not None
                ]:
                    assert e_join.dependent_fields is not None
                    for field_name in e_join.dependent_fields:
                        try:
                            view_name = LookerUtil._extract_view_from_field(field_name)
                            if (view_name == explore.name) and aliased_explore:
                                assert explore.view_name is not None
                                view_name = explore.view_name
                            views.add(view_name)
                        except AssertionError:
                            reporter.report_warning(
                                key=f"chart-field-{field_name}",
                                reason="The field was not prefixed by a view name. This can happen when the field references another dynamic field.",
                            )
                            continue
            else:
                assert explore.view_name is not None
                views.add(explore.view_name)

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
                                )
                            )

            return cls(
                name=explore_name,
                model_name=model,
                project_name=explore.project_name,
                label=explore.label,
                description=explore.description,
                fields=view_fields,
                upstream_views=list(views),
                source_file=explore.source_file,
            )
        except SDKError as e:
            logger.warn(
                "Failed to extract explore {} from model {}.".format(
                    explore_name, model
                )
            )
            logger.debug(
                "Failed to extract explore {} from model {} with {}".format(
                    explore_name, model, e
                )
            )
        except AssertionError:
            reporter.report_warning(
                key="chart-",
                reason="Was unable to find dependent views for this chart",
            )
        return None

    def get_mapping(self, variable: str, config: LookerCommonConfig) -> str:
        assert variable in naming_pattern_variables
        if variable == "project":
            assert self.project_name is not None
            return self.project_name
        if variable == "model":
            return self.model_name
        if variable == "name":
            return self.name
        if variable == "env":
            return config.env.lower()
        if variable == "platform":
            return config.platform_name
        assert False, "Unreachable code"

    def get_explore_urn(self, config: LookerCommonConfig) -> str:
        dataset_name = config.explore_naming_pattern.pattern
        assert config.explore_naming_pattern.variables is not None
        for v in config.explore_naming_pattern.variables:
            dataset_name = dataset_name.replace(
                "{" + v + "}", self.get_mapping(v, config)
            )

        return builder.make_dataset_urn(config.platform_name, dataset_name, config.env)

    def get_explore_browse_path(self, config: LookerCommonConfig) -> str:
        browse_path = config.explore_browse_pattern.pattern
        assert config.explore_browse_pattern.variables is not None
        for v in config.explore_browse_pattern.variables:
            browse_path = browse_path.replace(
                "{" + v + "}", self.get_mapping(v, config)
            )
        return browse_path

    def _get_url(self, base_url):
        # If the base_url contains a port number (like https://company.looker.com:19999) remove the port number
        m = re.match("^(.*):([0-9]+)$", base_url)
        if m is not None:
            base_url = m.group(1)
        return f"{base_url}/explore/{self.model_name}/{self.name}"

    def _to_metadata_events(  # noqa: C901
        self, config: LookerCommonConfig, reporter: SourceReport, base_url: str
    ) -> Optional[List[Union[MetadataChangeEvent, MetadataChangeProposalWrapper]]]:
        # We only generate MCE-s for explores that contain from clauses and do NOT contain joins
        # All other explores (passthrough explores and joins) end in correct resolution of lineage, and don't need additional nodes in the graph.

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
            description=self.description,
            customProperties=custom_properties,
        )
        dataset_props.externalUrl = self._get_url(base_url)

        dataset_snapshot.aspects.append(dataset_props)
        if self.upstream_views is not None:
            assert self.project_name is not None
            upstreams = [
                UpstreamClass(
                    dataset=LookerViewId(
                        project_name=self.project_name,
                        model_name=self.model_name,
                        view_name=view_name,
                    ).get_urn(config),
                    type=DatasetLineageTypeClass.VIEW,
                )
                for view_name in sorted(self.upstream_views)
            ]
            upstream_lineage = UpstreamLineage(upstreams=upstreams)
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
