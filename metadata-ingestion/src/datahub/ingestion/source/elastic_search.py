import json
import logging
import re
import time
from collections import defaultdict
from dataclasses import dataclass, field
from hashlib import md5
from typing import Any, Dict, Generator, Iterable, List, Optional, Tuple, Type

from elasticsearch import Elasticsearch
from pydantic import validator
from pydantic.fields import Field

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.configuration.validate_host_port import validate_host_port
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source_config.operation_config import (
    OperationConfig,
    is_profiling_enabled,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import StatusClass
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DataPlatformInstanceClass,
    DatasetProfileClass,
    DatasetPropertiesClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    OtherSchemaClass,
    RecordTypeClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    SubTypesClass,
)
from datahub.utilities.config_clean import remove_protocol
from datahub.utilities.urns.dataset_urn import DatasetUrn

logger = logging.getLogger(__name__)


class ElasticToSchemaFieldConverter:
    # FieldPath format version.
    version_string: str = "[version=2.0]"

    _field_type_to_schema_field_type: Dict[str, Type] = {
        # Bool
        "boolean": BooleanTypeClass,
        # Binary
        "binary": BytesTypeClass,
        # Numbers
        "byte": NumberTypeClass,
        "integer": NumberTypeClass,
        "long": NumberTypeClass,
        "short": NumberTypeClass,
        "double": NumberTypeClass,
        "float": NumberTypeClass,
        "half_float": NumberTypeClass,
        "scaled_float": NumberTypeClass,
        "unsigned_long": NumberTypeClass,
        "token_count": NumberTypeClass,
        # Dates
        "date": DateTypeClass,
        "date_nanos": DateTypeClass,
        # Strings
        "keyword": StringTypeClass,
        "constant_keyword": StringTypeClass,
        "wildcard": StringTypeClass,
        "text": StringTypeClass,
        "match_only_text": StringTypeClass,
        "completion": StringTypeClass,
        "search_as_you_type": StringTypeClass,
        "ip": StringTypeClass,
        # Records
        "object": RecordTypeClass,
        "flattened": RecordTypeClass,
        "nested": RecordTypeClass,
        "geo_point": RecordTypeClass,
        # Arrays
        "histogram": ArrayTypeClass,
        "aggregate_metric_double": ArrayTypeClass,
    }

    @staticmethod
    def get_column_type(elastic_column_type: str) -> SchemaFieldDataType:
        type_class: Optional[
            Type
        ] = ElasticToSchemaFieldConverter._field_type_to_schema_field_type.get(
            elastic_column_type
        )
        if type_class is None:
            logger.warning(
                f"Cannot map {elastic_column_type!r} to SchemaFieldDataType, using NullTypeClass."
            )
            type_class = NullTypeClass

        return SchemaFieldDataType(type=type_class())

    def __init__(self) -> None:
        self._prefix_name_stack: List[str] = [self.version_string]

    def _get_cur_field_path(self) -> str:
        return ".".join(self._prefix_name_stack)

    def _get_schema_fields(
        self, elastic_schema_dict: Dict[str, Any]
    ) -> Generator[SchemaField, None, None]:
        # append each schema field (sort so output is consistent)
        PROPERTIES: str = "properties"
        for columnName, column in elastic_schema_dict.items():
            elastic_type: Optional[str] = column.get("type")
            nested_props: Optional[Dict[str, Any]] = column.get(PROPERTIES)
            if elastic_type is not None:
                self._prefix_name_stack.append(f"[type={elastic_type}].{columnName}")
                schema_field_data_type = self.get_column_type(elastic_type)
                schema_field = SchemaField(
                    fieldPath=self._get_cur_field_path(),
                    nativeDataType=elastic_type,
                    type=schema_field_data_type,
                    description=None,
                    nullable=True,
                    recursive=False,
                )
                yield schema_field
                self._prefix_name_stack.pop()
            elif nested_props:
                self._prefix_name_stack.append(f"[type={PROPERTIES}].{columnName}")
                schema_field = SchemaField(
                    fieldPath=self._get_cur_field_path(),
                    nativeDataType=PROPERTIES,
                    type=SchemaFieldDataTypeClass(RecordTypeClass()),
                    description=None,
                    nullable=True,
                    recursive=False,
                )
                yield schema_field
                yield from self._get_schema_fields(nested_props)
                self._prefix_name_stack.pop()
            else:
                # Unexpected! Log a warning.
                logger.warning(
                    f"Elastic schema does not have either 'type' or 'properties'!"
                    f" Schema={json.dumps(elastic_schema_dict)}"
                )
                continue

    @classmethod
    def get_schema_fields(
        cls, elastic_mappings: Dict[str, Any]
    ) -> Generator[SchemaField, None, None]:
        converter = cls()
        properties = elastic_mappings.get("properties")
        if not properties:
            logger.warning(
                f"Missing 'properties' in elastic search mappings={json.dumps(elastic_mappings)}!"
            )
            return
        yield from converter._get_schema_fields(properties)


@dataclass
class ElasticsearchSourceReport(SourceReport):
    index_scanned: int = 0
    filtered: List[str] = field(default_factory=list)

    def report_index_scanned(self, index: str) -> None:
        self.index_scanned += 1

    def report_dropped(self, index: str) -> None:
        self.filtered.append(index)


class ElasticProfiling(ConfigModel):
    enabled: bool = Field(
        default=False,
        description="Whether to enable profiling for the elastic search source.",
    )
    operation_config: OperationConfig = Field(
        default_factory=OperationConfig,
        description="Experimental feature. To specify operation configs.",
    )


class CollapseUrns(ConfigModel):
    urns_suffix_regex: List[str] = Field(
        default_factory=list,
        description="""List of regex patterns to remove from the name of the URN. All of the indices before removal of URNs are considered as the same dataset. These are applied in order for each URN.
        The main case where you would want to have multiple of these if the name where you are trying to remove suffix from have different formats.
        e.g. ending with -YYYY-MM-DD as well as ending -epochtime would require you to have 2 regex patterns to remove the suffixes across all URNs.""",
    )


def collapse_name(name: str, collapse_urns: CollapseUrns) -> str:
    for suffix in collapse_urns.urns_suffix_regex:
        name = re.sub(suffix, "", name)
    return name


def collapse_urn(urn: str, collapse_urns: CollapseUrns) -> str:
    if len(collapse_urns.urns_suffix_regex) == 0:
        return urn
    urn_obj = DatasetUrn.create_from_string(urn)
    name = collapse_name(name=urn_obj.get_dataset_name(), collapse_urns=collapse_urns)
    data_platform_urn = urn_obj.get_data_platform_urn()
    return str(
        DatasetUrn.create_from_ids(
            platform_id=data_platform_urn.get_entity_id_as_string(),
            table_name=name,
            env=urn_obj.get_env(),
        )
    )


class ElasticsearchSourceConfig(PlatformInstanceConfigMixin, EnvConfigMixin):
    host: str = Field(
        default="localhost:9200", description="The elastic search host URI."
    )
    username: Optional[str] = Field(
        default=None, description="The username credential."
    )
    password: Optional[str] = Field(
        default=None, description="The password credential."
    )

    use_ssl: bool = Field(
        default=False, description="Whether to use SSL for the connection or not."
    )

    verify_certs: bool = Field(
        default=False, description="Whether to verify SSL certificates."
    )

    ca_certs: Optional[str] = Field(
        default=None, description="Path to a certificate authority (CA) certificate."
    )

    client_cert: Optional[str] = Field(
        default=None,
        description="Path to the file containing the private key and the certificate, or cert only if using client_key.",
    )

    client_key: Optional[str] = Field(
        default=None,
        description="Path to the file containing the private key if using separate cert and key files.",
    )

    ssl_assert_hostname: bool = Field(
        default=False, description="Use hostname verification if not False."
    )

    ssl_assert_fingerprint: Optional[str] = Field(
        default=None,
        description="Verify the supplied certificate fingerprint if not None.",
    )

    url_prefix: str = Field(
        default="",
        description="There are cases where an enterprise would have multiple elastic search clusters. One way for them to manage is to have a single endpoint for all the elastic search clusters and use url_prefix for routing requests to different clusters.",
    )
    index_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern(allow=[".*"], deny=["^_.*", "^ilm-history.*"]),
        description="regex patterns for indexes to filter in ingestion.",
    )
    ingest_index_templates: bool = Field(
        default=False, description="Ingests ES index templates if enabled."
    )
    index_template_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern(allow=[".*"], deny=["^_.*"]),
        description="The regex patterns for filtering index templates to ingest.",
    )

    profiling: ElasticProfiling = Field(
        default_factory=ElasticProfiling,
        description="Configs to ingest data profiles from ElasticSearch.",
    )
    collapse_urns: CollapseUrns = Field(
        default_factory=CollapseUrns,
        description="""List of regex patterns to remove from the name of the URN. All of the indices before removal of URNs are considered as the same dataset. These are applied in order for each URN.
        The main case where you would want to have multiple of these if the name where you are trying to remove suffix from have different formats.
        e.g. ending with -YYYY-MM-DD as well as ending -epochtime would require you to have 2 regex patterns to remove the suffixes across all URNs.""",
    )

    def is_profiling_enabled(self) -> bool:
        return self.profiling.enabled and is_profiling_enabled(
            self.profiling.operation_config
        )

    @validator("host")
    def host_colon_port_comma(cls, host_val: str) -> str:
        for entry in host_val.split(","):
            entry = remove_protocol(entry)
            for suffix in ["/"]:
                if entry.endswith(suffix):
                    entry = entry[: -len(suffix)]
            validate_host_port(entry)
        return host_val

    @property
    def http_auth(self) -> Optional[Tuple[str, str]]:
        return None if self.username is None else (self.username, self.password or "")


@platform_name("Elasticsearch")
@config_class(ElasticsearchSourceConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
class ElasticsearchSource(Source):

    """
    This plugin extracts the following:

    - Metadata for indexes
    - Column types associated with each index field
    """

    def __init__(self, config: ElasticsearchSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config
        self.client = Elasticsearch(
            self.source_config.host,
            http_auth=self.source_config.http_auth,
            use_ssl=self.source_config.use_ssl,
            verify_certs=self.source_config.verify_certs,
            ca_certs=self.source_config.ca_certs,
            client_cert=self.source_config.client_cert,
            client_key=self.source_config.client_key,
            ssl_assert_hostname=self.source_config.ssl_assert_hostname,
            ssl_assert_fingerprint=self.source_config.ssl_assert_fingerprint,
            url_prefix=self.source_config.url_prefix,
        )
        self.report = ElasticsearchSourceReport()
        self.data_stream_partition_count: Dict[str, int] = defaultdict(int)
        self.platform: str = "elasticsearch"
        self.cat_response: Optional[List[Dict[str, Any]]] = None

    @classmethod
    def create(
        cls, config_dict: Dict[str, Any], ctx: PipelineContext
    ) -> "ElasticsearchSource":
        config = ElasticsearchSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        indices = self.client.indices.get_alias()
        for index in indices:
            self.report.report_index_scanned(index)

            if self.source_config.index_pattern.allowed(index):
                for mcp in self._extract_mcps(index, is_index=True):
                    yield mcp.as_workunit()
            else:
                self.report.report_dropped(index)

        for mcp in self._get_data_stream_index_count_mcps():
            yield mcp.as_workunit()
        if self.source_config.ingest_index_templates:
            templates = self.client.indices.get_template()
            for template in templates:
                if self.source_config.index_template_pattern.allowed(template):
                    for mcp in self._extract_mcps(template, is_index=False):
                        yield mcp.as_workunit()

    def _get_data_stream_index_count_mcps(
        self,
    ) -> Iterable[MetadataChangeProposalWrapper]:
        for data_stream, count in self.data_stream_partition_count.items():
            dataset_urn: str = make_dataset_urn_with_platform_instance(
                platform=self.platform,
                name=data_stream,
                env=self.source_config.env,
                platform_instance=self.source_config.platform_instance,
            )
            dataset_urn = collapse_urn(
                urn=dataset_urn, collapse_urns=self.source_config.collapse_urns
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=DatasetPropertiesClass(
                    customProperties={"numPartitions": str(count)}
                ),
            )

    def _extract_mcps(
        self, index: str, is_index: bool = True
    ) -> Iterable[MetadataChangeProposalWrapper]:
        logger.debug(f"index='{index}', is_index={is_index}")

        if is_index:
            raw_index = self.client.indices.get(index=index)
            raw_index_metadata = raw_index[index]

            # 0. Dedup data_streams.
            data_stream = raw_index_metadata.get("data_stream")
            if data_stream:
                index = data_stream
                self.data_stream_partition_count[index] += 1
                if self.data_stream_partition_count[index] > 1:
                    # This is a duplicate, skip processing it further.
                    return
        else:
            raw_index = self.client.indices.get_template(name=index)
            raw_index_metadata = raw_index[index]
        collapsed_index_name = collapse_name(
            name=index, collapse_urns=self.source_config.collapse_urns
        )

        # 1. Construct and emit the schemaMetadata aspect
        # 1.1 Generate the schema fields from ES mappings.
        index_mappings = raw_index_metadata["mappings"]
        index_mappings_json_str: str = json.dumps(index_mappings)
        md5_hash = md5(index_mappings_json_str.encode()).hexdigest()
        schema_fields = list(
            ElasticToSchemaFieldConverter.get_schema_fields(index_mappings)
        )
        if not schema_fields:
            return

        # 1.2 Generate the SchemaMetadata aspect
        schema_metadata = SchemaMetadata(
            schemaName=collapsed_index_name,
            platform=make_data_platform_urn(self.platform),
            version=0,
            hash=md5_hash,
            platformSchema=OtherSchemaClass(rawSchema=index_mappings_json_str),
            fields=schema_fields,
        )

        # 1.3 Emit the mcp
        dataset_urn: str = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=index,
            platform_instance=self.source_config.platform_instance,
            env=self.source_config.env,
        )
        dataset_urn = collapse_urn(
            urn=dataset_urn, collapse_urns=self.source_config.collapse_urns
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=schema_metadata,
        )

        # 2. Construct and emit the status aspect.
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=StatusClass(removed=False),
        )

        # 3. Construct and emit subtype
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypesClass(
                typeNames=[
                    DatasetSubTypes.ELASTIC_INDEX_TEMPLATE
                    if not is_index
                    else DatasetSubTypes.ELASTIC_INDEX
                    if not data_stream
                    else DatasetSubTypes.ELASTIC_DATASTREAM
                ]
            ),
        )

        # 4. Construct and emit properties if needed. Will attempt to get the following properties
        custom_properties: Dict[str, str] = {}
        # 4.1 aliases
        index_aliases: List[str] = raw_index_metadata.get("aliases", {}).keys()
        if index_aliases:
            custom_properties["aliases"] = ",".join(index_aliases)
        # 4.2 index_patterns
        index_patterns: List[str] = raw_index_metadata.get("index_patterns", [])
        if index_patterns:
            custom_properties["index_patterns"] = ",".join(index_patterns)

        # 4.3 number_of_shards
        index_settings: Dict[str, Any] = raw_index_metadata.get("settings", {}).get(
            "index", {}
        )
        num_shards: str = index_settings.get("number_of_shards", "")
        if num_shards:
            custom_properties["num_shards"] = num_shards
        # 4.4 number_of_replicas
        num_replicas: str = index_settings.get("number_of_replicas", "")
        if num_replicas:
            custom_properties["num_replicas"] = num_replicas

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DatasetPropertiesClass(customProperties=custom_properties),
        )

        # 5. Construct and emit platform instance aspect
        if self.source_config.platform_instance:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=DataPlatformInstanceClass(
                    platform=make_data_platform_urn(self.platform),
                    instance=make_dataplatform_instance_urn(
                        self.platform, self.source_config.platform_instance
                    ),
                ),
            )

        if self.source_config.is_profiling_enabled():
            if self.cat_response is None:
                self.cat_response = self.client.cat.indices(
                    params={
                        "format": "json",
                        "bytes": "b",
                        "h": "index,docs.count,store.size",
                    }
                )
                if self.cat_response is None:
                    return
                for item in self.cat_response:
                    item["index"] = collapse_name(
                        name=item["index"],
                        collapse_urns=self.source_config.collapse_urns,
                    )

            profile_info_current = list(
                filter(lambda x: x["index"] == collapsed_index_name, self.cat_response)
            )
            if len(profile_info_current) > 0:
                self.cat_response = list(
                    filter(
                        lambda x: x["index"] != collapsed_index_name, self.cat_response
                    )
                )
                row_count = 0
                size_in_bytes = 0
                for profile_info in profile_info_current:
                    if profile_info["docs.count"] is not None:
                        row_count += int(profile_info["docs.count"])
                    if profile_info["store.size"] is not None:
                        size_in_bytes += int(profile_info["store.size"])
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=DatasetProfileClass(
                        timestampMillis=int(time.time() * 1000),
                        rowCount=row_count,
                        columnCount=len(schema_fields),
                        sizeInBytes=size_in_bytes,
                    ),
                )

    def get_report(self):
        return self.report

    def close(self):
        if self.client:
            self.client.close()
        super().close()
