import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from hashlib import md5
from typing import Any, Dict, Generator, Iterable, List, Optional, Tuple, Type

from elasticsearch import Elasticsearch
from pydantic import validator
from pydantic.fields import Field

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import DatasetSourceConfigBase
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
    ChangeTypeClass,
    DataPlatformInstanceClass,
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


class ElasticsearchSourceConfig(DatasetSourceConfigBase):
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


@platform_name("Elastic Search")
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

    @classmethod
    def create(
        cls, config_dict: Dict[str, Any], ctx: PipelineContext
    ) -> "ElasticsearchSource":
        config = ElasticsearchSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        indices = self.client.indices.get_alias()

        for index in indices:
            self.report.report_index_scanned(index)

            if self.source_config.index_pattern.allowed(index):
                for mcp in self._extract_mcps(index, is_index=True):
                    wu = MetadataWorkUnit(id=f"index-{index}", mcp=mcp)
                    self.report.report_workunit(wu)
                    yield wu
            else:
                self.report.report_dropped(index)

        for mcp in self._get_data_stream_index_count_mcps():
            wu = MetadataWorkUnit(id=f"index-{index}", mcp=mcp)
            self.report.report_workunit(wu)
            yield wu
        if self.source_config.ingest_index_templates:
            templates = self.client.indices.get_template()
            for template in templates:
                if self.source_config.index_template_pattern.allowed(template):
                    for mcp in self._extract_mcps(template, is_index=False):
                        wu = MetadataWorkUnit(id=f"template-{template}", mcp=mcp)
                        self.report.report_workunit(wu)
                        yield wu

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
            yield MetadataChangeProposalWrapper(
                entityType="dataset",
                entityUrn=dataset_urn,
                aspectName="datasetProperties",
                aspect=DatasetPropertiesClass(
                    customProperties={"numPartitions": str(count)}
                ),
                changeType=ChangeTypeClass.UPSERT,
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
            schemaName=index,
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
        yield MetadataChangeProposalWrapper(
            entityType="dataset",
            entityUrn=dataset_urn,
            aspectName="schemaMetadata",
            aspect=schema_metadata,
            changeType=ChangeTypeClass.UPSERT,
        )

        # 2. Construct and emit the status aspect.
        yield MetadataChangeProposalWrapper(
            entityType="dataset",
            entityUrn=dataset_urn,
            aspectName="status",
            aspect=StatusClass(removed=False),
            changeType=ChangeTypeClass.UPSERT,
        )

        # 3. Construct and emit subtype
        yield MetadataChangeProposalWrapper(
            entityType="dataset",
            entityUrn=dataset_urn,
            aspectName="subTypes",
            aspect=SubTypesClass(
                typeNames=[
                    "Index Template"
                    if not is_index
                    else "Index"
                    if not data_stream
                    else "Datastream"
                ]
            ),
            changeType=ChangeTypeClass.UPSERT,
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
            entityType="dataset",
            entityUrn=dataset_urn,
            aspectName="datasetProperties",
            aspect=DatasetPropertiesClass(customProperties=custom_properties),
            changeType=ChangeTypeClass.UPSERT,
        )

        # 5. Construct and emit platform instance aspect
        if self.source_config.platform_instance:
            yield MetadataChangeProposalWrapper(
                entityType="dataset",
                entityUrn=dataset_urn,
                aspectName="dataPlatformInstance",
                aspect=DataPlatformInstanceClass(
                    platform=make_data_platform_urn(self.platform),
                    instance=make_dataplatform_instance_urn(
                        self.platform, self.source_config.platform_instance
                    ),
                ),
                changeType=ChangeTypeClass.UPSERT,
            )

    def get_report(self):
        return self.report

    def close(self):
        if self.client:
            self.client.close()
        super().close()
