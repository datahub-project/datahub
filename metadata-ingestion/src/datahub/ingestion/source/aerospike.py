import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    ValuesView,
)

import aerospike
import aerospike_helpers
from pydantic import PositiveInt, SecretStr, field_validator
from pydantic.fields import Field

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    NamespaceKey,
    add_dataset_to_container,
    gen_containers,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.schema_inference.object import (
    SchemaDescription,
    construct_schema,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulIngestionConfigBase,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaField,
    SchemaFieldDataType,
    SchemalessClass,
    SchemaMetadata,
    StringTypeClass,
    UnionTypeClass,
)
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
)
from datahub.metadata.urns import DatasetUrn
from datahub.utilities.lossy_collections import LossyList

logger = logging.getLogger(__name__)


class AuthMode(Enum):
    AUTH_EXTERNAL = aerospike.AUTH_EXTERNAL
    AUTH_EXTERNAL_INSECURE = aerospike.AUTH_EXTERNAL_INSECURE
    AUTH_INTERNAL = aerospike.AUTH_INTERNAL


class AerospikeConfig(
    PlatformInstanceConfigMixin, EnvConfigMixin, StatefulIngestionConfigBase
):
    # See the Aerospike authentication docs for details and examples.
    hosts: List[tuple] = Field(
        default=[("localhost", 3000)], description="Aerospike hosts list."
    )

    @field_validator("hosts")
    @classmethod
    def validate_hosts(cls, hosts: List[tuple]) -> List[tuple]:
        for host in hosts:
            if len(host) < 2:
                raise ValueError(
                    f"Each host must have at least a hostname and port, got: {host!r}"
                )
            try:
                int(host[1])
            except (ValueError, TypeError) as e:
                raise ValueError(
                    f"Port must be an integer, got {host[1]!r} for host {host[0]!r}"
                ) from e
        return hosts

    username: Optional[str] = Field(default=None, description="Aerospike username.")
    password: Optional[SecretStr] = Field(
        default=None, description="Aerospike password."
    )
    auth_mode: Optional[AuthMode] = Field(
        default=AuthMode.AUTH_INTERNAL,
        description="The authentication mode with the server.",
    )
    tls_enabled: bool = Field(
        default=False, description="Whether to use TLS for the connection."
    )
    tls_capath: Optional[str] = Field(
        default=None, description="Path to the CA certificate file."
    )
    tls_cafile: Optional[str] = Field(
        default=None, description="Path to the CA certificate file."
    )
    inferSchemaDepth: int = Field(
        default=1,
        description="The depth of nested fields to infer schema. If set to `-1`, infer schema at all levels. If set to `0`, does not infer the schema. Default is `1`.",
    )
    schemaSamplingSize: Optional[PositiveInt] = Field(
        default=1000,
        description="Number of documents to use when inferring schema. If set to `null`, all documents will be scanned.",
    )
    maxSchemaSize: Optional[PositiveInt] = Field(
        default=300, description="Maximum number of fields to include in the schema."
    )
    namespace_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for namespaces to filter in ingestion.",
    )
    set_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for sets to filter in ingestion.",
    )
    ignore_empty_sets: bool = Field(
        default=False, description="Ignore empty sets in the schema inference."
    )
    records_per_second: int = Field(
        default=0,
        description="Number of records per second for Aerospike query. Default is 0, which means no limit.",
    )
    schema_query_timeout_ms: Optional[int] = Field(
        default=None,
        description="Socket timeout in milliseconds for schema inference queries. Default None uses the Aerospike client default.",
    )
    login_timeout_ms: Optional[int] = Field(
        default=None,
        description="Login timeout in milliseconds. Default None, using the default value of the Aerospike Python client.",
    )
    include_xdr: bool = Field(
        default=False, description="Include XDR information in the dataset properties."
    )
    # Custom Stateful Ingestion settings
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None


@dataclass
class AerospikeSourceReport(StaleEntityRemovalSourceReport):
    filtered: LossyList[str] = field(default_factory=LossyList)

    def report_dropped(self, name: str) -> None:
        self.filtered.append(name)


# map Aerospike Python Client types to canonical Aerospike strings
PYTHON_TYPE_TO_AEROSPIKE_TYPE = {
    int: "integer",
    bool: "boolean",
    str: "string",
    float: "double",
    dict: "map",
    aerospike.KeyOrderedDict: "map",
    list: "list",
    bytes: "blob",
    aerospike.GeoJSON: "GeoJSON",
    aerospike_helpers.HyperLogLog: "HyperLogLog",
    type(None): "null",
    "mixed": "mixed",
}

# map Aerospike Python Client types to DataHub classes
_field_type_mapping: Dict[Union[Type, str], Type] = {
    int: NumberTypeClass,
    bool: BooleanTypeClass,
    str: StringTypeClass,
    float: NumberTypeClass,
    dict: RecordTypeClass,
    aerospike.KeyOrderedDict: RecordTypeClass,
    list: ArrayTypeClass,
    bytes: BytesTypeClass,
    aerospike.GeoJSON: RecordTypeClass,
    aerospike_helpers.HyperLogLog: BytesTypeClass,
    type(None): NullTypeClass,
    "mixed": UnionTypeClass,
}


class AerospikeSet:
    def __init__(self, info_string: str):
        self.ns: str
        self.set: str
        self.objects: int
        self.tombstones: int
        self.memory_data_bytes: int
        self.device_data_bytes: int
        self.truncate_lut: int
        self.sindexes: int
        self.index_populating: bool

        info_list = info_string.split(":")
        for item in info_list:
            if "=" not in item:
                continue
            key, value = item.split("=", 1)
            if value.isdigit():
                setattr(self, key, int(value))
            elif value.lower() in ["true", "false"]:
                setattr(self, key, value.lower() == "true")
            else:
                setattr(self, key, value)


def construct_schema_aerospike(
    client: aerospike.Client,
    as_set: AerospikeSet,
    delimiter: str,
    records_per_second: int = 0,
    sample_size: Optional[int] = None,
    socket_timeout_ms: Optional[int] = None,
) -> Dict[Tuple[str, ...], SchemaDescription]:
    """
    Calls construct_schema on an Aerospike set

    Returned schema is keyed by tuples of nested field names, with each
    value containing 'types', 'count', 'nullable', 'delimited_name', and 'type' attributes.

    Parameters
    ----------
        client:
            the Aerospike client
        as_set:
            the Aerospike set
        delimiter:
            string to concatenate field names by
        records_per_second:
            number of records per second for Aerospike query
        sample_size:
            number of items in the set to sample
            (reads entire set if not provided)
        socket_timeout_ms:
            socket timeout in milliseconds for the query
    """

    query = client.query(as_set.ns, as_set.set)
    if sample_size:
        query.max_records = sample_size
    query.records_per_second = records_per_second
    if socket_timeout_ms is not None:
        query.socket_timeout = socket_timeout_ms  # type: ignore[attr-defined]

    try:
        res = query.results()
        records = [{**record[2], "PK": record[0][2]} for record in res]
    except Exception as e:
        logger.error(f"Error querying Aerospike set: {e}")
        records = []
    return construct_schema(records, delimiter)


@platform_name("Aerospike")
@config_class(AerospikeConfig)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@dataclass
class AerospikeSource(StatefulIngestionSourceBase):
    """
    This plugin extracts the following:

    - Namespaces and associated metadata
    - Sets in each namespace and schemas for each set (via schema inference)

    By default, schema inference samples 1,000 documents from each set. Setting `schemaSamplingSize: null` will scan the entire set.

    Note that `schemaSamplingSize` has no effect if `enableSchemaInference: False` is set.

    Really large schemas will be further truncated to a maximum of 300 schema fields. This is configurable using the `maxSchemaSize` parameter.

    """

    config: AerospikeConfig
    report: AerospikeSourceReport
    aerospike_client: aerospike.Client
    platform: str = "aerospike"

    def __init__(self, ctx: PipelineContext, config: AerospikeConfig):
        super().__init__(config, ctx)
        self.config = config
        self.report = AerospikeSourceReport()

        hosts = [
            (x[0], int(x[1]), x[2] if len(x) > 2 else None) for x in self.config.hosts
        ]
        client_config: Dict[str, Any] = {"hosts": hosts}
        if self.config.username is not None:
            client_config["user"] = self.config.username
        if self.config.password is not None:
            client_config["password"] = self.config.password.get_secret_value()
        if self.config.auth_mode is not None:
            client_config["auth_mode"] = self.config.auth_mode.value
        if self.config.login_timeout_ms is not None:
            client_config["login_timeout_ms"] = self.config.login_timeout_ms
        if self.config.tls_enabled:
            client_config["tls"] = {}
            client_config["tls"]["enable"] = self.config.tls_enabled
            if self.config.tls_capath is not None:
                client_config["tls"]["capath"] = self.config.tls_capath
            if self.config.tls_cafile is not None:
                client_config["tls"]["cafile"] = self.config.tls_cafile

        try:
            self.aerospike_client = aerospike.client(client_config).connect()
        except Exception as e:
            self.report.failure(
                message="Failed to connect to Aerospike",
                title="Connection Error",
                exc=e,
            )
            raise

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AerospikeSource":
        config = AerospikeConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_aerospike_type_string(
        self, field_type: Union[Type, str], set_name: str
    ) -> str:
        """
        Return Aerospike type string from a Python type

        Parameters
        ----------
            field_type:
                type of Python object
            set_name:
                name of set (for logging)
        """
        try:
            type_string = PYTHON_TYPE_TO_AEROSPIKE_TYPE[field_type]
        except KeyError:
            self.report.report_warning(
                message="unable to map type to metadata schema",
                context=f"{set_name}: {field_type}",
            )
            PYTHON_TYPE_TO_AEROSPIKE_TYPE[field_type] = "unknown"
            type_string = "unknown"

        return type_string

    def get_field_type(
        self, field_type: Union[Type, str], set_name: str
    ) -> SchemaFieldDataType:
        """
        Maps types encountered in Aerospike Python client to corresponding schema types.

        Parameters
        ----------
            field_type:
                type of Python object
            set_name:
                name of set (for logging)
        """
        TypeClass: Optional[Type] = _field_type_mapping.get(field_type)

        if TypeClass is None:
            self.report.report_warning(
                message="unable to map type to metadata schema",
                context=f"{set_name}: {field_type}",
            )
            TypeClass = NullTypeClass

        return SchemaFieldDataType(type=TypeClass())

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        all_sets = self.get_sets()
        namespaces = sorted(set(aerospike_set.ns for aerospike_set in all_sets))
        for namespace in namespaces:
            yield from self._get_namespace_workunits(namespace, all_sets)

    def _get_namespace_workunits(
        self, namespace: str, all_sets: List[AerospikeSet]
    ) -> Iterable[MetadataWorkUnit]:
        if not self.config.namespace_pattern.allowed(namespace):
            self.report.report_dropped(namespace)
            return

        namespace_key = NamespaceKey(
            namespace=namespace,
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
        )
        yield from gen_containers(
            container_key=namespace_key,
            name=namespace,
            sub_types=[DatasetContainerSubTypes.NAMESPACE],
        )

        ns_sets: List[AerospikeSet] = [
            aerospike_set for aerospike_set in all_sets if aerospike_set.ns == namespace
        ]
        if self.config.include_xdr:
            xdr_sets = self.xdr_sets(
                namespace, [aerospike_set.set for aerospike_set in ns_sets]
            )
        else:
            xdr_sets = {}

        # traverse sets in sorted order so output is consistent
        for curr_set in sorted(ns_sets, key=lambda x: x.set):
            dataset_name = f"{namespace}.{curr_set.set}"
            if not self.config.set_pattern.allowed(dataset_name):
                self.report.report_dropped(dataset_name)
                continue
            try:
                yield from self._get_set_workunits(curr_set, namespace_key, xdr_sets)
            except Exception as e:
                self.report.warning(
                    message="Failed to extract set",
                    context=dataset_name,
                    exc=e,
                )

    def _get_set_workunits(
        self,
        curr_set: AerospikeSet,
        namespace_key: NamespaceKey,
        xdr_sets: Dict[str, List[str]],
    ) -> Iterable[MetadataWorkUnit]:
        namespace = curr_set.ns
        dataset_name = f"{namespace}.{curr_set.set}"

        dataset_urn = DatasetUrn.create_from_ids(
            platform_id=self.platform,
            table_name=dataset_name,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )

        data_platform_instance = None
        if self.config.platform_instance:
            data_platform_instance = DataPlatformInstanceClass(
                platform=make_data_platform_urn(self.platform),
                instance=make_dataplatform_instance_urn(
                    self.platform, self.config.platform_instance
                ),
            )

        custom_properties: Dict[str, str] = {
            "record_count": str(curr_set.objects),
        }
        # Storage stats format varies by Aerospike version:
        # older versions use memory_data_bytes + device_data_bytes; newer use data_used_bytes
        for stat in ("memory_data_bytes", "device_data_bytes", "data_used_bytes"):
            value = getattr(curr_set, stat, None)
            if value is not None:
                custom_properties[stat] = str(value)
        set_xdr = xdr_sets.get(curr_set.set)
        if set_xdr:
            custom_properties["xdr_dcs"] = ",".join(set_xdr)

        dataset_properties = DatasetPropertiesClass(
            name=curr_set.set,
            tags=[],
            customProperties=custom_properties,
        )

        schema_metadata: Optional[SchemaMetadata] = None
        if self.config.inferSchemaDepth != 0:
            schema_metadata = self._infer_schema_metadata(
                as_set=curr_set,
                dataset_urn=dataset_urn,
                dataset_properties=dataset_properties,
            )

        yield from add_dataset_to_container(namespace_key, dataset_urn.urn())
        yield from [
            mcp.as_workunit()
            for mcp in MetadataChangeProposalWrapper.construct_many(
                entityUrn=dataset_urn.urn(),
                aspects=[
                    schema_metadata,
                    dataset_properties,
                    data_platform_instance,
                ],
            )
        ]

    def get_sets(self) -> List[AerospikeSet]:
        try:
            sets_info: str = self.aerospike_client.info_random_node("sets")
        except Exception as e:
            self.report.failure(
                message="Failed to retrieve sets info from Aerospike",
                exc=e,
            )
            raise
        sets_info = (
            sets_info[len("sets\t") :] if sets_info.startswith("sets\t") else sets_info
        )
        sets_info = sets_info[: -len(";\n")] if sets_info.endswith(";\n") else sets_info

        all_sets: List[AerospikeSet] = [
            AerospikeSet(item) for item in sets_info.split(";") if item
        ]
        if self.config.ignore_empty_sets:
            all_sets = [
                aerospike_set for aerospike_set in all_sets if aerospike_set.objects > 0
            ]
        return all_sets

    def _infer_schema_metadata(
        self,
        dataset_urn: DatasetUrn,
        as_set: AerospikeSet,
        dataset_properties: DatasetPropertiesClass,
    ) -> SchemaMetadata:
        set_full_schema: Dict[Tuple[str, ...], SchemaDescription] = (
            construct_schema_aerospike(
                client=self.aerospike_client,
                as_set=as_set,
                delimiter=".",
                records_per_second=self.config.records_per_second,
                sample_size=self.config.schemaSamplingSize,
                socket_timeout_ms=self.config.schema_query_timeout_ms,
            )
        )

        set_schema, dataset_properties = self.limit_schema_size(
            set_full_schema, dataset_properties
        )

        set_fields: Union[List[SchemaDescription], ValuesView[SchemaDescription]] = (
            set_schema.values()
        )
        logger.debug(f"Size of set {as_set.set} fields = {len(set_fields)}")
        # append each schema field (sort so output is consistent)
        canonical_schema: List[SchemaField] = []
        for schema_field in set_fields:
            field = SchemaField(
                fieldPath=schema_field["delimited_name"],
                nativeDataType=self.get_aerospike_type_string(
                    schema_field["type"], dataset_urn.name
                ),
                type=self.get_field_type(schema_field["type"], dataset_urn.name),
                description=None,
                nullable=schema_field["nullable"],
                recursive=False,
            )
            canonical_schema.append(field)

        # create schema metadata object for set
        return SchemaMetadata(
            schemaName=as_set.set,
            platform=dataset_urn.platform,
            version=0,
            hash="",
            platformSchema=SchemalessClass(),
            fields=canonical_schema,
            primaryKeys=["PK"],
        )

    def limit_schema_size(
        self,
        schema: Dict[Tuple[str, ...], SchemaDescription],
        dataset_properties: DatasetPropertiesClass,
    ) -> Tuple[Dict[Tuple[str, ...], SchemaDescription], DatasetPropertiesClass]:
        """
        Limits the size of the schema to the maxSchemaSize and inferSchemaDepth
        """

        if self.config.inferSchemaDepth != -1:
            # Infer schema only at the specified depth
            truncated_schema = {
                k: v
                for k, v in schema.items()
                if len(k) <= self.config.inferSchemaDepth
            }
            if len(truncated_schema) < len(schema):
                logger.debug(
                    f"Truncated schema from {len(schema)} to {len(truncated_schema)}"
                )
                schema_depth = max([len(k) for k in schema])
                self.report.report_warning(
                    title="Schema depth limit reached",
                    message="Truncating the collection schema because it has too many nested levels.",
                    context=f"Schema Depth: {len(schema)}, Configured threshold: {self.config.inferSchemaDepth}",
                )
                dataset_properties.customProperties["schema.truncated"] = "True"
                dataset_properties.customProperties["schema.totalDepth"] = (
                    f"{schema_depth}"
                )
                schema = truncated_schema

        schema_size = len(schema)
        max_schema_size = self.config.maxSchemaSize
        if max_schema_size is not None and schema_size > max_schema_size:
            self.report.report_warning(
                title="Too many schema fields",
                message="Downsampling the collection schema because it has too many schema fields.",
                context=f"Schema Size: {schema_size}, Configured threshold: {max_schema_size}",
            )
            dataset_properties.customProperties["schema.downsampled"] = "True"
            dataset_properties.customProperties["schema.totalFields"] = f"{schema_size}"
            # downsample the schema, using frequency as the sort key
            schema = dict(
                sorted(
                    schema.items(),
                    key=lambda x: (
                        -x[1]["count"],
                        x[1]["delimited_name"],
                    ),
                )[0:max_schema_size]
            )
        return schema, dataset_properties

    def xdr_sets(self, namespace: str, sets: List[str]) -> Dict[str, List[str]]:
        sets_dc: Dict[str, List[str]] = {key: [] for key in sets}
        try:
            dcs = (
                self.aerospike_client.info_random_node("get-config:context=xdr")
                .split("dcs=")[1]
                .split(";")[0]
                .split(",")
            )
        except Exception as e:
            self.report.warning(
                message="Failed to retrieve XDR config from Aerospike",
                context=namespace,
                exc=e,
            )
            return sets_dc
        if dcs == [""]:
            logger.debug("No DCs found")
            return sets_dc
        for dc in dcs:
            try:
                xdr_info: str = (
                    self.aerospike_client.info_random_node(
                        f"get-config:context=xdr;namespace={namespace};dc={dc}"
                    )
                    .split("\t")[1]
                    .split("\n")[0]
                )
            except Exception as e:
                self.report.warning(
                    message="Failed to retrieve XDR config for DC",
                    context=f"{namespace}/{dc}",
                    exc=e,
                )
                continue
            shipped_sets = AerospikeSource._get_dc_shipped_sets(xdr_info, sets)
            for as_set in shipped_sets:
                sets_dc[as_set].append(dc)
        return sets_dc

    @staticmethod
    def _get_dc_shipped_sets(xdr_info: str, sets: List[str]) -> List[str]:
        xdr = {
            k: v
            for pair in xdr_info.split(";")
            if "=" in pair
            for k, v in [pair.split("=", 1)]
        }
        if xdr["enabled"] != "true":
            return []
        if xdr["ship-only-specified-sets"] == "false":
            ignored_sets = xdr["ignored-sets"].split(",")
            return [as_set for as_set in sets if as_set not in ignored_sets]
        return xdr["shipped-sets"].split(",")

    def get_report(self) -> AerospikeSourceReport:
        return self.report

    def close(self) -> None:
        try:
            self.aerospike_client.close()
        except Exception as e:
            logger.warning(f"Failed to close Aerospike client: {e}")
        super().close()
