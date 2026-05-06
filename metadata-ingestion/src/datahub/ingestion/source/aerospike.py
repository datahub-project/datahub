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
from pydantic import ConfigDict, PositiveInt, SecretStr, field_validator
from pydantic.fields import Field

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.emitter.mcp_builder import NamespaceKey
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
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset
from datahub.sdk.entity import Entity
from datahub.utilities.lossy_collections import LossyList

logger = logging.getLogger(__name__)


class AuthMode(Enum):
    AUTH_EXTERNAL = aerospike.AUTH_EXTERNAL
    AUTH_EXTERNAL_INSECURE = aerospike.AUTH_EXTERNAL_INSECURE
    AUTH_INTERNAL = aerospike.AUTH_INTERNAL


class AerospikeConfig(
    PlatformInstanceConfigMixin, EnvConfigMixin, StatefulIngestionConfigBase
):
    model_config = ConfigDict(populate_by_name=True)

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
        default=None, description="Path to the CA certificate directory."
    )
    tls_cafile: Optional[str] = Field(
        default=None, description="Path to the CA certificate file."
    )
    infer_schema_depth: int = Field(
        default=1,
        description="The depth of nested fields to infer schema. If set to `-1`, infer schema at all levels. If set to `0`, does not infer the schema. Default is `1`.",
        alias="inferSchemaDepth",
    )
    schema_sampling_size: Optional[PositiveInt] = Field(
        default=1000,
        description="Number of documents to use when inferring schema. If set to `null`, all documents will be scanned.",
        alias="schemaSamplingSize",
    )
    max_schema_size: Optional[PositiveInt] = Field(
        default=300,
        description="Maximum number of fields to include in the schema.",
        alias="maxSchemaSize",
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


@dataclass
class AerospikeSet:
    ns: str = ""
    set: str = ""
    objects: int = 0
    tombstones: int = 0
    # Storage stats vary by Aerospike version: older versions use memory_data_bytes +
    # device_data_bytes; newer versions use data_used_bytes. None means not reported.
    memory_data_bytes: Optional[int] = None
    device_data_bytes: Optional[int] = None
    data_used_bytes: Optional[int] = None
    truncate_lut: int = 0
    sindexes: int = 0
    index_populating: bool = False

    _KNOWN_FIELDS = frozenset(
        {
            "ns",
            "set",
            "objects",
            "tombstones",
            "memory_data_bytes",
            "device_data_bytes",
            "data_used_bytes",
            "truncate_lut",
            "sindexes",
            "index_populating",
        }
    )

    @classmethod
    def from_info_string(cls, info_string: str) -> "AerospikeSet":
        kwargs: Dict[str, Any] = {}
        for item in info_string.split(":"):
            if "=" not in item:
                continue
            key, value = item.split("=", 1)
            if key not in cls._KNOWN_FIELDS:
                continue
            if value.isdigit():
                kwargs[key] = int(value)
            elif value.lower() in ["true", "false"]:
                kwargs[key] = value.lower() == "true"
            else:
                kwargs[key] = value
        return cls(**kwargs)


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

    By default, schema inference samples 1,000 documents from each set. Setting `schema_sampling_size: null` will scan the entire set.

    Note that `schema_sampling_size` has no effect if `infer_schema_depth` is set to `0`.

    Really large schemas will be further truncated to a maximum of 300 schema fields. This is configurable using the `max_schema_size` parameter.

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
        type_string = PYTHON_TYPE_TO_AEROSPIKE_TYPE.get(field_type)
        if type_string is None:
            self.report.report_warning(
                message="unable to map type to metadata schema",
                context=f"{set_name}: {field_type}",
            )
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

    def get_workunits_internal(
        self,
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        all_sets = self.get_sets()
        namespaces = sorted(set(aerospike_set.ns for aerospike_set in all_sets))
        for namespace in namespaces:
            yield from self._get_namespace_workunits(namespace, all_sets)

    def _get_namespace_workunits(
        self, namespace: str, all_sets: List[AerospikeSet]
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        if not self.config.namespace_pattern.allowed(namespace):
            self.report.report_dropped(namespace)
            return

        namespace_key = NamespaceKey(
            namespace=namespace,
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
        )
        yield Container(
            namespace_key,
            display_name=namespace,
            subtype=DatasetContainerSubTypes.NAMESPACE,
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
                yield self._generate_dataset(curr_set, namespace_key, xdr_sets)
            except Exception as e:
                self.report.warning(
                    message="Failed to extract set",
                    context=dataset_name,
                    exc=e,
                )

    def _build_custom_properties(
        self, curr_set: AerospikeSet, xdr_sets: Dict[str, List[str]]
    ) -> Dict[str, str]:
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
        return custom_properties

    def _generate_dataset(
        self,
        curr_set: AerospikeSet,
        namespace_key: NamespaceKey,
        xdr_sets: Dict[str, List[str]],
    ) -> Dataset:
        dataset_name = f"{curr_set.ns}.{curr_set.set}"
        custom_properties = self._build_custom_properties(curr_set, xdr_sets)

        schema_metadata: Optional[SchemaMetadata] = None
        if self.config.infer_schema_depth != 0:
            schema_metadata = self._infer_schema_metadata(
                as_set=curr_set,
                dataset_name=dataset_name,
                custom_properties=custom_properties,
            )

        return Dataset(
            platform=self.platform,
            name=dataset_name,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
            display_name=curr_set.set,
            parent_container=namespace_key,
            custom_properties=custom_properties,
            schema=schema_metadata,
        )

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
            AerospikeSet.from_info_string(item) for item in sets_info.split(";") if item
        ]
        if self.config.ignore_empty_sets:
            all_sets = [
                aerospike_set for aerospike_set in all_sets if aerospike_set.objects > 0
            ]
        return all_sets

    def _infer_schema_metadata(
        self,
        as_set: AerospikeSet,
        dataset_name: str,
        custom_properties: Dict[str, str],
    ) -> SchemaMetadata:
        set_full_schema: Dict[Tuple[str, ...], SchemaDescription] = (
            construct_schema_aerospike(
                client=self.aerospike_client,
                as_set=as_set,
                delimiter=".",
                records_per_second=self.config.records_per_second,
                sample_size=self.config.schema_sampling_size,
                socket_timeout_ms=self.config.schema_query_timeout_ms,
            )
        )

        set_schema = self._limit_schema_size(set_full_schema, custom_properties)

        set_fields: Union[List[SchemaDescription], ValuesView[SchemaDescription]] = (
            set_schema.values()
        )
        logger.debug(f"Size of set {as_set.set} fields = {len(set_fields)}")
        canonical_schema: List[SchemaField] = []
        for schema_field in set_fields:
            field = SchemaField(
                fieldPath=schema_field["delimited_name"],
                nativeDataType=self.get_aerospike_type_string(
                    schema_field["type"], dataset_name
                ),
                type=self.get_field_type(schema_field["type"], dataset_name),
                description=None,
                nullable=schema_field["nullable"],
                recursive=False,
            )
            canonical_schema.append(field)

        return SchemaMetadata(
            schemaName=as_set.set,
            platform=f"urn:li:dataPlatform:{self.platform}",
            version=0,
            hash="",
            platformSchema=SchemalessClass(),
            fields=canonical_schema,
            primaryKeys=["PK"],
        )

    def _limit_schema_size(
        self,
        schema: Dict[Tuple[str, ...], SchemaDescription],
        custom_properties: Dict[str, str],
    ) -> Dict[Tuple[str, ...], SchemaDescription]:
        """
        Limits the size of the schema to the max_schema_size and infer_schema_depth.
        Mutates custom_properties in-place to record truncation metadata.
        """

        if self.config.infer_schema_depth != -1:
            truncated_schema = {
                k: v
                for k, v in schema.items()
                if len(k) <= self.config.infer_schema_depth
            }
            if len(truncated_schema) < len(schema):
                logger.debug(
                    f"Truncated schema from {len(schema)} to {len(truncated_schema)}"
                )
                schema_depth = max([len(k) for k in schema])
                self.report.report_warning(
                    title="Schema depth limit reached",
                    message="Truncating the collection schema because it has too many nested levels.",
                    context=f"Schema Depth: {len(schema)}, Configured threshold: {self.config.infer_schema_depth}",
                )
                custom_properties["schema.truncated"] = "True"
                custom_properties["schema.totalDepth"] = f"{schema_depth}"
                schema = truncated_schema

        schema_size = len(schema)
        max_schema_size = self.config.max_schema_size
        if max_schema_size is not None and schema_size > max_schema_size:
            self.report.report_warning(
                title="Too many schema fields",
                message="Downsampling the collection schema because it has too many schema fields.",
                context=f"Schema Size: {schema_size}, Configured threshold: {max_schema_size}",
            )
            custom_properties["schema.downsampled"] = "True"
            custom_properties["schema.totalFields"] = f"{schema_size}"
            schema = dict(
                sorted(
                    schema.items(),
                    key=lambda x: (
                        -x[1]["count"],
                        x[1]["delimited_name"],
                    ),
                )[0:max_schema_size]
            )
        return schema

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
        if xdr.get("enabled") != "true":
            return []
        if xdr.get("ship-only-specified-sets") == "false":
            ignored_sets = xdr.get("ignored-sets", "").split(",")
            return [as_set for as_set in sets if as_set not in ignored_sets]
        return xdr.get("shipped-sets", "").split(",")

    def get_report(self) -> AerospikeSourceReport:
        return self.report

    def close(self) -> None:
        try:
            self.aerospike_client.close()
        except Exception as e:
            logger.warning(f"Failed to close Aerospike client: {e}")
        super().close()
