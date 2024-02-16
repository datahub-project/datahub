import logging
from dataclasses import dataclass, field
from typing import Any, Counter, Dict, Iterable, List, Optional, Tuple, Type, Union

import boto3
import pydantic
from botocore.client import BaseClient
from pydantic.fields import Field

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import DatasetSourceConfigMixin
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_domain_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import add_domain_to_entity_wu
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor, SourceCapability
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.schema_inference.object import SchemaDescription
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
from datahub.utilities.registries.domain_registry import DomainRegistry

MAX_ITEMS_TO_RETRIEVE = 100
PAGE_SIZE = 100
MAX_SCHEMA_SIZE = 300
MAX_PRIMARY_KEYS_SIZE = 100
FIELD_DELIMITER = "."

logger: logging.Logger = logging.getLogger(__name__)


class DynamoDBConfig(DatasetSourceConfigMixin, StatefulIngestionConfigBase):
    # TODO: refactor the config to use AwsConnectionConfig and create a method get_dynamodb_client
    # in the class to provide optional region name input
    aws_access_key_id: str = Field(description="AWS Access Key ID.")
    aws_secret_access_key: pydantic.SecretStr = Field(description="AWS Secret Key.")

    domain: Dict[str, AllowDenyPattern] = Field(
        default=dict(),
        description="regex patterns for tables to filter to assign domain_key. ",
    )

    # This config option allows user to include a list of items from a table when we scan and construct the schema,
    # the key of this dict is table name and the value is the list of item primary keys in dynamodb format,
    # if the table use composite key then the value should have partition key and sort key present
    include_table_item: Optional[Dict[str, List[Dict]]] = Field(
        default=None,
        description="[Advanced] The primary keys of items of a table in dynamodb format the user would like to include in schema. "
        'Refer "Advanced Configurations" section for more details',
    )

    table_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for tables to filter in ingestion. The table name format is 'region.table'",
    )
    # Custom Stateful Ingestion settings
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None


@dataclass
class DynamoDBSourceReport(StaleEntityRemovalSourceReport):
    filtered: List[str] = field(default_factory=list)

    def report_dropped(self, name: str) -> None:
        self.filtered.append(name)


# map attribute data types to native types
_attribute_type_to_native_type_mapping: Dict[str, str] = {
    "N": "Numbers",
    "B": "Bytes",
    "S": "String",
    "M": "Map",
    "L": "List",
    "SS": "String List",
    "NS": "Number List",
    "BS": "Binary Set",
    "NULL": "Null",
    # if the attribute type is NULL the attribute value will be true or false.
    "BOOL": "Boolean",
    "mixed": "mixed",
}
# map DynamoDB attribute types to DataHub classes
_attribute_type_to_field_type_mapping: Dict[str, Type] = {
    "N": NumberTypeClass,
    "B": BytesTypeClass,
    "S": StringTypeClass,
    "M": RecordTypeClass,
    "L": ArrayTypeClass,
    "SS": ArrayTypeClass,
    "NS": ArrayTypeClass,
    "BS": ArrayTypeClass,
    "NULL": BooleanTypeClass,
    "BOOL": BooleanTypeClass,
    "mixed": UnionTypeClass,
}


@platform_name("DynamoDB", id="dynamodb")
@config_class(DynamoDBConfig)
@support_status(SupportStatus.TESTING)
@capability(
    SourceCapability.PLATFORM_INSTANCE,
    "By default, platform_instance will use the AWS account id",
)
@capability(
    SourceCapability.DELETION_DETECTION,
    "Optionally enabled via `stateful_ingestion.remove_stale_metadata`",
    supported=True,
)
class DynamoDBSource(StatefulIngestionSourceBase):
    """
    This plugin extracts the following:

    AWS DynamoDB table names with their region, and infer schema of attribute names and types by scanning
    the table

    """

    config: DynamoDBConfig
    report: DynamoDBSourceReport
    platform: str

    def __init__(self, ctx: PipelineContext, config: DynamoDBConfig, platform: str):
        super().__init__(config, ctx)
        self.config = config
        self.report = DynamoDBSourceReport()
        self.platform = platform

        if self.config.domain:
            self.domain_registry = DomainRegistry(
                cached_domains=[domain_id for domain_id in self.config.domain],
                graph=self.ctx.graph,
            )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "DynamoDBSource":
        config = DynamoDBConfig.parse_obj(config_dict)
        return cls(ctx, config, "dynamodb")

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        # This is a offline call to get available region names from botocore library
        session = boto3.Session()
        dynamodb_regions = session.get_available_regions("dynamodb")
        logger.info(f"region names {dynamodb_regions}")

        # traverse databases in sorted order so output is consistent
        for region in dynamodb_regions:
            logger.info(f"Processing region {region}")
            # create a new dynamodb client for each region,
            # it seems for one client we could only list the table of one specific region,
            # the list_tables() method don't take any config that related to region
            dynamodb_client = boto3.client(
                "dynamodb",
                region_name=region,
                aws_access_key_id=self.config.aws_access_key_id,
                aws_secret_access_key=self.config.aws_secret_access_key.get_secret_value(),
            )

            for table_name in self._list_tables(dynamodb_client):
                dataset_name = f"{region}.{table_name}"
                if not self.config.table_pattern.allowed(dataset_name):
                    logger.debug(f"skipping table: {dataset_name}")
                    self.report.report_dropped(dataset_name)
                    continue

                logger.debug(f"Processing table: {dataset_name}")
                table_info = dynamodb_client.describe_table(TableName=table_name)[
                    "Table"
                ]
                account_id = table_info["TableArn"].split(":")[4]
                platform_instance = self.config.platform_instance or account_id
                dataset_urn = make_dataset_urn_with_platform_instance(
                    platform=self.platform,
                    platform_instance=platform_instance,
                    name=dataset_name,
                )
                dataset_properties = DatasetPropertiesClass(
                    tags=[],
                    customProperties={
                        "table.arn": table_info["TableArn"],
                        "table.totalItems": str(table_info["ItemCount"]),
                    },
                )
                primary_key_dict = self.extract_primary_key_from_key_schema(table_info)
                table_schema = self.construct_schema_from_dynamodb(
                    dynamodb_client, region, table_name
                )
                schema_metadata = self.construct_schema_metadata(
                    table_name,
                    dataset_urn,
                    dataset_properties,
                    table_schema,
                    primary_key_dict,
                )

                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=schema_metadata,
                ).as_workunit()

                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=dataset_properties,
                ).as_workunit()

                yield from self._get_domain_wu(
                    dataset_name=table_name,
                    entity_urn=dataset_urn,
                )

                platform_instance_aspect = DataPlatformInstanceClass(
                    platform=make_data_platform_urn(self.platform),
                    instance=make_dataplatform_instance_urn(
                        self.platform, platform_instance
                    ),
                )

                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=platform_instance_aspect,
                ).as_workunit()

    def _list_tables(
        self,
        dynamodb_client: BaseClient,
    ) -> Iterable[str]:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/paginator/ListTables.html
        try:
            for page in dynamodb_client.get_paginator("list_tables").paginate():
                table_names = page.get("TableNames")
                if table_names:
                    yield from table_names
        except Exception as ex:
            # TODO: If regions is config input then this would be self.report.report_warning,
            # we can create dynamodb client to take aws region or regions as user input
            logger.info(f"Exception happened while listing tables, skipping: {ex}")

    def construct_schema_from_dynamodb(
        self,
        dynamodb_client: BaseClient,
        region: str,
        table_name: str,
    ) -> Dict[Tuple[str, ...], SchemaDescription]:
        """
        This will use the dynamodb client to scan the given table to retrieve items with pagination,
        and construct the schema of this table by reading the attributes of the retrieved items
        """
        paginator = dynamodb_client.get_paginator("scan")
        schema: Dict[Tuple[str, ...], SchemaDescription] = {}
        """
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Paginator.Scan
        Note that the behavior of the pagination does not align with the documentation according to https://stackoverflow.com/questions/39201093/how-to-use-boto3-pagination

        What we'll do is to create a paginator and boto3 library handles the pagination automatically. We'll iterate through pages
        and retrieve the items from page.

        The MaxItems is the total number of items to return, and PageSize is the size of each page, we are assigning same value
        to these two config. If MaxItems is more than PageSize then we expect MaxItems / PageSize pages in response_iterator will return
        """
        self.include_table_item_to_schema(dynamodb_client, region, table_name, schema)
        response_iterator = paginator.paginate(
            TableName=table_name,
            PaginationConfig={
                "MaxItems": MAX_ITEMS_TO_RETRIEVE,
                "PageSize": PAGE_SIZE,
            },
        )
        # iterate through pagination result to retrieve items
        for page in response_iterator:
            items = page["Items"]
            if len(items) > 0:
                self.construct_schema_from_items(items, schema)

        return schema

    def include_table_item_to_schema(
        self,
        dynamodb_client: Any,
        region: str,
        table_name: str,
        schema: Dict[Tuple[str, ...], SchemaDescription],
    ) -> None:
        """
        It will look up in the config include_table_item dict to see if "region.table_name" exists as key,
        if it exists then get the items by primary key from the table and put it to schema
        """
        if self.config.include_table_item is None:
            return
        dataset_name = f"{region}.{table_name}"
        if dataset_name not in self.config.include_table_item.keys():
            return
        primary_key_list = self.config.include_table_item.get(dataset_name)
        assert isinstance(primary_key_list, List)
        if len(primary_key_list) > MAX_PRIMARY_KEYS_SIZE:
            logger.info(
                f"the provided primary keys list size exceeded the max size for table {dataset_name}, we'll only process the first {MAX_PRIMARY_KEYS_SIZE} items"
            )
            primary_key_list = primary_key_list[0:MAX_PRIMARY_KEYS_SIZE]
        items = []
        response = dynamodb_client.batch_get_item(
            RequestItems={table_name: {"Keys": primary_key_list}}
        ).get("Responses")
        if response is None:
            logger.error(
                f"failed to retrieve item from table {table_name} by the given key {primary_key_list}"
            )
            return
        logger.debug(
            f"successfully retrieved {len(primary_key_list)} items based on supplied primary key list"
        )
        items = response.get(table_name)

        self.construct_schema_from_items(items, schema)

    def construct_schema_from_items(
        self,
        items: List[Dict[str, Dict]],
        schema: Dict[Tuple[str, ...], SchemaDescription],
    ) -> None:
        """
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.scan
        each item in the list is a dict, the key represents the attribute name,
        and the value is a one entry dict, more details in the below code comments
        we are writing our own construct schema method, take the attribute name as key and SchemaDescription as value
        """
        for document in items:
            self.append_schema(schema, document)

    def append_schema(
        self,
        schema: Dict[Tuple[str, ...], SchemaDescription],
        document: Dict[str, Dict],
        parent_field_path: Tuple[str, ...] = (),
    ) -> None:
        # the key is the attribute name and the value is a dict with only one entry,
        # whose key is the data type and value is the data and we will recursively expand
        # map data type to get flattened field
        for key, value in document.items():
            if value is not None:
                data_type = list(value.keys())[0]
                attribute_value = value[data_type]
                current_field_path = parent_field_path + (key,)
                # Handle nested maps by recursive calls
                if data_type == "M":
                    logger.debug(
                        f"expanding nested fields for map, current_field_path: {current_field_path}"
                    )
                    self.append_schema(schema, attribute_value, current_field_path)

                if current_field_path not in schema:
                    schema[current_field_path] = {
                        "types": Counter({data_type: 1}),
                        "count": 1,
                        # It seems we don't have collapsed field name so we are using attribute name here
                        "delimited_name": FIELD_DELIMITER.join(current_field_path),
                        "type": data_type,
                        "nullable": False,
                    }
                else:
                    schema[current_field_path]["types"].update({data_type: 1})
                    schema[current_field_path]["count"] += 1
                    # if we found an attribute name with different attribute type, we consider this attribute type as "mixed"
                    if len(schema[current_field_path]["types"]) > 1:
                        schema[current_field_path]["type"] = "mixed"
                    schema[current_field_path]["nullable"] |= (
                        attribute_value is None
                    )  # Mark as nullable if null encountered
                types = schema[current_field_path]["types"]
                logger.debug(
                    f"append schema with field_path: {current_field_path} and type: {types}"
                )

    def construct_schema_metadata(
        self,
        table_name: str,
        dataset_urn: str,
        dataset_properties: DatasetPropertiesClass,
        schema: Dict[Tuple[str, ...], SchemaDescription],
        primary_key_dict: Dict[str, str],
    ) -> SchemaMetadata:
        """ "
        To construct the schema metadata, it will first sort the schema by the occurrence of attribute names
        in descending order and truncate the schema by MAX_SCHEMA_SIZE, and then start to construct the
        schema metadata sorted by attribute name
        """

        canonical_schema: List[SchemaField] = []
        schema_size = len(schema.values())
        table_fields = list(schema.values())
        if schema_size > MAX_SCHEMA_SIZE:
            # downsample the schema, using frequency as the sort key
            self.report.report_warning(
                key=dataset_urn,
                reason=f"Downsampling the table schema because MAX_SCHEMA_SIZE threshold is {MAX_SCHEMA_SIZE}",
            )

            # Add this information to the custom properties so user can know they are looking at down sampled schema
            dataset_properties.customProperties["schema.downsampled"] = "True"
            dataset_properties.customProperties["schema.totalFields"] = f"{schema_size}"
        # append each schema field, schema will be sorted by count descending and delimited_name ascending and sliced to only include MAX_SCHEMA_SIZE items
        for schema_field in sorted(
            table_fields,
            key=lambda x: (
                -x["count"],
                x["delimited_name"],
            ),  # Negate `count` for descending order, `delimited_name` stays the same for ascending
        )[0:MAX_SCHEMA_SIZE]:
            field_path = schema_field["delimited_name"]
            native_data_type = self.get_native_type(schema_field["type"], table_name)
            type = self.get_field_type(schema_field["type"], table_name)
            description = None
            nullable = True
            if field_path in primary_key_dict:
                description = (
                    "Partition Key"
                    if primary_key_dict.get(field_path) == "HASH"
                    else "Sort Key"
                )
                # primary key should not be nullable
                nullable = False

            field = SchemaField(
                fieldPath=field_path,
                nativeDataType=native_data_type,
                type=type,
                description=description,
                nullable=nullable,
                recursive=False,
            )
            canonical_schema.append(field)

        # create schema metadata object for table
        schema_metadata = SchemaMetadata(
            schemaName=table_name,
            platform=f"urn:li:dataPlatform:{self.platform}",
            version=0,
            hash="",
            platformSchema=SchemalessClass(),
            fields=canonical_schema,
        )
        return schema_metadata

    def extract_primary_key_from_key_schema(
        self, table_info: Dict[str, Any]
    ) -> Dict[str, str]:
        key_schema = table_info.get("KeySchema")
        primary_key_dict = {}
        assert isinstance(key_schema, List)
        for key in key_schema:
            attribute_name = key.get("AttributeName")
            key_type = key.get("KeyType")
            primary_key_dict[attribute_name] = key_type
        return primary_key_dict

    def get_native_type(self, attribute_type: Union[type, str], table_name: str) -> str:
        assert isinstance(attribute_type, str)
        type_string: Optional[str] = _attribute_type_to_native_type_mapping.get(
            attribute_type
        )
        if type_string is None:
            self.report.report_warning(
                table_name, f"unable to map type {attribute_type} to native data type"
            )
            return _attribute_type_to_native_type_mapping[attribute_type]
        return type_string

    def get_field_type(
        self, attribute_type: Union[type, str], table_name: str
    ) -> SchemaFieldDataType:
        assert isinstance(attribute_type, str)
        type_class: Optional[type] = _attribute_type_to_field_type_mapping.get(
            attribute_type
        )

        if type_class is None:
            self.report.report_warning(
                table_name,
                f"unable to map type {attribute_type} to metadata schema field type",
            )
            type_class = NullTypeClass
        return SchemaFieldDataType(type=type_class())

    def get_report(self) -> DynamoDBSourceReport:
        return self.report

    def _get_domain_wu(
        self, dataset_name: str, entity_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        domain_urn = None
        for domain, pattern in self.config.domain.items():
            if pattern.allowed(dataset_name):
                domain_urn = make_domain_urn(
                    self.domain_registry.get_domain_urn(domain)
                )
                break

        if domain_urn:
            yield from add_domain_to_entity_wu(
                entity_urn=entity_urn,
                domain_urn=domain_urn,
            )
