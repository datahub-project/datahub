import logging
from typing import Any, Optional

from botocore.exceptions import BotoCoreError, ClientError

from datahub.emitter.mce_builder import make_data_platform_urn
from datahub.ingestion.source.aws.aws_common import aws_error_code
from datahub.ingestion.source.aws.glue_schema_registry import parse_glue_schema_fields
from datahub.ingestion.source.aws_messaging.config import GlueSchemaRegistryConfigBase
from datahub.ingestion.source.aws_messaging.constants import GLUE_ENTITY_NOT_FOUND
from datahub.ingestion.source.aws_messaging.report import AwsMessagingReportBase
from datahub.metadata.schema_classes import KafkaSchemaClass, SchemaMetadataClass

logger = logging.getLogger(__name__)


class MessagingSchemaRegistry:
    """Schemas for queues or topics from AWS Glue Schema Registry, looked up by an
    explicit name map first and, if enabled, a same-name convention."""

    def __init__(
        self,
        *,
        config: GlueSchemaRegistryConfigBase,
        glue_client: Any,
        platform: str,
        report: AwsMessagingReportBase,
    ):
        self.config = config
        self._glue = glue_client
        self.platform = platform
        self.report = report

    def get_schema_metadata(self, name: str) -> Optional[SchemaMetadataClass]:
        explicit = self.config.schema_map().get(name)
        schema_name = explicit or (name if self.config.use_naming_convention else None)
        if not schema_name:
            return None
        try:
            response = self._glue.get_schema_version(
                SchemaId={
                    "RegistryName": self.config.registry_name,
                    "SchemaName": schema_name,
                },
                SchemaVersionNumber={"LatestVersion": True},
            )
        except (ClientError, BotoCoreError) as e:
            code = aws_error_code(e)
            if code == GLUE_ENTITY_NOT_FOUND and explicit is None:
                # Expected for most names when probing by convention.
                logger.debug(f"No Glue schema named {schema_name} for {name}")
                self.report.schema_naming_convention_misses.append(name)
                return None
            self.report.schema_resolution_failures.append(f"{name}: {code}")
            self.report.warning(
                title="Glue schema fetch failed",
                message="glue:GetSchemaVersion failed; emitting without a schema.",
                context=f"{name}: schema={schema_name} registry={self.config.registry_name}: {code}",
                exc=e,
            )
            return None

        schema_def: str = response.get("SchemaDefinition", "")
        data_format: str = response.get("DataFormat", "").upper()
        try:
            fields = parse_glue_schema_fields(schema_def, data_format, name)
        except (
            Exception
        ) as e:  # schema parsers raise different exception types per format
            self.report.schema_resolution_failures.append(
                f"{name}: parse failed ({data_format})"
            )
            self.report.warning(
                title="Glue schema parse failed",
                message="The schema definition could not be parsed; emitting without a schema.",
                context=f"{name}: format={data_format}",
                exc=e,
            )
            return None
        if fields is None:
            self.report.schema_resolution_failures.append(
                f"{name}: unsupported format ({data_format})"
            )
            return None
        if not fields:
            # The protobuf converter returns no fields when compilation fails or
            # the schema is cyclic; an empty schema would read as "no columns".
            self.report.schema_resolution_failures.append(
                f"{name}: no fields parsed ({data_format})"
            )
            self.report.warning(
                title="Glue schema has no usable fields",
                message="The schema parsed to no fields, so none is attached.",
                context=f"{name}: schema={schema_name} format={data_format}",
            )
            return None
        self.report.schemas_attached += 1
        return SchemaMetadataClass(
            schemaName=name,
            platform=make_data_platform_urn(self.platform),
            version=0,
            hash="",
            platformSchema=KafkaSchemaClass(
                documentSchema=schema_def, documentSchemaType=data_format
            ),
            fields=fields,
        )
