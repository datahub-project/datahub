"""Glue Schema Registry resolver and Avro/JSON/Protobuf parser.

Pattern adapted from confluent_schema_registry.py:
  1. Explicit stream_schema_map override wins
  2. Naming convention fallback (stream_name == schema_name)

Schema parsing reuses the platform-agnostic utilities under
datahub.ingestion.extractor.{schema_util, json_schema_util, protobuf_util}.
"""

import json
import logging
from typing import TYPE_CHECKING, List, Optional

from botocore.exceptions import BotoCoreError, ClientError

from datahub.ingestion.extractor import schema_util
from datahub.ingestion.extractor.json_schema_util import JsonSchemaTranslator
from datahub.ingestion.source.kinesis.kinesis_aws_utils import aws_error_code
from datahub.ingestion.source.kinesis.kinesis_config import (
    KinesisGlueSchemaRegistryConfig,
)
from datahub.ingestion.source.kinesis.kinesis_report import KinesisSourceReport
from datahub.metadata.schema_classes import (
    KafkaSchemaClass,
    SchemaFieldClass,
    SchemaMetadataClass,
)

if TYPE_CHECKING:
    from mypy_boto3_glue import GlueClient

logger = logging.getLogger(__name__)


class KinesisGlueSchemaRegistry:
    """Fetch schemas from AWS Glue Schema Registry and convert to DataHub SchemaMetadata."""

    def __init__(
        self,
        config: KinesisGlueSchemaRegistryConfig,
        glue_client: "GlueClient",
        platform_urn: str,
        report: KinesisSourceReport,
    ):
        self.config = config
        self._glue = glue_client
        self.platform_urn = platform_urn
        self.report = report

    def _resolve_schema_name(self, stream_name: str) -> Optional[str]:
        if stream_name in self.config.stream_schema_map:
            return self.config.stream_schema_map[stream_name]
        if self.config.use_naming_convention:
            return stream_name
        return None

    def get_schema_metadata(self, stream_name: str) -> Optional[SchemaMetadataClass]:
        if not self.config.enabled:
            return None
        schema_name = self._resolve_schema_name(stream_name)
        if not schema_name:
            return None
        # Distinguish a user-declared mapping from a naming-convention probe: a miss
        # on an explicit mapping is a real problem (user said the schema is there);
        # a miss on a naming-convention probe is the expected outcome for streams
        # without GSR schemas.
        is_explicit_mapping = stream_name in self.config.stream_schema_map
        try:
            resp = self._glue.get_schema_version(
                SchemaId={
                    "RegistryName": self.config.registry_name,
                    "SchemaName": schema_name,
                },
                SchemaVersionNumber={"LatestVersion": True},
            )
        except (ClientError, BotoCoreError) as e:
            # Widened catch (was `ClientError` only) for consistency with the
            # other six boto3 callsites in the connector. BotoCoreError
            # subclasses — NoCredentialsError, EndpointConnectionError,
            # ConnectTimeoutError, etc. — would otherwise crash the entire
            # KDS extraction loop instead of degrading to a per-stream
            # warning. The EntityNotFoundException naming-convention branch
            # below is naturally ClientError-only: aws_error_code() returns
            # exception class names for BotoCoreError subclasses (e.g.
            # "NoCredentialsError"), which can't match the
            # "EntityNotFoundException" string, so those errors fall
            # through to the warning path as intended.
            code = aws_error_code(e)
            if code == "EntityNotFoundException" and not is_explicit_mapping:
                # Expected outcome for streams that don't have GSR schemas. Log at
                # DEBUG (full info is one report-field lookup away) and tally in a
                # separate counter so users can see "naming convention missed for N
                # streams" without WARNING noise.
                logger.debug(
                    f"GSR naming-convention probe: no schema named '{schema_name}' in "
                    f"registry '{self.config.registry_name}' for stream '{stream_name}'."
                )
                self.report.report_gsr_naming_convention_miss(stream_name)
                return None
            # Real failure: AccessDenied, ValidationException, throttling, or a
            # user-declared mapping that doesn't resolve. These deserve a warning.
            self.report.warning(
                title="GSR Schema Fetch Failed",
                message=(
                    f"glue:GetSchemaVersion returned {code}. Emitting stream without schema. "
                    f"Verify glue:GetSchemaVersion permission and that schema exists in registry."
                ),
                context=f"stream={stream_name} schema={schema_name} registry={self.config.registry_name}",
                exc=e,
            )
            self.report.report_schema_resolution_failure(stream_name, code)
            return None

        schema_def: str = resp.get("SchemaDefinition", "")
        data_format: str = resp.get("DataFormat", "").upper()

        fields = self._parse_fields(schema_def, data_format, stream_name)
        if fields is None:
            return None

        platform_schema = KafkaSchemaClass(
            documentSchema=schema_def, documentSchemaType=data_format
        )
        return SchemaMetadataClass(
            schemaName=stream_name,
            platform=self.platform_urn,
            version=0,
            hash="",  # md5 hash optional; populating it requires hashlib import
            platformSchema=platform_schema,
            fields=fields,
        )

    def _parse_fields(
        self, schema_def: str, data_format: str, stream_name: str
    ) -> Optional[List[SchemaFieldClass]]:
        try:
            if data_format == "AVRO":
                # swallow_exceptions=False so parse errors reach our handler
                # below and get recorded as parse-failed in the report.
                return schema_util.avro_schema_to_mce_fields(
                    schema_def, is_key_schema=False, swallow_exceptions=False
                )
            if data_format == "JSON":
                return list(
                    JsonSchemaTranslator.get_fields_from_schema(
                        json.loads(schema_def),
                        is_key_schema=False,
                    )
                )
            if data_format == "PROTOBUF":
                # Lazy import: protobuf_util pulls in grpc/networkx/protobuf,
                # which are not part of the kinesis extras (aws_common).
                # Importing here keeps the base kinesis install small.
                from datahub.ingestion.extractor import protobuf_util
                from datahub.ingestion.extractor.protobuf_util import (
                    ProtobufSchema,
                )

                return protobuf_util.protobuf_schema_to_mce_fields(
                    ProtobufSchema(f"{stream_name}.proto", schema_def),
                    imported_schemas=[],
                    is_key_schema=False,
                )
        except (
            Exception
        ) as e:  # schema parsers raise heterogeneous exception types per format
            self.report.warning(
                title="Schema Parse Failed",
                message=(
                    f"Could not parse {data_format} schema. Check the schema definition "
                    f"is valid {data_format}. Stream will be emitted without schemaMetadata."
                ),
                context=f"stream={stream_name} format={data_format}",
                exc=e,
            )
            self.report.report_schema_resolution_failure(
                stream_name, f"parse-failed ({data_format})"
            )
            return None
        self.report.warning(
            title="Unsupported Schema Format",
            message=(
                f"DataFormat {data_format!r} is not supported (V1 supports AVRO, JSON, PROTOBUF). "
                f"Stream emitted without schemaMetadata."
            ),
            context=f"stream={stream_name} format={data_format}",
        )
        self.report.report_schema_resolution_failure(
            stream_name, f"unsupported-format ({data_format})"
        )
        return None
