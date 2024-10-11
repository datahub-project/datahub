import json
import logging
import re
from dataclasses import dataclass
from hashlib import md5
from typing import Iterable, List, Optional, Tuple

import requests

from datahub.configuration.common import ConfigurationError
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
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.extractor import schema_util
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source_config.pulsar import PulsarSourceConfig
from datahub.ingestion.source_report.pulsar import PulsarSourceReport
from datahub.metadata.com.linkedin.pegasus2avro.common import StatusClass
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    KafkaSchema,
    SchemaField,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    SubTypesClass,
)

logger = logging.getLogger(__name__)


class PulsarTopic:
    __slots__ = ["topic_parts", "fullname", "type", "tenant", "namespace", "topic"]

    def __init__(self, topic):
        topic_parts = re.split("[: /]", topic)
        self.fullname = topic
        self.type = topic_parts[0]
        self.tenant = topic_parts[3]
        self.namespace = topic_parts[4]
        self.topic = topic_parts[5]


class PulsarSchema:
    __slots__ = [
        "schema_version",
        "schema_name",
        "schema_description",
        "schema_type",
        "schema_str",
        "properties",
    ]

    def __init__(self, schema):
        self.schema_version = schema.get("version")

        avro_schema = json.loads(schema.get("data"))
        self.schema_name = avro_schema.get("namespace") + "." + avro_schema.get("name")
        self.schema_description = avro_schema.get("doc")
        self.schema_type = schema.get("type")
        self.schema_str = schema.get("data")
        self.properties = schema.get("properties")


@platform_name("Pulsar")
@support_status(SupportStatus.INCUBATING)
@config_class(PulsarSourceConfig)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@dataclass
class PulsarSource(StatefulIngestionSourceBase):
    def __init__(self, config: PulsarSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.platform: str = "pulsar"
        self.config: PulsarSourceConfig = config
        self.report: PulsarSourceReport = PulsarSourceReport()

        self.base_url: str = f"{self.config.web_service_url}/admin/v2"
        self.tenants: List[str] = config.tenants

        self.session = requests.Session()
        self.session.verify = self.config.verify_ssl
        self.session.headers.update(
            {
                "Content-Type": "application/json",
            }
        )

        if self._is_oauth_authentication_configured():
            # Get OpenId configuration from issuer, e.g. token_endpoint
            oid_config_url = (
                f"{self.config.issuer_url}/.well-known/openid-configuration"
            )
            oid_config_response = requests.get(
                oid_config_url, verify=self.session.verify, allow_redirects=False
            )

            if oid_config_response:
                self.config.oid_config.update(oid_config_response.json())
            else:
                logger.error(
                    f"Unexpected response while getting discovery document using {oid_config_url} : {oid_config_response}"
                )

            if "token_endpoint" not in self.config.oid_config:
                raise Exception(
                    "The token_endpoint is not set, please verify the configured issuer_url or"
                    " set oid_config.token_endpoint manually in the configuration file."
                )

        # Authentication configured
        if (
            self._is_token_authentication_configured()
            or self._is_oauth_authentication_configured()
        ):
            # Update session header with Bearer token
            self.session.headers.update(
                {"Authorization": f"Bearer {self.get_access_token()}"}
            )

    def get_access_token(self) -> str:
        """
        Returns an access token used for authentication, token comes from config or third party provider
        when issuer_url is provided
        """
        # JWT, get access token (jwt) from config
        if self._is_token_authentication_configured():
            return str(self.config.token)

        # OAuth, connect to issuer and return access token
        if self._is_oauth_authentication_configured():
            assert self.config.client_id
            assert self.config.client_secret
            data = {"grant_type": "client_credentials"}
            try:
                # Get a token from the issuer
                token_endpoint = self.config.oid_config["token_endpoint"]
                logger.info(f"Request access token from {token_endpoint}")
                token_response = requests.post(
                    url=token_endpoint,
                    data=data,
                    verify=self.session.verify,
                    allow_redirects=False,
                    auth=(
                        self.config.client_id,
                        self.config.client_secret,
                    ),
                )
                token_response.raise_for_status()

                return token_response.json()["access_token"]

            except requests.exceptions.RequestException as e:
                logger.error(f"An error occurred while handling your request: {e}")
        # Failed to get an access token,
        raise ConfigurationError(
            f"Failed to get the Pulsar access token from token_endpoint {self.config.oid_config.get('token_endpoint')}."
            f" Please check your input configuration."
        )

    def _get_pulsar_metadata(self, url):
        """
        Interacts with the Pulsar Admin Api and returns Pulsar metadata. Invocations with insufficient privileges
        are logged.
        """
        try:
            # Request the Pulsar metadata
            response = self.session.get(url, timeout=self.config.timeout)
            response.raise_for_status()
            # Return the response for status_code 200
            return response.json()

        except requests.exceptions.HTTPError as http_error:
            # Topics can exist without a schema, log the warning and move on
            if http_error.response.status_code == 404 and "/schemas/" in url:
                message = (
                    f"Failed to get schema from schema registry. The topic is either schema-less or"
                    f" no messages have been written to the topic yet."
                    f" {http_error}"
                )
                self.report.report_warning("NoSchemaFound", message)
            else:
                # Authorization error
                message = f"An HTTP error occurred: {http_error}"
                self.report.report_warning("HTTPError", message)
        except requests.exceptions.RequestException as e:
            raise Exception(
                f"An ambiguous exception occurred while handling the request: {e}"
            )

    @classmethod
    def create(cls, config_dict, ctx):
        config = PulsarSourceConfig.parse_obj(config_dict)

        # Do not include each individual partition for partitioned topics,
        if config.exclude_individual_partitions:
            config.topic_patterns.deny.append(r".*-partition-[0-9]+")

        return cls(config, ctx)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """
        Interacts with the Pulsar Admin Api and loops over tenants, namespaces and topics. For every topic
        the schema information is retrieved if available.

        Pulsar web service admin rest api urls for retrieving topic information
            - [web_service_url]/admin/v2/persistent/{tenant}/{namespace}
            - [web_service_url]/admin/v2/persistent/{tenant}/{namespace}/partitioned
            - [web_service_url]/admin/v2/non-persistent/{tenant}/{namespace}
            - [web_service_url]/admin/v2/non-persistent/{tenant}/{namespace}/partitioned
        """
        topic_urls = [
            self.base_url + "/persistent/{}",
            self.base_url + "/persistent/{}/partitioned",
            self.base_url + "/non-persistent/{}",
            self.base_url + "/non-persistent/{}/partitioned",
        ]

        # Report the Pulsar broker version we are communicating with
        self.report.report_pulsar_version(
            self.session.get(
                f"{self.base_url}/brokers/version", timeout=self.config.timeout
            ).text
        )

        # If no tenants are provided, request all tenants from cluster using /admin/v2/tenants endpoint.
        # Requesting cluster tenant information requires superuser privileges
        if not self.tenants:
            self.tenants = self._get_pulsar_metadata(f"{self.base_url}/tenants") or []

        # Initialize counters
        self.report.tenants_scanned = 0
        self.report.namespaces_scanned = 0
        self.report.topics_scanned = 0

        for tenant in self.tenants:
            self.report.tenants_scanned += 1
            if self.config.tenant_patterns.allowed(tenant):
                # Get namespaces belonging to a tenant, /admin/v2/%s/namespaces
                # A tenant admin role has sufficient privileges to perform this action
                namespaces = (
                    self._get_pulsar_metadata(f"{self.base_url}/namespaces/{tenant}")
                    or []
                )

                for namespace in namespaces:
                    self.report.namespaces_scanned += 1
                    if self.config.namespace_patterns.allowed(namespace):
                        # Get all topics (persistent, non-persistent and partitioned) belonging to a tenant/namespace
                        # Four endpoint invocations are needs to get all topic metadata for a namespace
                        topics = {}
                        for url in topic_urls:
                            # Topics are partitioned when admin url ends with /partitioned
                            partitioned = url.endswith("/partitioned")
                            # Get the topics for each type
                            pulsar_topics = (
                                self._get_pulsar_metadata(url.format(namespace)) or []
                            )
                            # Create a mesh of topics with partitioned values, the
                            # partitioned info is added as a custom properties later
                            topics.update(
                                {topic: partitioned for topic in pulsar_topics}
                            )

                        # For all allowed topics get the metadata
                        for topic, is_partitioned in topics.items():
                            self.report.topics_scanned += 1
                            if self.config.topic_patterns.allowed(topic):
                                yield from self._extract_record(topic, is_partitioned)
                            else:
                                self.report.report_topics_dropped(topic)
                    else:
                        self.report.report_namespaces_dropped(namespace)
            else:
                self.report.report_tenants_dropped(tenant)

    def _is_token_authentication_configured(self) -> bool:
        return self.config.token is not None

    def _is_oauth_authentication_configured(self) -> bool:
        return self.config.issuer_url is not None

    def _get_schema_and_fields(
        self, pulsar_topic: PulsarTopic, is_key_schema: bool
    ) -> Tuple[Optional[PulsarSchema], List[SchemaField]]:
        pulsar_schema: Optional[PulsarSchema] = None

        schema_url = (
            self.base_url
            + f"/schemas/{pulsar_topic.tenant}/{pulsar_topic.namespace}/{pulsar_topic.topic}/schema"
        )

        schema_payload = self._get_pulsar_metadata(schema_url)

        # Get the type and schema from the Pulsar Schema
        if schema_payload is not None:
            # pulsar_schema: Optional[PulsarSchema] = None
            pulsar_schema = PulsarSchema(schema_payload)

        # Obtain the schema fields from schema for the topic.
        fields: List[SchemaField] = []
        if pulsar_schema is not None:
            fields = self._get_schema_fields(
                pulsar_topic=pulsar_topic,
                schema=pulsar_schema,
                is_key_schema=is_key_schema,
            )
        return pulsar_schema, fields

    def _get_schema_fields(
        self, pulsar_topic: PulsarTopic, schema: PulsarSchema, is_key_schema: bool
    ) -> List[SchemaField]:
        # Parse the schema and convert it to SchemaFields.
        fields: List[SchemaField] = []
        if schema.schema_type in ["AVRO", "JSON"]:
            # Extract fields from schema and get the FQN for the schema
            fields = schema_util.avro_schema_to_mce_fields(
                schema.schema_str, is_key_schema=is_key_schema
            )
        else:
            self.report.report_warning(
                pulsar_topic.fullname,
                f"Parsing Pulsar schema type {schema.schema_type} is currently not implemented",
            )
        return fields

    def _get_schema_metadata(
        self, pulsar_topic: PulsarTopic, platform_urn: str
    ) -> Tuple[Optional[PulsarSchema], Optional[SchemaMetadata]]:
        # FIXME: Type annotations are not working for this function.
        schema, fields = self._get_schema_and_fields(
            pulsar_topic=pulsar_topic, is_key_schema=False
        )  # type: Tuple[Optional[PulsarSchema], List[SchemaField]]

        # Create the schemaMetadata aspect.
        if schema is not None:
            md5_hash = md5(schema.schema_str.encode()).hexdigest()

            return schema, SchemaMetadata(
                schemaName=schema.schema_name,
                version=schema.schema_version,
                hash=md5_hash,
                platform=platform_urn,
                platformSchema=KafkaSchema(
                    documentSchema=schema.schema_str if schema else "",
                    documentSchemaType=schema.schema_type if schema else None,
                    keySchema=None,
                    keySchemaType=None,
                ),
                fields=fields,
            )
        return None, None

    def _extract_record(
        self, topic: str, partitioned: bool
    ) -> Iterable[MetadataWorkUnit]:
        logger.info(f"topic = {topic}")

        # 1. Create and emit the default dataset for the topic. Extract type, tenant, namespace
        # and topic name from full Pulsar topic name i.e. persistent://tenant/namespace/topic
        pulsar_topic = PulsarTopic(topic)

        platform_urn = make_data_platform_urn(self.platform)
        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=pulsar_topic.fullname,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

        # 2. Emit schemaMetadata aspect
        schema, schema_metadata = self._get_schema_metadata(pulsar_topic, platform_urn)
        if schema_metadata is not None:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=schema_metadata,
            ).as_workunit()

        # TODO Add topic properties (Pulsar 2.10.0 feature)
        # 3. Construct and emit dataset properties aspect
        if schema is not None:
            # Add some static properties to the schema properties
            schema_properties = {
                **schema.properties,
                "schema_version": str(schema.schema_version),
                "schema_type": schema.schema_type,
                "partitioned": str(partitioned).lower(),
            }

            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=DatasetPropertiesClass(
                    description=schema.schema_description,
                    customProperties=schema_properties,
                ),
            ).as_workunit()

        # 4. Emit browsePaths aspect
        pulsar_path = (
            f"{pulsar_topic.tenant}/{pulsar_topic.namespace}/{pulsar_topic.topic}"
        )
        browse_path_suffix = (
            f"{self.config.platform_instance}/{pulsar_path}"
            if self.config.platform_instance
            else pulsar_path
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=BrowsePathsClass(
                [f"/{self.config.env.lower()}/{self.platform}/{browse_path_suffix}"]
            ),
        ).as_workunit()

        # 5. Emit dataPlatformInstance aspect.
        if self.config.platform_instance:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=DataPlatformInstanceClass(
                    platform=platform_urn,
                    instance=make_dataplatform_instance_urn(
                        self.platform, self.config.platform_instance
                    ),
                ),
            ).as_workunit()

        # 6. Emit subtype aspect marking this as a "topic"
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypesClass(typeNames=[DatasetSubTypes.TOPIC]),
        ).as_workunit()

        # 7. Emit domains aspect
        domain_urn: Optional[str] = None
        for domain, pattern in self.config.domain.items():
            if pattern.allowed(pulsar_topic.fullname):
                domain_urn = make_domain_urn(domain)

        if domain_urn:
            yield from add_domain_to_entity_wu(
                entity_urn=dataset_urn,
                domain_urn=domain_urn,
            )

    def get_report(self):
        return self.report

    def close(self):
        super().close()
        self.session.close()
