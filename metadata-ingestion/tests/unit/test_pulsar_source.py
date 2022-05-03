import unittest
from typing import Any, Dict
from unittest.mock import patch

import pytest

from datahub.configuration.common import ConfigurationError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.pulsar import (
    PulsarSchema,
    PulsarSource,
    PulsarSourceConfig,
    PulsarTopic,
)

mock_schema_response: Dict[str, Any] = {
    "version": 1,
    "type": "AVRO",
    "timestamp": 0,
    "data": '{"type":"record","name":"FooSchema","namespace":"foo.bar","doc":"Description of FooSchema","fields":[{"name":"field1","type":{"type":"string","avro.java.string":"String"},"doc":"Description of field1"},{"name":"field2","type":"long","doc":"Some description","default":0}]}',
    "properties": {"__jsr310ConversionEnabled": "false", "__alwaysAllowNull": "true"},
}


class TestPulsarSourceConfig:
    def test_pulsar_source_config_valid_web_service_url(self):
        assert (
            PulsarSourceConfig().web_service_url_scheme_host_port(
                "http://localhost:8080/"
            )
            == "http://localhost:8080"
        )

    def test_pulsar_source_config_invalid_web_service_url_scheme(self):
        with pytest.raises(
            ConfigurationError, match=r"Scheme should be http or https, found ftp"
        ):
            PulsarSourceConfig().web_service_url_scheme_host_port(
                "ftp://localhost:8080/"
            )

    def test_pulsar_source_config_invalid_web_service_url_host(self):
        with pytest.raises(
            ConfigurationError,
            match=r"Not a valid hostname, hostname contains invalid characters, found localhost&",
        ):
            PulsarSourceConfig().web_service_url_scheme_host_port(
                "http://localhost&:8080/"
            )


class TestPulsarTopic:
    def test_pulsar_source_parse_topic_string(self) -> None:
        topic = "persistent://tenant/namespace/topic"
        pulsar_topic = PulsarTopic(topic)
        assert pulsar_topic.type == "persistent"
        assert pulsar_topic.tenant == "tenant"
        assert pulsar_topic.namespace == "namespace"
        assert pulsar_topic.topic == "topic"
        assert pulsar_topic.fullname == "persistent://tenant/namespace/topic"


class TestPulsarSchema:
    def test_pulsar_source_parse_pulsar_schema(self) -> None:
        pulsar_schema = PulsarSchema(mock_schema_response)
        assert pulsar_schema.schema_type == "AVRO"
        assert (
            pulsar_schema.schema_str
            == '{"type":"record","name":"FooSchema","namespace":"foo.bar","doc":"Description of FooSchema","fields":[{"name":"field1","type":{"type":"string","avro.java.string":"String"},"doc":"Description of field1"},{"name":"field2","type":"long","doc":"Some description","default":0}]}'
        )
        assert pulsar_schema.schema_name == "foo.bar.FooSchema"
        assert pulsar_schema.schema_version == 1
        assert pulsar_schema.schema_description == "Description of FooSchema"
        assert pulsar_schema.properties == {
            "__jsr310ConversionEnabled": "false",
            "__alwaysAllowNull": "true",
        }


class TestPulsarSource(unittest.TestCase):
    def test_pulsar_source_get_token_jwt(self):
        ctx = PipelineContext(run_id="test")
        pulsar_source = PulsarSource.create(
            {"web_service_url": "http://localhost:8080", "token": "jwt_token"},
            ctx,
        )
        # source = PulsarSource(
        #    ctx=PipelineContext(run_id="pulsar-source-test"),
        #    config=self.token_config)
        assert pulsar_source.get_access_token() == "jwt_token"

    @patch("datahub.ingestion.source.pulsar.requests.get", autospec=True)
    @patch("datahub.ingestion.source.pulsar.requests.post", autospec=True)
    def test_pulsar_source_get_token_oauth(self, mock_post, mock_get):
        ctx = PipelineContext(run_id="test")
        mock_get.return_value.json.return_value = {
            "token_endpoint": "http://127.0.0.1:8083/realms/pulsar/protocol/openid-connect/token"
        }

        pulsar_source = PulsarSource.create(
            {
                "web_service_url": "http://localhost:8080",
                "issuer_url": "http://localhost:8083/realms/pulsar",
                "client_id": "client_id",
                "client_secret": "client_secret",
            },
            ctx,
        )
        mock_post.return_value.json.return_value = {"access_token": "oauth_token"}
        assert pulsar_source.get_access_token() == "oauth_token"

    @patch("datahub.ingestion.source.pulsar.requests.Session.get", autospec=True)
    def test_pulsar_source_get_workunits_all_tenant(self, mock_session):
        ctx = PipelineContext(run_id="test")
        pulsar_source = PulsarSource.create(
            {
                "web_service_url": "http://localhost:8080",
            },
            ctx,
        )

        # Mock fetching Pulsar metadata
        with patch(
            "datahub.ingestion.source.pulsar.PulsarSource._get_pulsar_metadata"
        ) as mock:
            mock.side_effect = [
                ["t_1"],  # tenant list
                ["t_1/ns_1"],  # namespaces list
                ["persistent://t_1/ns_1/topic_1"],  # persistent topic list
                [],  # persistent partitioned topic list
                [],  # none-persistent topic list
                [],  # none-persistent partitioned topic list
                mock_schema_response,
            ]  # schema for persistent://t_1/ns_1/topic

            work_units = list(pulsar_source.get_workunits())
            first_mcp = work_units[0].metadata
            assert isinstance(first_mcp, MetadataChangeProposalWrapper)

            # Expected calls 7
            # http://localhost:8080/admin/v2/tenants
            # http://localhost:8080/admin/v2/namespaces/t_1
            # http://localhost:8080/admin/v2/persistent/t_1/ns_1
            # http://localhost:8080/admin/v2/persistent/t_1/ns_1/partitioned
            # http://localhost:8080/admin/v2/non-persistent/t_1/ns_1
            # http://localhost:8080/admin/v2/non-persistent/t_1/ns_1/partitioned
            # http://localhost:8080/admin/v2/schemas/t_1/ns_1/topic_1/schema
            assert mock.call_count == 7
            # expecting 5 mcp for one topic with default config
            assert len(work_units) == 5

    @patch("datahub.ingestion.source.pulsar.requests.Session.get", autospec=True)
    def test_pulsar_source_get_workunits_custom_tenant(self, mock_session):
        ctx = PipelineContext(run_id="test")
        pulsar_source = PulsarSource.create(
            {
                "web_service_url": "http://localhost:8080",
                "tenants": ["t_1", "t_2"],
            },
            ctx,
        )

        # Mock fetching Pulsar metadata
        with patch(
            "datahub.ingestion.source.pulsar.PulsarSource._get_pulsar_metadata"
        ) as mock:
            mock.side_effect = [
                ["t_1/ns_1"],  # namespaces list
                ["persistent://t_1/ns_1/topic_1"],  # topic list
                [],  # empty persistent partitioned topic list
                [],  # empty none-persistent topic list
                [],  # empty none-persistent partitioned topic list
                mock_schema_response,  # schema for persistent://t_1/ns_1/topic
                [],  # no namespaces for tenant t_2
            ]

            work_units = list(pulsar_source.get_workunits())
            first_mcp = work_units[0].metadata
            assert isinstance(first_mcp, MetadataChangeProposalWrapper)

            # Expected calls 7
            # http://localhost:8080/admin/v2/namespaces/t_1
            # http://localhost:8080/admin/v2/persistent/t_1/ns_1
            # http://localhost:8080/admin/v2/persistent/t_1/ns_1/partitioned
            # http://localhost:8080/admin/v2/non-persistent/t_1/ns_1
            # http://localhost:8080/admin/v2/non-persistent/t_1/ns_1/partitioned
            # http://localhost:8080/admin/v2/schemas/t_1/ns_1/topic_1/schema
            # http://localhost:8080/admin/v2/namespaces/t_2
            assert mock.call_count == 7
            # expecting 5 mcp for one topic with default config
            assert len(work_units) == 5

    @patch("datahub.ingestion.source.pulsar.requests.Session.get", autospec=True)
    def test_pulsar_source_get_workunits_patterns(self, mock_session):
        ctx = PipelineContext(run_id="test")
        pulsar_source = PulsarSource.create(
            {
                "web_service_url": "http://localhost:8080",
                "tenants": ["t_1", "t_2", "bad_t_3"],
                "tenant_patterns": {"deny": ["bad_t_3"]},
                "namespace_patterns": {"allow": [r"t_1/ns_1"]},
                "topic_patterns": {"allow": [r"persistent://t_1/ns_1/topic_1"]},
            },
            ctx,
        )

        # Mock fetching Pulsar metadata
        with patch(
            "datahub.ingestion.source.pulsar.PulsarSource._get_pulsar_metadata"
        ) as mock:
            mock.side_effect = [
                ["t_1/ns_1", "t_2/ns_1"],  # namespaces list
                [
                    "persistent://t_1/ns_1/topic_1",  # persistent topic list
                    "non-persistent://t_1/ns_1/bad_topic",
                ],  # topic will be filtered out
                [],  # persistent partitioned topic list
                [],  # none-persistent topic list
                [],  # none-persistent partitioned topic list
                mock_schema_response,  # schema for persistent://t_1/ns_1/topic
                [],  # no namespaces for tenant t_2
            ]

            work_units = list(pulsar_source.get_workunits())
            first_mcp = work_units[0].metadata
            assert isinstance(first_mcp, MetadataChangeProposalWrapper)

            # Expected calls 7
            # http://localhost:8080/admin/v2/namespaces/t_1
            # http://localhost:8080/admin/v2/persistent/t_1/ns_1
            # http://localhost:8080/admin/v2/persistent/t_1/ns_1/partitioned
            # http://localhost:8080/admin/v2/non-persistent/t_1/ns_1
            # http://localhost:8080/admin/v2/non-persistent/t_1/ns_1/partitioned
            # http://localhost:8080/admin/v2/schemas/t_1/ns_1/topic_1/schema
            # http://localhost:8080/admin/v2/namespaces/t_2
            assert mock.call_count == 7
            # expecting 5 mcp for one topic with default config
            assert len(work_units) == 5
