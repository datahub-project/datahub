import logging
import pathlib
import shutil
import ssl
import tempfile
import time
from unittest.mock import patch

import pytest

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.cassandra.cassandra_api import CassandraAPI
from datahub.ingestion.source.cassandra.cassandra_config import CassandraSourceConfig
from datahub.testing import mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port

logger = logging.getLogger(__name__)

_resources_dir = pathlib.Path(__file__).parent

pytestmark = pytest.mark.integration_batch_4


@pytest.mark.integration
def test_cassandra_ingest(docker_compose_runner, pytestconfig, tmp_path, monkeypatch):
    # Tricky: The cassandra container makes modifications directly to the cassandra.yaml
    # config file.
    # See https://github.com/docker-library/cassandra/issues/165
    # To avoid spurious diffs, we copy the config file to a temporary location
    # and depend on that instead. The docker-compose file has the corresponding
    # env variable usage to pick up the config file.
    cassandra_config_file = _resources_dir / "setup/cassandra.yaml"
    shutil.copy(cassandra_config_file, tmp_path / "cassandra.yaml")
    monkeypatch.setenv("CASSANDRA_CONFIG_DIR", str(tmp_path))

    with docker_compose_runner(
        _resources_dir / "docker-compose.yml", "cassandra"
    ) as docker_services:
        wait_for_port(docker_services, "test-cassandra", 9042)

        time.sleep(5)

        # Run the metadata ingestion pipeline.
        logger.info("Starting the ingestion test...")
        pipeline = Pipeline.create(
            {
                "run_id": "cassandra-test",
                "source": {
                    "type": "cassandra",
                    "config": {
                        "platform_instance": "dev_instance",
                        "contact_point": "localhost",
                        "port": 9042,
                        "profiling": {"enabled": True},
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/cassandra_mcps.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        # Verify the output.
        logger.info("Verifying output.")
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=f"{tmp_path}/cassandra_mcps.json",
            golden_path=_resources_dir / "cassandra_mcps_golden.json",
        )


@pytest.mark.integration
def test_cassandra_ssl_configuration():
    """Test SSL configuration and context creation with different SSL versions using mocking."""
    from datahub.ingestion.api.source import SourceReport

    # Test different SSL versions
    ssl_versions = ["TLS_CLIENT", "TLSv1", "TLSv1_1", "TLSv1_2", "TLSv1_3"]

    for ssl_version in ssl_versions:
        config = CassandraSourceConfig(
            contact_point="localhost",
            port=9042,
            ssl_ca_certs="/tmp/test_ca.crt",
            ssl_version=ssl_version,
        )

        report = SourceReport()
        api = CassandraAPI(config, report)

        # Mock the SSL context creation to avoid file system dependencies
        with patch("ssl.SSLContext") as mock_ssl_context:
            mock_context = mock_ssl_context.return_value
            mock_context.load_verify_locations.return_value = None

            # Test that SSL context is created with correct protocol
            try:
                # This will fail at connection time, but SSL context creation should work
                api.authenticate()
            except Exception:
                # Expected to fail since we're not actually connecting
                pass

            # Verify SSL context was created with the correct protocol
            expected_protocol = {
                "TLS_CLIENT": ssl.PROTOCOL_TLS_CLIENT,
                "TLSv1": ssl.PROTOCOL_TLSv1,
                "TLSv1_1": ssl.PROTOCOL_TLSv1_1,
                "TLSv1_2": ssl.PROTOCOL_TLSv1_2,
                "TLSv1_3": ssl.PROTOCOL_TLSv1_2,  # Python's ssl module uses TLSv1_2 for TLS 1.3
            }[ssl_version]

            mock_ssl_context.assert_called_with(expected_protocol)


@pytest.mark.integration
def test_cassandra_ssl_certificate_validation():
    """Test SSL certificate validation and error handling."""
    from datahub.ingestion.api.source import SourceReport

    # Create temporary certificate files for testing
    with tempfile.NamedTemporaryFile(mode="w", suffix=".crt", delete=False) as ca_cert:
        ca_cert.write(
            "-----BEGIN CERTIFICATE-----\nMOCK_CA_CERT\n-----END CERTIFICATE-----\n"
        )
        ca_cert_path = ca_cert.name

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".crt", delete=False
    ) as client_cert:
        client_cert.write(
            "-----BEGIN CERTIFICATE-----\nMOCK_CLIENT_CERT\n-----END CERTIFICATE-----\n"
        )
        client_cert_path = client_cert.name

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".key", delete=False
    ) as client_key:
        client_key.write(
            "-----BEGIN PRIVATE KEY-----\nMOCK_CLIENT_KEY\n-----END PRIVATE KEY-----\n"
        )
        client_key_path = client_key.name

    try:
        config = CassandraSourceConfig(
            contact_point="localhost",
            port=9042,
            ssl_ca_certs=ca_cert_path,
            ssl_certfile=client_cert_path,
            ssl_keyfile=client_key_path,
            ssl_version="TLS_CLIENT",
        )

        report = SourceReport()
        api = CassandraAPI(config, report)

        # Mock the SSL context to avoid actual file loading
        with patch("ssl.SSLContext") as mock_ssl_context:
            mock_context = mock_ssl_context.return_value
            mock_context.load_verify_locations.return_value = None
            mock_context.load_cert_chain.return_value = None

            # Test that SSL context is created and certificates are loaded
            try:
                api.authenticate()
            except Exception:
                # Expected to fail since we're not actually connecting
                pass

            # Verify SSL context methods were called correctly
            mock_context.load_verify_locations.assert_called_with(ca_cert_path)
            mock_context.load_cert_chain.assert_called_with(
                certfile=client_cert_path,
                keyfile=client_key_path,
            )

    finally:
        # Clean up temporary files
        import os

        for file_path in [ca_cert_path, client_cert_path, client_key_path]:
            try:
                os.unlink(file_path)
            except OSError:
                pass


@pytest.mark.integration
def test_cassandra_ssl_invalid_certificate_error():
    """Test SSL connection with invalid certificates to ensure proper error handling."""
    from datahub.ingestion.api.source import SourceReport

    config = CassandraSourceConfig(
        contact_point="localhost",
        port=9042,
        ssl_ca_certs="/nonexistent/ca.crt",  # Invalid path
        ssl_version="TLS_CLIENT",
    )

    report = SourceReport()
    api = CassandraAPI(config, report)

    # Test that authentication fails gracefully with invalid certificate path
    result = api.authenticate()
    assert result is False
    assert len(report.failures) > 0
    assert "Failed to authenticate" in str(report.failures[0])


@pytest.mark.integration
def test_cassandra_ssl_missing_certificate_file_error():
    """Test SSL configuration with missing certificate file."""
    from datahub.ingestion.api.source import SourceReport

    config = CassandraSourceConfig(
        contact_point="localhost",
        port=9042,
        ssl_ca_certs="/tmp/test_ca.crt",  # Need ssl_ca_certs to trigger SSL context creation
        ssl_certfile="/nonexistent/client.crt",  # Only certfile, no keyfile
        ssl_version="TLS_CLIENT",
    )

    report = SourceReport()
    api = CassandraAPI(config, report)

    # Mock the SSL context creation to avoid connection attempts
    with patch("ssl.SSLContext") as mock_ssl_context:
        mock_context = mock_ssl_context.return_value
        mock_context.load_verify_locations.return_value = None

        # Test that authentication fails with proper error message
        result = api.authenticate()
        assert result is False
        assert len(report.failures) > 0
        # Should fail because both certfile and keyfile must be provided
        assert "Both ssl_certfile and ssl_keyfile must be provided" in str(
            report.failures[0]
        )


@pytest.mark.integration
def test_cassandra_ssl_missing_keyfile_error():
    """Test SSL configuration with missing key file."""
    from datahub.ingestion.api.source import SourceReport

    config = CassandraSourceConfig(
        contact_point="localhost",
        port=9042,
        ssl_ca_certs="/tmp/test_ca.crt",  # Need ssl_ca_certs to trigger SSL context creation
        ssl_keyfile="/nonexistent/client.key",  # Only keyfile, no certfile
        ssl_version="TLS_CLIENT",
    )

    report = SourceReport()
    api = CassandraAPI(config, report)

    # Mock the SSL context creation to avoid connection attempts
    with patch("ssl.SSLContext") as mock_ssl_context:
        mock_context = mock_ssl_context.return_value
        mock_context.load_verify_locations.return_value = None

        # Test that authentication fails with proper error message
        result = api.authenticate()
        assert result is False
        assert len(report.failures) > 0
        # Should fail because both certfile and keyfile must be provided
        assert "Both ssl_certfile and ssl_keyfile must be provided" in str(
            report.failures[0]
        )
