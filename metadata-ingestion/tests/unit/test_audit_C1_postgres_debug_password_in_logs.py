import logging
from io import StringIO
from typing import Iterator, Tuple
from unittest.mock import patch
from urllib.parse import quote

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.postgres import PostgresConfig, PostgresSource
from datahub.masking.masking_filter import install_masking_filter
from datahub.masking.secret_registry import SecretRegistry
from tests.test_helpers.masking_state_helpers import reset_masking_process_state

SOURCE_LOGGER = "datahub.ingestion.source.sql.postgres.source"


@pytest.fixture
def masked_logs() -> Iterator[Tuple[logging.Logger, StringIO]]:
    reset_masking_process_state()
    stream = StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter("%(message)s"))
    source_logger = logging.getLogger(SOURCE_LOGGER)
    source_logger.setLevel(logging.DEBUG)
    source_logger.addHandler(handler)
    install_masking_filter(
        secret_registry=SecretRegistry.get_instance(), install_stdout_wrapper=False
    )
    try:
        yield source_logger, stream
    finally:
        source_logger.removeHandler(handler)
        reset_masking_process_state()


def _run_get_inspectors(config_dict: dict) -> None:
    config = PostgresConfig.model_validate(config_dict)
    with patch(
        "datahub.ingestion.source.sql.postgres.source.create_engine"
    ) as create_engine_mock:
        execute_mock = create_engine_mock.return_value.connect.return_value.__enter__.return_value.execute
        execute_mock.return_value = []
        source = PostgresSource(config, PipelineContext(run_id="audit-c1"))
        list(source.get_inspectors())


def test_simple_password_is_masked(masked_logs) -> None:
    _, stream = masked_logs
    password = "alphanumericpassword123"
    _run_get_inspectors(
        {
            "username": "my_user",
            "password": password,
            "host_port": "my-host:5432",
            "database": "my_db",
        }
    )
    output = stream.getvalue()
    assert "sql_alchemy_url=" in output, output
    assert password not in output, output


def test_password_with_special_characters_is_not_leaked(masked_logs) -> None:
    _, stream = masked_logs
    password = "Str0ng!Pass#2024"
    _run_get_inspectors(
        {
            "username": "my_user",
            "password": password,
            "host_port": "my-host:5432",
            "database": "my_db",
        }
    )
    output = stream.getvalue()
    assert "sql_alchemy_url=" in output, output
    rendered = quote(password, safe=" +")
    assert rendered not in output, (
        f"password rendering {rendered!r} reached the debug log: {output!r}"
    )
    assert password not in output, output


def test_password_in_sqlalchemy_uri_is_not_leaked(masked_logs) -> None:
    _, stream = masked_logs
    password = "placeholder-uri-secret-42"
    _run_get_inspectors(
        {
            "sqlalchemy_uri": f"postgresql+psycopg2://my_user:{password}@my-host:5432/my_db",
            "host_port": "my-host:5432",
        }
    )
    output = stream.getvalue()
    assert "sql_alchemy_url=" in output, output
    rendered = quote(password, safe=" +")
    assert rendered not in output, (
        f"password rendering {rendered!r} reached the debug log: {output!r}"
    )
    assert password not in output, output
