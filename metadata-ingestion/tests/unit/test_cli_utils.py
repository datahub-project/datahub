import os
from unittest import mock

from datahub.cli import cli_utils


def test_first_non_null():
    assert cli_utils.first_non_null([]) is None
    assert cli_utils.first_non_null([None]) is None
    assert cli_utils.first_non_null([None, "1"]) == "1"
    assert cli_utils.first_non_null([None, "1", "2"]) == "1"
    assert cli_utils.first_non_null(["3", "1", "2"]) == "3"
    assert cli_utils.first_non_null(["", "1", "2"]) == "1"
    assert cli_utils.first_non_null([" ", "1", "2"]) == "1"


@mock.patch.dict(os.environ, {"DATAHUB_GMS_HOST": "http://localhost:9092"})
def test_correct_url_when_gms_host_in_old_format():
    assert cli_utils.get_details_from_env() == ("http://localhost:9092", None)


@mock.patch.dict(
    os.environ, {"DATAHUB_GMS_HOST": "localhost", "DATAHUB_GMS_PORT": "8080"}
)
def test_correct_url_when_gms_host_and_port_set():
    assert cli_utils.get_details_from_env() == ("http://localhost:8080", None)


@mock.patch.dict(
    os.environ,
    {
        "DATAHUB_GMS_URL": "https://example.com",
        "DATAHUB_GMS_HOST": "localhost",
        "DATAHUB_GMS_PORT": "8080",
    },
)
def test_correct_url_when_gms_host_port_url_set():
    assert cli_utils.get_details_from_env() == ("http://localhost:8080", None)


@mock.patch.dict(
    os.environ,
    {
        "DATAHUB_GMS_URL": "https://example.com",
        "DATAHUB_GMS_HOST": "localhost",
        "DATAHUB_GMS_PORT": "8080",
        "DATAHUB_GMS_PROTOCOL": "https",
    },
)
def test_correct_url_when_gms_host_port_url_protocol_set():
    assert cli_utils.get_details_from_env() == ("https://localhost:8080", None)


@mock.patch.dict(
    os.environ,
    {
        "DATAHUB_GMS_URL": "https://example.com",
    },
)
def test_correct_url_when_url_set():
    assert cli_utils.get_details_from_env() == ("https://example.com", None)
