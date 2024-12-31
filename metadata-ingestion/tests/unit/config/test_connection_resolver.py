from unittest.mock import MagicMock

import pydantic
import pytest

from datahub.configuration.common import ConfigModel
from datahub.configuration.connection_resolver import auto_connection_resolver
from datahub.ingestion.api.global_context import set_graph_context


class MyConnectionType(ConfigModel):
    username: str
    password: str

    _connection = auto_connection_resolver()


def test_auto_connection_resolver():
    # Test a normal config.
    config = MyConnectionType.parse_obj(
        {"username": "test_user", "password": "test_password"}
    )
    assert config.username == "test_user"
    assert config.password == "test_password"

    # No graph context -> should raise an error.
    with pytest.raises(pydantic.ValidationError, match=r"requires a .*graph"):
        config = MyConnectionType.parse_obj(
            {
                "connection": "test_connection",
            }
        )

    # Missing connection -> should raise an error.
    fake_graph = MagicMock()
    fake_graph.get_connection_json.return_value = None
    with set_graph_context(fake_graph):
        with pytest.raises(pydantic.ValidationError, match=r"not found"):
            config = MyConnectionType.parse_obj(
                {
                    "connection": "urn:li:dataHubConnection:missing-connection",
                }
            )

    # Bad connection config -> should raise an error.
    fake_graph.get_connection_json.return_value = {"bad_key": "bad_value"}
    with set_graph_context(fake_graph):
        with pytest.raises(pydantic.ValidationError):
            config = MyConnectionType.parse_obj(
                {
                    "connection": "urn:li:dataHubConnection:bad-connection",
                }
            )

    # Good connection config.
    fake_graph.get_connection_json.return_value = {
        "username": "test_user",
        "password": "test_password",
    }
    with set_graph_context(fake_graph):
        config = MyConnectionType.parse_obj(
            {
                "connection": "urn:li:dataHubConnection:good-connection",
                "username": "override_user",
            }
        )
        assert config.username == "override_user"
        assert config.password == "test_password"
