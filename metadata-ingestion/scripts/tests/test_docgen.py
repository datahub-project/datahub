import sys
from typing import Any, Dict, List, Optional, Union

import pydantic

sys.path.append(".")

from enum import Enum

from docs_config_table import FieldRow

from datahub.configuration.common import ConfigModel


class Platform(Enum):
    DBT = "DBT"
    LOOKER = "LOOKER"


class Connection(ConfigModel):
    path: Union[pydantic.FilePath, pydantic.DirectoryPath]
    headers: Dict[str, str]
    connect_args: Optional[Dict[str, Any]] = pydantic.Field(
        default=None,
        description="Connect args to pass to underlying driver",
        exclude=True,
    )


class BaseConfig(ConfigModel):
    """
    A base config class
    """

    env: str
    platform: Optional[Platform] = Platform.DBT


class FinalConfig(BaseConfig):
    connection: Connection
    field_array: List[str]
    connection_map: Dict[str, Connection]


def test_field_row():
    field_path = "[version=2.0].[type=FinalConfig].[type=map].[type=Connection].connection_map.[type=union].[type=string(file-path)].path"
    assert FieldRow.field_path_to_components(field_path) == [
        "connection_map",
        "`key`",
        "path",
    ]

    field_path = "[version=2.0].[type=FinalConfig].[type=map].[type=Connection].connection_map.[type=map].[type=string].headers"
    assert FieldRow.field_path_to_components(field_path) == [
        "connection_map",
        "`key`",
        "headers",
    ]

    field_path = "[version=2.0].[type=FinalConfig].[type=Connection].connection.[type=map].[type=string].headers"
    assert FieldRow.field_path_to_components(field_path) == ["connection", "headers"]

    field_path = "[version=2.0].[type=FinalConfig].[type=map].[type=Connection].connection_map.[type=union].[type=string(file-path)].path"
    assert FieldRow.field_path_to_components(field_path) == [
        "connection_map",
        "`key`",
        "path",
    ]

    field_path = "[version=2.0].[type=FinalConfig].[type=string].env"
    assert FieldRow.field_path_to_components(field_path) == ["env"]

    field_path = "[version=2.0].[type=FinalConfig].[type=map].[type=Connection].connection_map.[type=union].[type=string(file-path)].path"
    assert FieldRow.field_path_to_components(field_path) == [
        "connection_map",
        "`key`",
        "path",
    ]
