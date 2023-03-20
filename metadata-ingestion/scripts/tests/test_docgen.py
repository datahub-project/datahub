import sys
from typing import Any, Dict, List, Optional, Union

import pydantic
 
# setting path
sys.path.append('../../scripts')

from docgen import gen_md_table_from_struct, FieldRow, FieldTree
from datahub.configuration.common import ConfigModel
from enum import Enum

class Platform(Enum):
    DBT="DBT"
    LOOKER="LOOKER"

class Connection(ConfigModel):
    path: Union[pydantic.FilePath, pydantic.DirectoryPath]
    headers: Dict[str, str]
    connect_args: Optional[Dict[str, Any]] = pydantic.Field(
        default=None,
        description="Connect args to pass to underlying driver",
        exclude=True,
    )

class BaseConfig(ConfigModel):
    '''
    A base config class
    '''
    env: str
    platform: Optional[Platform] = Platform.DBT

class FinalConfig(BaseConfig):
    connection: Connection
    field_array: List[str]
    connection_map: Dict[str, Connection]
        
def test_nested_config():
    schema = FinalConfig.schema()
    generated_markdown = gen_md_table_from_struct(schema)
    assert generated_markdown == [
        '| Field [Required] | Type | Description | Default | Notes |\n| ---   | ---  | --- | -- | -- |\n',
        '| env [✅] | string |  | None |   |\n',
        '| connection [✅] | Connection |  | None |   |\n',
        '| connection.headers [❓ (required if connection is set)] | map(str,string) |  | None |   |\n',
        '| connection.path [❓ (required if connection is set)] | UnionType (See notes for variants) |  | None | One of string(file-path),string(directory-path) |\n',
        '| connection.connect_args  | object | Connect args to pass to underlying driver | None |   |\n',
        '| connection_map [✅] | map(str,Connection) |  | None |   |\n',
        '| connection_map.`key`.path [❓ (required if connection_map is set)] | UnionType (See notes for variants) |  | None | One of string(file-path),string(directory-path) |\n',
        '| connection_map.`key`.headers [❓ (required if connection_map is set)] | map(str,string) |  | None |   |\n',
        '| connection_map.`key`.connect_args  | object | Connect args to pass to underlying driver | None |   |\n',
        '| field_array  | array(string) |  | None |   |\n',
        '| platform  | Enum |  | DBT |   |\n'
    ]

class DuplicateConfig(ConfigModel):
    final: Optional[FinalConfig]
    connection: Optional[Connection]

def test_duplicate_config():
    schema = DuplicateConfig.schema()
    generated_markdown = gen_md_table_from_struct(schema)
    assert generated_markdown == [
         '| Field [Required] | Type | Description | Default | Notes |\n| ---   | ---  | --- | -- | -- |\n',
         '| connection  | Connection |  | None |   |\n',
         '| connection.headers [❓ (required if connection is set)] | map(str,string) |  | None |   |\n',
         '| connection.path [❓ (required if connection is set)] | UnionType (See notes for variants) |  | None | One of string(file-path),string(directory-path) |\n',
         '| connection.connect_args  | object | Connect args to pass to underlying driver | None |   |\n',
         '| final  | FinalConfig | A base config class | None |   |\n',
         '| final.env [❓ (required if final is set)] | string |  | None |   |\n',
         '| final.connection [❓ (required if final is set)] | Connection |  | None |   |\n',
         '| final.connection.headers [❓ (required if connection is set)] | map(str,string) |  | None |   |\n',
        '| final.connection.path [❓ (required if connection is set)] | UnionType (See notes for variants) |  | None | One of string(file-path),string(directory-path) |\n',
        '| final.connection.connect_args  | object | Connect args to pass to underlying driver | None |   |\n',
        '| final.connection_map [❓ (required if final is set)] | map(str,Connection) |  | None |   |\n',
        '| final.connection_map.`key`.path [❓ (required if connection_map is set)] | UnionType (See notes for variants) |  | None | One of string(file-path),string(directory-path) |\n',
        '| final.connection_map.`key`.headers [❓ (required if connection_map is set)] | map(str,string) |  | None |   |\n',
        '| final.connection_map.`key`.connect_args  | object | Connect args to pass to underlying driver | None |   |\n',
        '| final.field_array  | array(string) |  | None |   |\n',
        '| final.platform  | Enum |  | DBT |   |\n'

    ]

def test_field_tree():
    field_tree = FieldTree().add_field(
        FieldRow(path="connection", parent=None, type_name="Connection", required=False, default="None", description="", inner_fields=[])
        ).add_field(
        FieldRow(path="connection.path", parent="connection", type_name="string(file-path)", required=True, default="None", description="", inner_fields=[])
        ).add_field(
        FieldRow(path="connection.path", parent="connection", type_name="string(directory-path)", required=True, default="None", description="", inner_fields=[])
        ).add_field(
        FieldRow(path="field_array", parent=None, type_name="array(string)", required=True, default="None", description="", inner_fields=[])        
        )
    field_tree.sort()
    sorted_fields = [f for f in field_tree.get_fields()]

def test_field_row():

    field_path = '[version=2.0].[type=FinalConfig].[type=map].[type=Connection].connection_map.[type=union].[type=string(file-path)].path'
    assert FieldRow.field_path_to_components(field_path) == [
        'connection_map',
        '`key`',
        'path'
    ]

    field_path = '[version=2.0].[type=FinalConfig].[type=map].[type=Connection].connection_map.[type=map].[type=string].headers'
    assert FieldRow.field_path_to_components(field_path) == [
        'connection_map',
        '`key`',
        'headers'
    ]


    field_path = '[version=2.0].[type=FinalConfig].[type=Connection].connection.[type=map].[type=string].headers'
    assert FieldRow.field_path_to_components(field_path) == [
        'connection',
        'headers'
    ]


    field_path = '[version=2.0].[type=FinalConfig].[type=map].[type=Connection].connection_map.[type=union].[type=string(file-path)].path'
    assert FieldRow.field_path_to_components(field_path) == [
        'connection_map',
        '`key`',
        'path'
    ]

    field_path = '[version=2.0].[type=FinalConfig].[type=string].env'
    assert FieldRow.field_path_to_components(field_path) == [
        'env'
    ]

    field_path = '[version=2.0].[type=FinalConfig].[type=map].[type=Connection].connection_map.[type=union].[type=string(file-path)].path'
    assert FieldRow.field_path_to_components(field_path) == [
        'connection_map',
        '`key`',
        'path'
    ]

