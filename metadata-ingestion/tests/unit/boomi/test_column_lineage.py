import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from unittest.mock import MagicMock

import pytest

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.ingestion.source.boomi.client import BoomiApiError
from datahub.ingestion.source.boomi.component_parser import (
    parse_map_component,
    parse_operation_component,
    parse_process_component,
    parse_profile_component,
)
from datahub.ingestion.source.boomi.config import BoomiSourceConfig
from datahub.ingestion.source.boomi.lineage import (
    BoomiLineageResolver,
    InsertColumns,
    MapFieldEdge,
    insert_columns,
    map_field_edges,
)
from datahub.ingestion.source.boomi.models import (
    ColumnLineageEdge,
    MapShape,
    ProcessLineage,
    ProfileField,
)
from datahub.ingestion.source.boomi.report import BoomiSourceReport
from datahub.metadata.schema_classes import (
    NumberTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
)

# Components captured from a Boomi trial account through the Platform API and
# anonymised. They are shared with the integration test.
COMPONENTS_DIR = (
    Path(__file__).parents[2] / "integration" / "boomi" / "fixtures" / "components"
)

POSTGRES_CONNECTION = "1a5d3fed-78e7-48d6-83e3-886669a42439"
HTTP_CONNECTION = "aff32f06-3c23-4012-8d05-e488bd3490e2"
RNA_GET = "b8cb6962-9950-4dfa-9bcb-d5670375ca82"
RNA_PROFILE = "9b842e54-05ff-435a-a87d-209d92a690cb"
MY_TABLE_SEND = "a061ed55-80de-4cfe-9fe5-073ca8cb2370"
MY_TABLE_PROFILE = "9edcf033-37ff-409d-8a92-c4382f5d6689"
SF_QUERY = "76666d46-78ca-4250-9aa6-e5f07e03da7c"
SF_PROFILE = "0a2a53f7-8fe0-4f39-bb71-0359712abf7d"
JSON_PROFILE = "3a1d9f5e-c9d7-4e6f-8988-4947a0eaa888"
RNA_MAP = "fd77758c-c5d5-465b-913c-d2b7df271b57"
SF_MAP = "9dcb95d6-18ae-41b3-a92f-fa5284dafd5e"
JSON_MAP = "ab447401-9c84-4848-b5cb-9da82e21008f"
RNA_PROCESS = "7d88f56c-bf97-4760-ba61-170b6b0f6e8c"
SF_PROCESS = "c7b70264-535d-423f-9f17-e80bc777dd17"
V2_MYSQL_CONNECTION = "64f429a7-f94a-420d-a5d0-03385e9c3fc8"
DB_V2 = "officialboomi-X3979C-dbv2da-prod"

RNA_TABLE = (
    "urn:li:dataset:(urn:li:dataPlatform:postgres,pfmegrnargs.rnc_database,PROD)"
)
MY_TABLE = "urn:li:dataset:(urn:li:dataPlatform:postgres,pfmegrnargs.my_table,PROD)"
V2_MY_TABLE = (
    "urn:li:dataset:(urn:li:dataPlatform:mysql,homo_sapiens_core_116_38.my_table,PROD)"
)
SF_ACCOUNT = "urn:li:dataset:(urn:li:dataPlatform:salesforce,Account,PROD)"
RNA_SQL = "SELECT id, descr, display_name FROM rnc_database ORDER BY id"


def _xml(component_id: str) -> str:
    return (COMPONENTS_DIR / f"{component_id}.xml").read_text()


def _components(**replacements: Dict[str, str]) -> Dict[str, str]:
    components = {path.stem: path.read_text() for path in COMPONENTS_DIR.glob("*.xml")}
    for component_id, pairs in replacements.items():
        xml = components[component_id]
        for old, new in pairs.items():
            assert old in xml, old
            xml = xml.replace(old, new)
        components[component_id] = xml
    return components


def _resolve(
    process_id: str,
    components: Dict[str, str],
    graph: Optional[MagicMock] = None,
    **config: object,
) -> Tuple[ProcessLineage, BoomiSourceReport]:
    client = MagicMock()

    def get_component_xml(component_id: str) -> str:
        if component_id not in components:
            raise BoomiApiError(404, "not found")
        return components[component_id]

    client.get_component_xml.side_effect = get_component_xml
    report = BoomiSourceReport()
    resolver = BoomiLineageResolver(
        BoomiSourceConfig.model_validate(
            {
                "account_id": "acme-AB1CD2",
                "username": "svc@example.com",
                "api_token": "secret-token",
                **config,
            }
        ),
        client,
        report,
        graph,
    )
    info = parse_process_component(components[process_id])
    return resolver.resolve_process(info), report


def _field(dataset: str, column: str) -> str:
    return make_schema_field_urn(dataset, column)


def _edge(
    upstreams: List[Tuple[str, str]], downstream: Tuple[str, str], transform: str
) -> ColumnLineageEdge:
    return ColumnLineageEdge(
        upstreams=sorted(_field(*upstream) for upstream in upstreams),
        downstream=_field(*downstream),
        transform=transform,
    )


def test_parse_map_component() -> None:
    component = parse_map_component(_xml(RNA_MAP))

    assert component.from_profile == RNA_PROFILE
    assert component.to_profile == MY_TABLE_PROFILE
    assert len(component.mappings) == 5
    assert {key: f.type for key, f in component.functions.items()} == {
        "1": "StringConcat",
        "2": "CurrentDate",
    }
    # The Get Current Date function has no inputs and the Default on key 6
    # names no source, so neither gives an upstream field.
    assert map_field_edges(component) == [
        MapFieldEdge(("5",), "5", "IDENTITY"),
        MapFieldEdge(("6", "7"), "6", "StringConcat"),
    ]


@pytest.mark.parametrize(
    "component_id, expected",
    [
        (
            MY_TABLE_PROFILE,
            {
                "5": ProfileField(
                    name="rna_db_id", path="Statement/Fields/rna_db_id", depth=0
                ),
                "6": ProfileField(
                    name="rna_db_label", path="Statement/Fields/rna_db_label", depth=0
                ),
                "7": ProfileField(
                    name="loaded_at", path="Statement/Fields/loaded_at", depth=0
                ),
            },
        ),
        (
            SF_PROFILE,
            {
                "2": ProfileField(name="Id", path="Account/Id", depth=1),
                "3": ProfileField(name="Name", path="Account/Name", depth=1),
                "4": ProfileField(name="Industry", path="Account/Industry", depth=1),
            },
        ),
        (
            JSON_PROFILE,
            {
                "1": ProfileField(name="Root", path="Root", depth=0),
                "3": ProfileField(name="id", path="Root/Object/id", depth=1),
                "4": ProfileField(name="title", path="Root/Object/title", depth=1),
                "7": ProfileField(
                    name="name", path="Root/Object/author/Object/name", depth=2
                ),
            },
        ),
    ],
)
def test_parse_profile_fields(
    component_id: str, expected: Dict[str, ProfileField]
) -> None:
    assert parse_profile_component(_xml(component_id)).fields == expected


def test_parse_process_map_shape_and_operation_profiles() -> None:
    info = parse_process_component(_xml(RNA_PROCESS))
    assert info.map_shapes == [MapShape(shape_name="shape3", map_id=RNA_MAP)]
    assert info.next_shapes["shape2"] == ["shape3"]
    assert info.next_shapes["shape3"] == ["shape4"]
    assert [shape.shape_name for shape in info.connector_actions] == [
        "shape2",
        "shape4",
    ]

    salesforce = parse_operation_component(_xml(SF_QUERY))
    assert salesforce.profile_ids == [SF_PROFILE]


def test_database_map_direct_and_function_edges() -> None:
    lineage, report = _resolve(RNA_PROCESS, _components())

    assert lineage.input_datasets == [RNA_TABLE]
    assert lineage.output_datasets == [MY_TABLE]
    assert lineage.column_lineage == [
        _edge([(RNA_TABLE, "id")], (MY_TABLE, "rna_db_id"), "IDENTITY"),
        _edge(
            [(RNA_TABLE, "descr"), (RNA_TABLE, "display_name")],
            (MY_TABLE, "rna_db_label"),
            "StringConcat",
        ),
    ]
    assert report.column_lineage_maps_resolved == 1
    assert dict(report.column_lineage_fields_skipped) == {}


def test_salesforce_map_keeps_field_api_names() -> None:
    lineage, _ = _resolve(SF_PROCESS, _components())

    assert lineage.column_lineage == [
        _edge([(SF_ACCOUNT, "Id")], (MY_TABLE, "rna_db_id"), "IDENTITY"),
        _edge([(SF_ACCOUNT, "Name")], (MY_TABLE, "rna_db_label"), "IDENTITY"),
    ]


def test_column_lineage_disabled() -> None:
    lineage, report = _resolve(RNA_PROCESS, _components(), include_column_lineage=False)

    assert lineage.input_datasets == [RNA_TABLE]
    assert lineage.column_lineage == []
    assert report.lineage_components_fetched == 5


@pytest.mark.parametrize(
    "sql, field_names, expected, skipped",
    [
        # An alias maps the profile field back to the column it selects.
        (
            "SELECT id, descr AS label_text, display_name FROM rnc_database",
            {'name="descr"': 'name="label_text"'},
            [
                _edge([(RNA_TABLE, "id")], (MY_TABLE, "rna_db_id"), "IDENTITY"),
                _edge(
                    [(RNA_TABLE, "descr"), (RNA_TABLE, "display_name")],
                    (MY_TABLE, "rna_db_label"),
                    "StringConcat",
                ),
            ],
            {},
        ),
        # With SELECT * every field name is taken as a column.
        (
            "SELECT * FROM rnc_database",
            {},
            [
                _edge([(RNA_TABLE, "id")], (MY_TABLE, "rna_db_id"), "IDENTITY"),
                _edge(
                    [(RNA_TABLE, "descr"), (RNA_TABLE, "display_name")],
                    (MY_TABLE, "rna_db_label"),
                    "StringConcat",
                ),
            ],
            {},
        ),
        # A field that is not in the select list is not guessed.
        (
            "SELECT id, descr FROM rnc_database",
            {},
            [
                _edge([(RNA_TABLE, "id")], (MY_TABLE, "rna_db_id"), "IDENTITY"),
                _edge(
                    [(RNA_TABLE, "descr")], (MY_TABLE, "rna_db_label"), "StringConcat"
                ),
            ],
            {"not_in_select": 1},
        ),
        (
            "SELECT d.id, d.descr, a.display_name FROM rnc_database d "
            "JOIN rnc_accessions a ON a.id = d.id",
            {},
            [],
            {"multi_table_sql": 3},
        ),
    ],
)
def test_database_read_sql_field_mapping(
    sql: str,
    field_names: Dict[str, str],
    expected: List[ColumnLineageEdge],
    skipped: Dict[str, int],
) -> None:
    components = _components(**{RNA_PROFILE: {RNA_SQL: sql, **field_names}})

    lineage, report = _resolve(RNA_PROCESS, components)

    assert lineage.column_lineage == expected
    assert dict(report.column_lineage_fields_skipped) == skipped


def test_skipped_field_is_counted_once_across_connector_pairs() -> None:
    # The map feeds two writes, so each source field pairs with both.
    second_write = (
        '<shape image="connectoraction_icon" name="shape6" '
        'shapetype="connectoraction" userlabel="Insert my_table again" '
        'x="723.0" y="148.0"><configuration><connectoraction actionType="Send" '
        f'allowDynamicCredentials="NONE" connectionId="{POSTGRES_CONNECTION}" '
        f'connectorType="database" hideSettings="false" operationId="{MY_TABLE_SEND}">'
        "<parameters/><dynamicProperties/></connectoraction></configuration>"
        "<dragpoints/></shape>"
    )
    components = _components(
        **{
            RNA_PROFILE: {RNA_SQL: "SELECT id, descr FROM rnc_database"},
            RNA_PROCESS: {
                "</shapes>": f"{second_write}</shapes>",
                'toShape="shape4" x="707.0" y="56.0"/>': (
                    'toShape="shape4" x="707.0" y="56.0"/>'
                    '<dragpoint name="shape3.dragpoint2" toShape="shape6" '
                    'x="707.0" y="156.0"/>'
                ),
            },
        }
    )

    lineage, report = _resolve(RNA_PROCESS, components)

    assert lineage.column_lineage == [
        _edge([(RNA_TABLE, "id")], (MY_TABLE, "rna_db_id"), "IDENTITY"),
        _edge([(RNA_TABLE, "descr")], (MY_TABLE, "rna_db_label"), "StringConcat"),
    ]
    assert dict(report.column_lineage_fields_skipped) == {"not_in_select": 1}


def test_map_after_http_read_has_no_matching_read() -> None:
    # An HTTP read resolves to no dataset, so the map finds no read to take
    # source columns from and is skipped before its JSON profile is used.
    components = _components(
        **{
            RNA_PROCESS: {
                f'actionType="Get" allowDynamicCredentials="NONE" connectionId="{POSTGRES_CONNECTION}" connectorType="database"': (
                    f'actionType="Get" allowDynamicCredentials="NONE" connectionId="{HTTP_CONNECTION}" connectorType="http"'
                ),
                f'mapId="{RNA_MAP}"': f'mapId="{JSON_MAP}"',
            }
        }
    )

    lineage, report = _resolve(RNA_PROCESS, components)

    assert lineage.output_datasets == [MY_TABLE]
    assert lineage.column_lineage == []
    assert dict(report.column_lineage_maps_skipped) == {"no_matching_read": 1}


@pytest.mark.parametrize(
    "replacements, reason",
    [
        (
            {RNA_MAP: {f'fromProfile="{RNA_PROFILE}"': 'fromProfile="other"'}},
            "no_matching_read",
        ),
        (
            {RNA_MAP: {f'toProfile="{MY_TABLE_PROFILE}"': 'toProfile="other"'}},
            "no_matching_write",
        ),
    ],
)
def test_map_profiles_must_match_operations(
    replacements: Dict[str, Dict[str, str]], reason: str
) -> None:
    lineage, report = _resolve(RNA_PROCESS, _components(**replacements))

    assert lineage.input_datasets == [RNA_TABLE]
    assert lineage.column_lineage == []
    assert dict(report.column_lineage_maps_skipped) == {reason: 1}


SOURCE = "urn:li:dataset:(urn:li:dataPlatform:postgres,pfmegrnargs.staging_orders,PROD)"


@pytest.mark.parametrize(
    "sql, expected, skipped",
    [
        (
            "INSERT INTO my_table (rna_db_id, rna_db_label) "
            "SELECT id, upper(display_name) AS label FROM staging_orders",
            [
                _edge([(SOURCE, "id")], (MY_TABLE, "rna_db_id"), "IDENTITY"),
                _edge([(SOURCE, "display_name")], (MY_TABLE, "rna_db_label"), "SQL"),
            ],
            {},
        ),
        # Columns are matched to the INSERT's column list by position, not
        # by the SELECT's output names.
        (
            "INSERT INTO my_table (rna_db_label, rna_db_id) "
            "SELECT rna_db_id, rna_db_label FROM staging_orders",
            [
                _edge([(SOURCE, "rna_db_label")], (MY_TABLE, "rna_db_id"), "IDENTITY"),
                _edge([(SOURCE, "rna_db_id")], (MY_TABLE, "rna_db_label"), "IDENTITY"),
            ],
            {},
        ),
        (
            "INSERT INTO my_table SELECT id FROM staging_orders",
            [],
            {"insert_without_column_list": 1},
        ),
    ],
)
def test_insert_select_column_lineage(
    sql: str, expected: List[ColumnLineageEdge], skipped: Dict[str, int]
) -> None:
    write_sql = (
        "INSERT INTO my_table (rna_db_id, rna_db_label, loaded_at) VALUES (?, ?, ?)"
    )
    components = _components(
        **{
            MY_TABLE_PROFILE: {
                write_sql: sql,
                'statementType="dynamicinsert"': 'statementType="standardinsert"',
            },
            # Without the map, only the SQL gives column lineage.
            RNA_PROCESS: {f'mapId="{RNA_MAP}"': 'mapId="missing-map"'},
        }
    )
    components["missing-map"] = components[RNA_MAP].replace(
        f'toProfile="{MY_TABLE_PROFILE}"', 'toProfile="other"'
    )

    lineage, report = _resolve(RNA_PROCESS, components)

    assert lineage.column_lineage == expected
    assert dict(report.column_lineage_fields_skipped) == skipped
    if expected:
        assert SOURCE in lineage.input_datasets


def test_insert_select_columns_take_precedence_over_map_edges() -> None:
    # rna_db_id comes from staging_orders in the SQL, so the map's edge into it
    # is dropped. rna_db_label is a parameter, so the map's edge is kept.
    components = _components(
        **{
            MY_TABLE_PROFILE: {
                "INSERT INTO my_table (rna_db_id, rna_db_label, loaded_at) VALUES (?, ?, ?)": (
                    "INSERT INTO my_table (rna_db_id, rna_db_label) "
                    "SELECT id, ? FROM staging_orders"
                ),
                'statementType="dynamicinsert"': 'statementType="standardinsert"',
            }
        }
    )

    lineage, report = _resolve(RNA_PROCESS, components)

    assert lineage.column_lineage == [
        _edge([(SOURCE, "id")], (MY_TABLE, "rna_db_id"), "IDENTITY"),
        _edge(
            [(RNA_TABLE, "descr"), (RNA_TABLE, "display_name")],
            (MY_TABLE, "rna_db_label"),
            "StringConcat",
        ),
    ]
    assert report.column_lineage_fields_skipped["written_by_sql"] == 1


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO my_table (rna_db_id, rna_db_label) SELECT * FROM staging_orders",
        "INSERT INTO my_table (rna_db_id, rna_db_label) "
        "SELECT id, display_name FROM staging_orders "
        "UNION SELECT id, display_name FROM staging_orders",
        "INSERT INTO my_table (rna_db_id, rna_db_label) SELECT id FROM staging_orders",
        "INSERT INTO my_table (rna_db_id, rna_db_label) "
        "SELECT id, id FROM staging_orders",
    ],
)
def test_map_edges_skipped_when_write_sql_columns_are_unknown(sql: str) -> None:
    # The SQL reads staging_orders but its filled columns cannot be matched
    # to the INSERT list, so any map edge into the write could be wrong.
    components = _components(
        **{
            MY_TABLE_PROFILE: {
                "INSERT INTO my_table (rna_db_id, rna_db_label, loaded_at) VALUES (?, ?, ?)": sql,
                'statementType="dynamicinsert"': 'statementType="standardinsert"',
            }
        }
    )

    lineage, report = _resolve(RNA_PROCESS, components)

    assert SOURCE in lineage.input_datasets
    assert not [
        edge for edge in lineage.column_lineage if RNA_TABLE in str(edge.upstreams)
    ]
    assert report.column_lineage_fields_skipped["unmapped_sql_write"] >= 1


SHAPE4_SEND = 'operationId="a061ed55-80de-4cfe-9fe5-073ca8cb2370" parameter-profile="EMBEDDED|databaseparameterchooser|9edcf033-37ff-409d-8a92-c4382f5d6689|a061ed55-80de-4cfe-9fe5-073ca8cb2370"'
RNA_SHAPES_END = "</shapes>"


def _with_shape(xml: str, shape: str) -> str:
    assert RNA_SHAPES_END in xml
    return xml.replace(RNA_SHAPES_END, shape + RNA_SHAPES_END, 1)


def test_sql_columns_of_one_write_keep_map_edges_into_another() -> None:
    # A second write to my_table fills rna_db_id from staging_orders. The map
    # feeds the first write only, so its edge into rna_db_id is kept.
    components = _components()
    components["send-2"] = components[MY_TABLE_SEND].replace(
        MY_TABLE_PROFILE, "profile-2"
    )
    components["profile-2"] = (
        components[MY_TABLE_PROFILE]
        .replace(
            "INSERT INTO my_table (rna_db_id, rna_db_label, loaded_at) VALUES (?, ?, ?)",
            "INSERT INTO my_table (rna_db_id) SELECT id FROM staging_orders",
        )
        .replace('statementType="dynamicinsert"', 'statementType="standardinsert"')
    )
    process = components[RNA_PROCESS]
    shape4_start = process.index('<shape image="connectoraction_icon" name="shape4"')
    shape4 = process[shape4_start : process.index("</shape>", shape4_start)]
    shape7 = re.sub(
        "<dragpoints>.*</dragpoints>",
        "<dragpoints/>",
        shape4.replace('name="shape4"', 'name="shape7"').replace(
            SHAPE4_SEND, 'operationId="send-2"'
        ),
    )
    components[RNA_PROCESS] = _with_shape(process, shape7 + "</shape>")

    lineage, report = _resolve(RNA_PROCESS, components)

    # The SQL edge and the map's direct edge share a transform, so they merge.
    assert (
        _edge([(SOURCE, "id"), (RNA_TABLE, "id")], (MY_TABLE, "rna_db_id"), "IDENTITY")
        in lineage.column_lineage
    )
    assert "written_by_sql" not in report.column_lineage_fields_skipped


def test_merge_write_gives_table_lineage_only() -> None:
    components = _components(
        **{
            MY_TABLE_PROFILE: {
                "INSERT INTO my_table (rna_db_id, rna_db_label, loaded_at) VALUES (?, ?, ?)": (
                    # The parser names the written columns after the USING
                    # query's outputs (rna_db_id, lbl), not the INSERT list.
                    "MERGE INTO my_table USING (SELECT id AS rna_db_id, "
                    "display_name AS lbl FROM staging_orders) s "
                    "ON my_table.rna_db_id = s.rna_db_id "
                    "WHEN NOT MATCHED THEN INSERT (rna_db_id, rna_db_label) "
                    "VALUES (s.rna_db_id, s.lbl)"
                ),
                'statementType="dynamicinsert"': 'statementType="standardinsert"',
            },
            RNA_PROCESS: {f'mapId="{RNA_MAP}"': 'mapId="missing-map"'},
        }
    )
    components["missing-map"] = components[RNA_MAP].replace(
        f'toProfile="{MY_TABLE_PROFILE}"', 'toProfile="other"'
    )

    lineage, report = _resolve(RNA_PROCESS, components)

    assert SOURCE in lineage.input_datasets
    assert MY_TABLE in lineage.output_datasets
    assert lineage.column_lineage == []
    assert report.column_lineage_fields_skipped["merge_statement"] == 2


@pytest.mark.parametrize("shape_type", ["processcall", "processroute"])
def test_map_does_not_reach_a_write_past_a_process_handoff(shape_type: str) -> None:
    components = _components(
        **{
            RNA_PROCESS: {
                'name="shape3.dragpoint1" toShape="shape4"': 'name="shape3.dragpoint1" toShape="shape6"'
            }
        }
    )
    components[RNA_PROCESS] = _with_shape(
        components[RNA_PROCESS],
        f'<shape image="{shape_type}_icon" name="shape6" shapetype="{shape_type}" '
        'userlabel="" x="600.0" y="48.0"><configuration><processcall '
        'processId="child-process" wait="true"/></configuration><dragpoints>'
        '<dragpoint name="shape6.dragpoint1" toShape="shape4" x="700.0" y="56.0"/>'
        "</dragpoints></shape>",
    )

    lineage, report = _resolve(RNA_PROCESS, components)

    assert MY_TABLE in lineage.output_datasets
    assert lineage.column_lineage == []
    assert report.column_lineage_maps_skipped["no_matching_write"] == 1


def test_unset_dragpoint_is_a_dead_end() -> None:
    components = _components(
        **{
            RNA_PROCESS: {
                'name="shape3.dragpoint1" toShape="shape4"': 'name="shape3.dragpoint1" toShape="unset"'
            }
        }
    )

    lineage, report = _resolve(RNA_PROCESS, components)

    assert MY_TABLE in lineage.output_datasets
    assert lineage.column_lineage == []
    assert not lineage.incomplete
    assert report.column_lineage_maps_skipped["no_matching_write"] == 1


def test_gui_process_with_only_a_start_shape() -> None:
    xml = (
        Path(__file__).parent / "fixtures" / "gui_start_only_process.xml"
    ).read_text()
    info = parse_process_component(xml)
    assert info.next_shapes == {"shape1": ["unset"]}
    assert info.flow_break_shapes == []

    lineage, report = _resolve("gui", {"gui": xml})

    assert lineage == ProcessLineage(
        input_datasets=[],
        output_datasets=[],
        child_process_ids=[],
        column_lineage=[],
        incomplete=False,
    )
    assert not report.failures and not report.warnings


def test_write_sql_fields_must_be_in_insert_column_list() -> None:
    components = _components(
        **{
            MY_TABLE_PROFILE: {
                'statementType="dynamicinsert"': 'statementType="standardinsert"',
                "(rna_db_id, rna_db_label, loaded_at)": "(rna_db_id, label, loaded_at)",
            }
        }
    )

    lineage, report = _resolve(RNA_PROCESS, components)

    assert lineage.column_lineage == [
        _edge([(RNA_TABLE, "id")], (MY_TABLE, "rna_db_id"), "IDENTITY"),
    ]
    assert dict(report.column_lineage_fields_skipped) == {"not_in_insert_columns": 1}


def _db_v2_send_components(
    request_profile: str, query: str, **replacements: Dict[str, str]
) -> Dict[str, str]:
    v2_send = (
        _xml(MY_TABLE_SEND)
        .replace('subType="database"', f'subType="{DB_V2}"')
        .replace(
            f'<DatabaseSendAction batchCount="10" commitOption="commitrows" enableBatching="true"><WriteProfile profileId="{MY_TABLE_PROFILE}"/>\n        </DatabaseSendAction>',
            '<GenericOperationConfig customOperationType="INSERT" objectTypeId="" '
            f'operationType="EXECUTE" requestProfile="{request_profile}" '
            'requestProfileType="json" responseProfileType="json">'
            f'<field id="query" type="string" value="{query}"/></GenericOperationConfig>',
        )
    )
    assert "GenericOperationConfig" in v2_send
    components = _components(
        **{
            RNA_PROCESS: {
                f'actionType="Send" allowDynamicCredentials="NONE" connectionId="{POSTGRES_CONNECTION}" connectorType="database"': (
                    f'actionType="Send" allowDynamicCredentials="NONE" connectionId="{V2_MYSQL_CONNECTION}" connectorType="{DB_V2}"'
                ),
            },
            **replacements,
        }
    )
    components[MY_TABLE_SEND] = v2_send
    return components


def test_db_v2_write_query_maps_fields_through_insert_columns() -> None:
    # A Database V2 write with a query names its columns in the INSERT, as a
    # legacy profile's SQL does.
    components = _db_v2_send_components(
        MY_TABLE_PROFILE,
        "INSERT INTO my_table (rna_db_label, rna_db_id) VALUES (?, ?)",
    )

    lineage, report = _resolve(RNA_PROCESS, components)

    assert lineage.output_datasets == [V2_MY_TABLE]
    assert lineage.column_lineage == [
        _edge([(RNA_TABLE, "id")], (V2_MY_TABLE, "rna_db_id"), "IDENTITY"),
        _edge(
            [(RNA_TABLE, "descr"), (RNA_TABLE, "display_name")],
            (V2_MY_TABLE, "rna_db_label"),
            "StringConcat",
        ),
    ]
    assert dict(report.column_lineage_maps_skipped) == {}
    assert dict(report.column_lineage_fields_skipped) == {}


def test_db_v2_write_skips_nested_json_fields() -> None:
    # The JSON request profile's author/name field is nested, so it is not
    # the table's name column even though the INSERT lists one.
    components = _db_v2_send_components(
        JSON_PROFILE,
        "INSERT INTO my_table (id, name) VALUES (?, ?)",
        **{
            RNA_MAP: {
                f'toProfile="{MY_TABLE_PROFILE}"': f'toProfile="{JSON_PROFILE}"',
                'toKey="5" toType="profile"': 'toKey="3" toType="profile"',
                'toKey="6" toType="profile"': 'toKey="7" toType="profile"',
            }
        },
    )

    lineage, report = _resolve(RNA_PROCESS, components)

    assert lineage.column_lineage == [
        _edge([(RNA_TABLE, "id")], (V2_MY_TABLE, "id"), "IDENTITY"),
    ]
    assert dict(report.column_lineage_fields_skipped) == {"nested_field": 1}


SNOWFLAKE = "officialboomi-X3979C-snowfl-prod"
SNOWFLAKE_CONNECTION = "5b1e0c2a-6f0d-4c1e-9a43-000000000018"
SNOWFLAKE_MY_TABLE = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.public.my_table,PROD)"
)


@pytest.mark.parametrize(
    "operation_fields",
    [
        pytest.param("", id="object_type_id"),
        pytest.param(
            '<field id="sqlStatement" type="string" value="INSERT INTO my_table '
            '(rna_db_label, rna_db_id) VALUES (?, ?)"/>',
            id="sql_field",
        ),
    ],
)
def test_jdbc_url_connector_write_gets_map_column_lineage(
    operation_fields: str,
) -> None:
    # The connection is only resolved from a JDBC URL in a field the source
    # plugin has no id for, and the operation carries its profile on
    # GenericOperationConfig as Database V2 does.
    connection = (
        _xml(V2_MYSQL_CONNECTION)
        .replace(f'subType="{DB_V2}"', f'subType="{SNOWFLAKE}"')
        .replace(
            '<field id="url" type="string" value="jdbc:mysql://ensembldb.ensembl.org:3306/homo_sapiens_core_116_38"/>',
            '<field id="connectionString" type="string" value="jdbc:snowflake://'
            'acct.snowflakecomputing.com/?db=ANALYTICS&amp;schema=PUBLIC"/>',
        )
    )
    send = (
        _xml(MY_TABLE_SEND)
        .replace('subType="database"', f'subType="{SNOWFLAKE}"')
        .replace(
            f'<DatabaseSendAction batchCount="10" commitOption="commitrows" enableBatching="true"><WriteProfile profileId="{MY_TABLE_PROFILE}"/>\n        </DatabaseSendAction>',
            '<GenericOperationConfig customOperationType="INSERT" objectTypeId="MY_TABLE" '
            f'operationType="CREATE" requestProfile="{MY_TABLE_PROFILE}" '
            f'requestProfileType="json" responseProfileType="json">{operation_fields}'
            "</GenericOperationConfig>",
        )
    )
    assert "connectionString" in connection and "GenericOperationConfig" in send
    components = _components(
        **{
            RNA_PROCESS: {
                f'actionType="Send" allowDynamicCredentials="NONE" connectionId="{POSTGRES_CONNECTION}" connectorType="database"': (
                    f'actionType="Send" allowDynamicCredentials="NONE" connectionId="{SNOWFLAKE_CONNECTION}" connectorType="{SNOWFLAKE}"'
                ),
            }
        }
    )
    components[SNOWFLAKE_CONNECTION] = connection
    components[MY_TABLE_SEND] = send

    lineage, report = _resolve(RNA_PROCESS, components)

    assert lineage.output_datasets == [SNOWFLAKE_MY_TABLE]
    assert lineage.column_lineage == [
        _edge([(RNA_TABLE, "id")], (SNOWFLAKE_MY_TABLE, "rna_db_id"), "IDENTITY"),
        _edge(
            [(RNA_TABLE, "descr"), (RNA_TABLE, "display_name")],
            (SNOWFLAKE_MY_TABLE, "rna_db_label"),
            "StringConcat",
        ),
    ]
    assert dict(report.column_lineage_maps_skipped) == {}
    assert dict(report.column_lineage_fields_skipped) == {}


@pytest.mark.parametrize("missing", [RNA_MAP, SF_PROFILE])
def test_unreadable_map_component_omits_process_lineage(missing: str) -> None:
    components = _components()
    del components[missing]

    lineage, report = _resolve(
        SF_PROCESS if missing == SF_PROFILE else RNA_PROCESS, components
    )

    assert lineage.incomplete
    assert lineage.column_lineage == []
    assert dict(report.column_lineage_maps_skipped) == {"component_unreadable": 1}


def test_graph_schema_sets_column_casing_and_drops_unknown_columns() -> None:
    def schema(*names: str) -> SchemaMetadataClass:
        return SchemaMetadataClass(
            schemaName="",
            platform="urn:li:dataPlatform:postgres",
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=[
                SchemaFieldClass(
                    fieldPath=name,
                    type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                    nativeDataType="",
                )
                for name in names
            ],
        )

    v2_id = "[version=2.0].[type=int].RNA_DB_ID"
    v2_label = "[version=2.0].[type=string].RNA_DB_LABEL"
    schemas = {
        RNA_TABLE: schema("ID", "DESCR"),
        # Some sources store v2 field paths; the URN must use them as stored.
        MY_TABLE: schema(v2_id, v2_label),
    }
    graph = MagicMock()
    graph.get_aspect.side_effect = lambda urn, aspect: schemas.get(urn)

    lineage, report = _resolve(RNA_PROCESS, _components(), graph=graph)

    assert lineage.column_lineage == [
        _edge([(RNA_TABLE, "ID")], (MY_TABLE, v2_id), "IDENTITY"),
        _edge([(RNA_TABLE, "DESCR")], (MY_TABLE, v2_label), "StringConcat"),
    ]
    assert dict(report.column_lineage_fields_skipped) == {"not_in_schema": 1}


@pytest.mark.parametrize(
    "sql, expected",
    [
        ("INSERT INTO (", InsertColumns(parsed=False)),
        ("UPDATE my_table SET a = ?", InsertColumns(parsed=True)),
        (
            "INSERT INTO my_table SELECT a FROM src",
            InsertColumns(parsed=True, is_insert=True, by_output=None),
        ),
        # Without one SELECT output per listed column, position says nothing.
        (
            "INSERT INTO my_table (a, b) SELECT * FROM src",
            InsertColumns(parsed=True, is_insert=True, by_output={}),
        ),
        (
            "INSERT INTO my_table (a, b) SELECT x FROM src",
            InsertColumns(parsed=True, is_insert=True, by_output={}),
        ),
        (
            "MERGE INTO my_table t USING src s ON t.a = s.a "
            "WHEN NOT MATCHED THEN INSERT (a) VALUES (s.a)",
            InsertColumns(parsed=True, is_merge=True),
        ),
        # An output name used twice cannot be tied to one column.
        (
            "INSERT INTO my_table (a, b, c) SELECT x, x, y FROM src",
            InsertColumns(
                parsed=True, is_insert=True, by_output={"y": "c"}, unambiguous=False
            ),
        ),
    ],
)
def test_insert_columns(sql: str, expected: InsertColumns) -> None:
    assert insert_columns(sql, "postgres") == expected


def test_schema_lookup_failure_falls_back_to_boomi_names() -> None:
    graph = MagicMock()
    graph.get_aspect.side_effect = RuntimeError("GMS unavailable")

    lineage, report = _resolve(RNA_PROCESS, _components(), graph=graph)

    assert lineage.column_lineage == [
        _edge([(RNA_TABLE, "id")], (MY_TABLE, "rna_db_id"), "IDENTITY"),
        _edge(
            [(RNA_TABLE, "descr"), (RNA_TABLE, "display_name")],
            (MY_TABLE, "rna_db_label"),
            "StringConcat",
        ),
    ]
    assert [w.title for w in report.warnings] == [
        "Failed to read dataset schemas from DataHub"
    ]
    # Each dataset's schema is requested once, even after a failure.
    assert graph.get_aspect.call_count == 2


def test_db_v2_read_query_maps_select_outputs() -> None:
    components = _components(
        **{
            RNA_PROCESS: {
                f'actionType="Get" allowDynamicCredentials="NONE" connectionId="{POSTGRES_CONNECTION}" connectorType="database"': (
                    f'actionType="Get" allowDynamicCredentials="NONE" connectionId="{V2_MYSQL_CONNECTION}" connectorType="{DB_V2}"'
                ),
            },
            RNA_GET: {
                'subType="database"': f'subType="{DB_V2}"',
                f'<DatabaseGetAction batchCount="0" maxRows="0"><ReadProfile profileId="{RNA_PROFILE}"/>\n        </DatabaseGetAction>': (
                    '<GenericOperationConfig customOperationType="QUERY" objectTypeId="" '
                    f'operationType="QUERY" responseProfile="{RNA_PROFILE}" '
                    'requestProfileType="json" responseProfileType="json">'
                    f'<field id="query" type="string" value="{RNA_SQL}"/>'
                    "</GenericOperationConfig>"
                ),
            },
        }
    )
    v2_rna_table = "urn:li:dataset:(urn:li:dataPlatform:mysql,homo_sapiens_core_116_38.rnc_database,PROD)"

    lineage, report = _resolve(RNA_PROCESS, components)

    assert lineage.input_datasets == [v2_rna_table]
    assert lineage.column_lineage == [
        _edge([(v2_rna_table, "id")], (MY_TABLE, "rna_db_id"), "IDENTITY"),
        _edge(
            [(v2_rna_table, "descr"), (v2_rna_table, "display_name")],
            (MY_TABLE, "rna_db_label"),
            "StringConcat",
        ),
    ]
