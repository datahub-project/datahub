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
    MapFieldEdge,
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

RNA_TABLE = (
    "urn:li:dataset:(urn:li:dataPlatform:postgres,pfmegrnargs.rnc_database,PROD)"
)
MY_TABLE = "urn:li:dataset:(urn:li:dataPlatform:postgres,pfmegrnargs.my_table,PROD)"
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


def test_json_source_without_dataset_is_skipped() -> None:
    # An HTTP read has no dataset, so a map from its JSON profile has no
    # source columns.
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

    schemas = {
        RNA_TABLE: schema("ID", "DESCR"),
        MY_TABLE: schema("RNA_DB_ID", "RNA_DB_LABEL"),
    }
    graph = MagicMock()
    graph.get_aspect.side_effect = lambda urn, aspect: schemas.get(urn)

    lineage, report = _resolve(RNA_PROCESS, _components(), graph=graph)

    assert lineage.column_lineage == [
        _edge([(RNA_TABLE, "ID")], (MY_TABLE, "RNA_DB_ID"), "IDENTITY"),
        _edge([(RNA_TABLE, "DESCR")], (MY_TABLE, "RNA_DB_LABEL"), "StringConcat"),
    ]
    assert dict(report.column_lineage_fields_skipped) == {"not_in_schema": 1}
