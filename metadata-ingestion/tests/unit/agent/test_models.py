from datahub.ingestion.agent.models import (
    FieldKind,
    FieldSpec,
    ProbeLeafKind,
    ProbeNode,
    ProbeResult,
    SourceSpec,
)
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)


def test_field_spec_to_dict_serializes_kind_as_string():
    spec = FieldSpec(
        name="password",
        kind=FieldKind.SECRET,
        required=True,
        type_name="SecretStr",
        default=None,
        description="The password.",
    )
    d = spec.to_dict()
    assert d["kind"] == "secret"
    assert d["name"] == "password"
    assert d["required"] is True


def test_probe_node_kind_serializes_to_subtype_string():
    node = ProbeNode(
        name="orders",
        kind=DatasetSubTypes.TABLE,
        fqn="db.public.orders",
        pattern_field="table_pattern",
    )
    d = node.to_dict()
    assert d["kind"] == "Table"
    assert d["pattern_field"] == "table_pattern"


def test_probe_result_round_trips_nodes():
    result = ProbeResult(
        source_type="snowflake",
        supported=True,
        parent_path=["MY_DB"],
        nodes=[
            ProbeNode(
                name="PUBLIC",
                kind=DatasetContainerSubTypes.SCHEMA,
                fqn="MY_DB.PUBLIC",
                pattern_field="schema_pattern",
            )
        ],
        truncated=False,
        fallback=None,
    )
    d = result.to_dict()
    assert d["supported"] is True
    assert d["nodes"][0]["kind"] == "Schema"
    assert d["parent_path"] == ["MY_DB"]


def test_source_spec_to_dict():
    spec = SourceSpec(
        source_type="mysql",
        fields=[
            FieldSpec("host_port", FieldKind.PLAIN, True, "str", None, "host:port")
        ],
        capabilities=[{"capability": "Data Profiling", "supported": True}],
    )
    d = spec.to_dict()
    assert d["source_type"] == "mysql"
    assert d["fields"][0]["kind"] == "plain"
    assert d["capabilities"][0]["supported"] is True


def test_probe_leaf_kind_column():
    assert str(ProbeLeafKind.COLUMN) == "Column"
