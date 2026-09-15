"""What `probe describe` serialises. ProbeNode/ProbeResult tests went with those
types: they described the deleted hierarchy's output shape, which nothing produces.
"""

from datahub.ingestion.agent.models import FieldKind, FieldSpec, SourceSpec


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
    fields = d["fields"]
    assert isinstance(fields, list)
    first_field = fields[0]
    assert isinstance(first_field, dict)
    assert first_field["kind"] == "plain"
    capabilities = d["capabilities"]
    assert isinstance(capabilities, list)
    first_capability = capabilities[0]
    assert isinstance(first_capability, dict)
    assert first_capability["supported"] is True
