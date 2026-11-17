"""What `probe describe` serialises, and the keys of the `probe run` envelope.
ProbeNode/ProbeResult tests went with those types: they described the deleted
hierarchy's output shape, which nothing produces.
"""

from typing import get_type_hints

from datahub.ingestion.agent.models import (
    FieldKind,
    FieldSpec,
    ProbeRunEnvelope,
    ProbeRunEnvelopeView,
    SourceSpec,
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
    assert d["required"]


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
    assert first_capability["supported"]


def test_the_run_envelope_view_names_exactly_the_keys_the_writer_writes() -> None:
    # Declared twice, precisely for ProbeMethodResult.to_dict and object-valued
    # for the reader of caller JSON, so a key added to one must be added to the
    # other or `probe filter --from-run` would never read it.
    assert get_type_hints(ProbeRunEnvelope).keys() == (
        get_type_hints(ProbeRunEnvelopeView).keys()
    )
