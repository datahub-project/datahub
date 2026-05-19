"""Unit tests for the Aerospike ingestion source.

Coverage is partitioned by what each test exercises:

- AerospikeSet.from_info_string  ............... info-string parsing
- AerospikeConfig hosts validator  ............. host/port tuple validation
- Client config construction (auth/TLS/timeout)  what is sent to aerospike.client(...)
- construct_schema_aerospike  .................. query wiring (timeout, sampling,
                                                  rate-limit, exception fallback)
- get_aerospike_type_string / get_field_type  .. Python-type -> Aerospike/DataHub map
- _limit_schema_size  .......................... depth truncation + field downsampling
- get_sets / ignore_empty_sets  ................ sets-info parsing + empty filter
- namespace_pattern / set_pattern  ............. allow/deny filtering
- _get_dc_shipped_sets / xdr_sets  ............. XDR config parsing
- infer_schema_depth == 0  ..................... skips schema inference path

Auth- and TLS-related branches are *not* exercised by the docker integration
test (the test cluster is single-node, no auth, no TLS, no XDR DCs) — those
paths are only verifiable here.
"""

from typing import Any, Dict, List, Tuple
from unittest.mock import MagicMock, patch

import aerospike
import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.aerospike import (
    AerospikeConfig,
    AerospikeSet,
    AerospikeSource,
    AuthMode,
    construct_schema_aerospike,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    BooleanTypeClass,
    NullTypeClass,
    NumberTypeClass,
    StringTypeClass,
)

# --------------------------------------------------------------------------- #
# Test helpers                                                                #
# --------------------------------------------------------------------------- #


class _StrictQuery:
    """Mimics aerospike.Query's C-extension attribute model.

    The real Query exposes exactly three settable attributes
    (max_records, records_per_second, ttl); any other assignment raises
    AttributeError. __slots__ reproduces that contract so the original
    `query.socket_timeout = X` bug cannot recur silently behind a MagicMock.
    """

    __slots__ = ("max_records", "records_per_second", "ttl", "_records", "policy_seen")

    def __init__(self, records: List[Tuple]) -> None:
        self._records = records
        self.policy_seen = None  # type: ignore[assignment]

    def results(self, policy: Any = None) -> List[Tuple]:
        self.policy_seen = policy  # type: ignore[assignment]
        return self._records


def _record(pk: str, bins: Dict[str, Any]) -> Tuple:
    """Build a record tuple shaped like the aerospike client returns:
    (key, meta, bins) where key is (namespace, set, user_key, digest)."""
    return (("test", "demo", pk, b"\x00" * 20), {"gen": 1, "ttl": 0}, bins)


@pytest.fixture
def patched_aerospike_client():
    """Patches aerospike.client so AerospikeSource(...) constructs without a real server.

    Yields (ctor_mock, client_mock). Tests configure client_mock.query /
    client_mock.info_random_node before exercising the source.
    """
    with patch("datahub.ingestion.source.aerospike.aerospike.client") as ctor_mock:
        client_mock = MagicMock()
        ctor_mock.return_value.connect.return_value = client_mock
        yield ctor_mock, client_mock


def _make_source(config: Dict[str, Any]) -> AerospikeSource:
    parsed = AerospikeConfig.parse_obj(config)
    return AerospikeSource(PipelineContext(run_id="test"), parsed)


def _schema_entry(name: str, count: int, py_type: type = str) -> Dict[str, Any]:
    return {
        "types": {py_type},
        "count": count,
        "nullable": False,
        "delimited_name": name,
        "type": py_type,
    }


# --------------------------------------------------------------------------- #
# 1. AerospikeSet.from_info_string                                            #
# --------------------------------------------------------------------------- #


class TestAerospikeSetFromInfoString:
    """Aerospike's `sets` info request returns colon-delimited key=value pairs.
    Field set varies between server versions, so unknown keys must be ignored."""

    def test_parses_typed_values(self):
        result = AerospikeSet.from_info_string(
            "ns=test:set=demo:objects=42:tombstones=3:data_used_bytes=2048:index_populating=false"
        )
        assert result == AerospikeSet(
            ns="test",
            set="demo",
            objects=42,
            tombstones=3,
            data_used_bytes=2048,
            index_populating=False,
        )

    def test_parses_old_storage_format(self):
        # Older Aerospike versions report memory_data_bytes + device_data_bytes
        # instead of data_used_bytes.
        result = AerospikeSet.from_info_string(
            "ns=test:set=demo:memory_data_bytes=100:device_data_bytes=200"
        )
        assert result.memory_data_bytes == 100
        assert result.device_data_bytes == 200
        assert result.data_used_bytes is None

    def test_ignores_unknown_keys_and_malformed_items(self):
        # `disable-eviction` is not in _KNOWN_FIELDS; `bare` has no `=`.
        result = AerospikeSet.from_info_string(
            "ns=test:set=demo:disable-eviction=false:bare:objects=7"
        )
        assert (result.ns, result.set, result.objects) == ("test", "demo", 7)

    def test_parses_boolean_case_insensitive(self):
        result = AerospikeSet.from_info_string("ns=test:set=demo:index_populating=TRUE")
        assert result.index_populating is True


# --------------------------------------------------------------------------- #
# 2. Hosts validator                                                          #
# --------------------------------------------------------------------------- #


class TestHostsValidator:
    def test_rejects_single_element_tuple(self):
        with pytest.raises(ValueError, match="at least a hostname and port"):
            AerospikeConfig.parse_obj({"hosts": [("h",)]})

    def test_rejects_non_integer_port(self):
        with pytest.raises(ValueError, match="Port must be an integer"):
            AerospikeConfig.parse_obj({"hosts": [("h", "not-a-port")]})

    def test_accepts_numeric_string_port(self):
        # validate_hosts coerces via int() so "3000" passes
        config = AerospikeConfig.parse_obj({"hosts": [("h", "3000")]})
        assert config.hosts == [("h", "3000")]


# --------------------------------------------------------------------------- #
# 3. Client config construction (auth + TLS + login_timeout)                  #
#    These branches are NOT covered by the docker integration test —          #
#    the test cluster runs unauthenticated, plaintext.                        #
# --------------------------------------------------------------------------- #


class TestClientConfigConstruction:
    def test_omits_optional_fields_by_default(self, patched_aerospike_client):
        ctor_mock, _ = patched_aerospike_client
        _make_source({"hosts": [("h", 3000)]})

        config_arg = ctor_mock.call_args[0][0]
        assert config_arg["hosts"] == [("h", 3000, None)]
        assert config_arg["auth_mode"] == aerospike.AUTH_INTERNAL
        assert "user" not in config_arg
        assert "password" not in config_arg
        assert "tls" not in config_arg
        assert "login_timeout_ms" not in config_arg

    def test_includes_auth_credentials_when_set(self, patched_aerospike_client):
        ctor_mock, _ = patched_aerospike_client
        _make_source(
            {
                "hosts": [("h", 3000)],
                "username": "admin",
                "password": "s3cret",
                "auth_mode": AuthMode.AUTH_EXTERNAL,
            }
        )
        config_arg = ctor_mock.call_args[0][0]
        assert config_arg["user"] == "admin"
        assert config_arg["password"] == "s3cret"
        assert config_arg["auth_mode"] == aerospike.AUTH_EXTERNAL

    def test_includes_tls_block_when_enabled(self, patched_aerospike_client):
        ctor_mock, _ = patched_aerospike_client
        _make_source(
            {
                "hosts": [("h", 3000)],
                "tls_enabled": True,
                "tls_capath": "/etc/aerospike/certs",
                "tls_cafile": "/etc/aerospike/ca.pem",
            }
        )
        assert ctor_mock.call_args[0][0]["tls"] == {
            "enable": True,
            "capath": "/etc/aerospike/certs",
            "cafile": "/etc/aerospike/ca.pem",
        }

    def test_omits_tls_block_when_disabled(self, patched_aerospike_client):
        # tls_capath/tls_cafile should be ignored if tls_enabled=False.
        ctor_mock, _ = patched_aerospike_client
        _make_source(
            {
                "hosts": [("h", 3000)],
                "tls_enabled": False,
                "tls_cafile": "/some/path",
            }
        )
        assert "tls" not in ctor_mock.call_args[0][0]

    def test_passes_login_timeout_when_set(self, patched_aerospike_client):
        ctor_mock, _ = patched_aerospike_client
        _make_source({"hosts": [("h", 3000)], "login_timeout_ms": 2000})
        assert ctor_mock.call_args[0][0]["login_timeout_ms"] == 2000

    def test_preserves_tls_name_from_three_tuple_host(self, patched_aerospike_client):
        ctor_mock, _ = patched_aerospike_client
        _make_source({"hosts": [("h", 3000, "tls-name")]})
        assert ctor_mock.call_args[0][0]["hosts"] == [("h", 3000, "tls-name")]

    def test_connection_failure_records_report_failure(self, patched_aerospike_client):
        ctor_mock, _ = patched_aerospike_client
        ctor_mock.return_value.connect.side_effect = RuntimeError("unreachable")
        with pytest.raises(RuntimeError):
            _make_source({"hosts": [("h", 3000)]})


# --------------------------------------------------------------------------- #
# 4. construct_schema_aerospike                                               #
# --------------------------------------------------------------------------- #


class TestConstructSchemaAerospike:
    def _client_with_query(self, query: _StrictQuery) -> MagicMock:
        client = MagicMock()
        client.query.return_value = query
        return client

    def test_passes_socket_timeout_via_policy(self):
        query = _StrictQuery([_record("pk-1", {"name": "alice"})])
        construct_schema_aerospike(
            self._client_with_query(query),
            AerospikeSet(ns="t", set="s"),
            ".",
            socket_timeout_ms=5000,
        )
        assert query.policy_seen == {"socket_timeout": 5000}

    def test_omits_socket_timeout_when_unset(self):
        query = _StrictQuery([_record("pk-1", {"x": 1})])
        construct_schema_aerospike(
            self._client_with_query(query),
            AerospikeSet(ns="t", set="s"),
            ".",
            socket_timeout_ms=None,
        )
        assert query.policy_seen == {}

    def test_aerospike_query_rejects_socket_timeout_attribute(self):
        # Regression: aerospike.Query is a C extension that rejects unknown
        # attribute assignment. _StrictQuery mirrors that contract via __slots__.
        # If the fix ever regresses to `query.socket_timeout = X`, the policy
        # tests above will fail with the same AttributeError as the original bug.
        query = _StrictQuery([])
        with pytest.raises(AttributeError):
            query.socket_timeout = 5000  # type: ignore[attr-defined]

    def test_sample_size_sets_max_records(self):
        query = _StrictQuery([_record("pk-1", {"x": 1})])
        construct_schema_aerospike(
            self._client_with_query(query),
            AerospikeSet(ns="t", set="s"),
            ".",
            sample_size=50,
        )
        assert query.max_records == 50

    def test_records_per_second_wired(self):
        query = _StrictQuery([])
        construct_schema_aerospike(
            self._client_with_query(query),
            AerospikeSet(ns="t", set="s"),
            ".",
            records_per_second=10,
        )
        assert query.records_per_second == 10

    def test_primary_key_synthesized_into_schema(self):
        query = _StrictQuery([_record("alice", {"age": 30})])
        schema = construct_schema_aerospike(
            self._client_with_query(query),
            AerospikeSet(ns="t", set="s"),
            ".",
        )
        assert ("PK",) in schema
        assert ("age",) in schema

    def test_query_exception_returns_empty_schema(self):
        # Network timeouts, auth failures, etc. during results() must not
        # crash ingestion — the per-set warning upstream handles it.
        query = MagicMock()
        query.results.side_effect = RuntimeError("connection lost")
        client = MagicMock()
        client.query.return_value = query

        schema = construct_schema_aerospike(client, AerospikeSet(ns="t", set="s"), ".")
        assert schema == {}


# --------------------------------------------------------------------------- #
# 5. Type mapping                                                             #
# --------------------------------------------------------------------------- #


class TestTypeMapping:
    def test_known_types_mapped(self, patched_aerospike_client):
        source = _make_source({"hosts": [("h", 3000)]})

        assert source.get_aerospike_type_string(int, "demo") == "integer"
        assert source.get_aerospike_type_string(str, "demo") == "string"
        assert source.get_aerospike_type_string(bool, "demo") == "boolean"
        assert source.get_aerospike_type_string(float, "demo") == "double"
        assert source.get_aerospike_type_string(list, "demo") == "list"
        assert source.get_aerospike_type_string(bytes, "demo") == "blob"

        assert isinstance(source.get_field_type(int, "demo").type, NumberTypeClass)
        assert isinstance(source.get_field_type(str, "demo").type, StringTypeClass)
        assert isinstance(source.get_field_type(bool, "demo").type, BooleanTypeClass)

    def test_unknown_type_string_is_unknown_and_warns(self, patched_aerospike_client):
        source = _make_source({"hosts": [("h", 3000)]})
        assert source.get_aerospike_type_string(complex, "demo") == "unknown"
        assert len(list(source.report.warnings)) >= 1

    def test_unknown_field_type_falls_back_to_null(self, patched_aerospike_client):
        source = _make_source({"hosts": [("h", 3000)]})
        result = source.get_field_type(complex, "demo")
        assert isinstance(result.type, NullTypeClass)


# --------------------------------------------------------------------------- #
# 6. _limit_schema_size                                                       #
# --------------------------------------------------------------------------- #


class TestLimitSchemaSize:
    """Two-stage truncation: first by nested depth (if depth != -1), then by
    field count (if size > max_schema_size). Both record their effects in the
    custom_properties dict so users can tell when a schema was truncated."""

    @staticmethod
    def _schema(
        *paths_with_counts: Tuple[str, int],
    ) -> Dict[Tuple[str, ...], Dict[str, Any]]:
        return {
            tuple(name.split(".")): _schema_entry(name, count)
            for name, count in paths_with_counts
        }

    def test_truncates_deeper_paths_when_depth_is_one(self, patched_aerospike_client):
        source = _make_source({"hosts": [("h", 3000)], "infer_schema_depth": 1})
        schema = self._schema(("a", 5), ("a.b", 3), ("a.b.c", 1))
        props: Dict[str, str] = {}

        result = source._limit_schema_size(schema, props)

        assert set(result.keys()) == {("a",)}
        assert props["schema.truncated"] == "True"
        assert props["schema.totalDepth"] == "3"

    def test_no_depth_truncation_when_minus_one(self, patched_aerospike_client):
        source = _make_source(
            {
                "hosts": [("h", 3000)],
                "infer_schema_depth": -1,
                "max_schema_size": 100,
            }
        )
        schema = self._schema(("a", 5), ("a.b", 3), ("a.b.c", 1))
        props: Dict[str, str] = {}

        result = source._limit_schema_size(schema, props)
        assert len(result) == 3
        assert "schema.truncated" not in props

    def test_downsamples_to_top_fields_by_count(self, patched_aerospike_client):
        source = _make_source(
            {
                "hosts": [("h", 3000)],
                "infer_schema_depth": -1,
                "max_schema_size": 2,
            }
        )
        schema = self._schema(("a", 1), ("b", 5), ("c", 3))
        props: Dict[str, str] = {}

        result = source._limit_schema_size(schema, props)

        # Top-2 by descending count: b (5), c (3)
        assert set(result.keys()) == {("b",), ("c",)}
        assert props["schema.downsampled"] == "True"
        assert props["schema.totalFields"] == "3"

    def test_no_downsample_when_under_max_size(self, patched_aerospike_client):
        source = _make_source(
            {
                "hosts": [("h", 3000)],
                "infer_schema_depth": -1,
                "max_schema_size": 10,
            }
        )
        schema = self._schema(("a", 1), ("b", 2))
        props: Dict[str, str] = {}

        result = source._limit_schema_size(schema, props)
        assert len(result) == 2
        assert "schema.downsampled" not in props


# --------------------------------------------------------------------------- #
# 7. get_sets + ignore_empty_sets                                             #
# --------------------------------------------------------------------------- #


class TestGetSets:
    """`info_random_node('sets')` returns 'sets\\t<entry>;<entry>;\\n'.
    The source strips the prefix/suffix and parses each entry."""

    SETS_INFO = "sets\tns=test:set=alpha:objects=10;ns=test:set=beta:objects=0;\n"

    def test_parses_info_response(self, patched_aerospike_client):
        _, client = patched_aerospike_client
        client.info_random_node.return_value = self.SETS_INFO

        source = _make_source({"hosts": [("h", 3000)]})
        sets = source.get_sets()

        assert {(s.ns, s.set, s.objects) for s in sets} == {
            ("test", "alpha", 10),
            ("test", "beta", 0),
        }

    def test_ignore_empty_sets_drops_zero_object_sets(self, patched_aerospike_client):
        _, client = patched_aerospike_client
        client.info_random_node.return_value = self.SETS_INFO

        source = _make_source({"hosts": [("h", 3000)], "ignore_empty_sets": True})
        sets = source.get_sets()
        assert {s.set for s in sets} == {"alpha"}

    def test_sets_info_failure_propagates_as_source_failure(
        self, patched_aerospike_client
    ):
        _, client = patched_aerospike_client
        client.info_random_node.side_effect = RuntimeError("node down")

        source = _make_source({"hosts": [("h", 3000)]})
        with pytest.raises(RuntimeError):
            source.get_sets()
        assert len(list(source.report.failures)) >= 1


# --------------------------------------------------------------------------- #
# 8. Namespace / set filtering                                                #
# --------------------------------------------------------------------------- #


class TestFiltering:
    def test_namespace_denied_yields_no_workunits(self, patched_aerospike_client):
        source = _make_source(
            {
                "hosts": [("h", 3000)],
                "namespace_pattern": {"allow": [".*"], "deny": ["secret"]},
            }
        )
        sets = [AerospikeSet(ns="secret", set="s1", objects=1)]

        workunits = list(source._get_namespace_workunits("secret", sets))
        assert workunits == []
        assert "secret" in list(source.report.filtered)

    def test_set_denied_is_filtered_but_namespace_emits(self, patched_aerospike_client):
        _, client = patched_aerospike_client
        client.query.return_value = _StrictQuery([])

        source = _make_source(
            {
                "hosts": [("h", 3000)],
                "set_pattern": {
                    "allow": [".*"],
                    "deny": [r"test\.private"],
                },
            }
        )
        sets = [
            AerospikeSet(ns="test", set="public", objects=1),
            AerospikeSet(ns="test", set="private", objects=1),
        ]

        list(source._get_namespace_workunits("test", sets))
        assert "test.private" in list(source.report.filtered)
        assert "test.public" not in list(source.report.filtered)


# --------------------------------------------------------------------------- #
# 9. XDR parsing: _get_dc_shipped_sets (pure) + xdr_sets (with mocked node)   #
# --------------------------------------------------------------------------- #


class TestGetDcShippedSets:
    """XDR decision matrix:
    enabled=false                     -> no sets ship
    ship-only-specified-sets=true     -> only shipped-sets ship
    ship-only-specified-sets=false    -> everything ships except ignored-sets
    """

    def test_disabled_dc_ships_nothing(self):
        assert (
            AerospikeSource._get_dc_shipped_sets(
                "enabled=false;ship-only-specified-sets=true;shipped-sets=foo,bar",
                sets=["foo", "bar", "baz"],
            )
            == []
        )

    def test_ship_only_specified_returns_only_listed(self):
        assert AerospikeSource._get_dc_shipped_sets(
            "enabled=true;ship-only-specified-sets=true;shipped-sets=alpha,beta",
            sets=["alpha", "beta", "gamma"],
        ) == ["alpha", "beta"]

    def test_ship_all_excludes_ignored_sets(self):
        assert AerospikeSource._get_dc_shipped_sets(
            "enabled=true;ship-only-specified-sets=false;ignored-sets=excluded",
            sets=["alpha", "excluded", "beta"],
        ) == ["alpha", "beta"]

    def test_ship_all_with_no_ignored_returns_all(self):
        assert AerospikeSource._get_dc_shipped_sets(
            "enabled=true;ship-only-specified-sets=false",
            sets=["alpha", "beta"],
        ) == ["alpha", "beta"]


class TestXdrSets:
    def test_returns_empty_mapping_when_no_dcs(self, patched_aerospike_client):
        _, client = patched_aerospike_client
        # The source splits on 'dcs=' then ';' then ',' — empty list of DCs
        # comes back as a single empty string.
        client.info_random_node.return_value = "xdr\tdcs=;\n"

        source = _make_source({"hosts": [("h", 3000)]})
        assert source.xdr_sets("test", ["s1", "s2"]) == {"s1": [], "s2": []}

    def test_per_dc_failure_recorded_as_warning_not_raised(
        self, patched_aerospike_client
    ):
        _, client = patched_aerospike_client

        def info(req: str) -> str:
            if "context=xdr" in req and "dc=" not in req:
                return "xdr\tdcs=DC1,DC2;\n"
            raise RuntimeError("DC unreachable")

        client.info_random_node.side_effect = info

        source = _make_source({"hosts": [("h", 3000)]})
        result = source.xdr_sets("test", ["s1"])

        assert result == {"s1": []}
        assert len(list(source.report.warnings)) >= 1

    def test_xdr_top_level_failure_recorded_as_warning(self, patched_aerospike_client):
        _, client = patched_aerospike_client
        client.info_random_node.side_effect = RuntimeError("xdr disabled")

        source = _make_source({"hosts": [("h", 3000)]})
        result = source.xdr_sets("test", ["s1", "s2"])

        # All sets fall back to empty DC lists; nothing raised.
        assert result == {"s1": [], "s2": []}
        assert len(list(source.report.warnings)) >= 1


# --------------------------------------------------------------------------- #
# 10. infer_schema_depth == 0 skips schema inference                          #
# --------------------------------------------------------------------------- #


class TestInferSchemaDepthZero:
    def test_zero_depth_skips_query(self, patched_aerospike_client):
        _, client = patched_aerospike_client

        source = _make_source({"hosts": [("h", 3000)], "infer_schema_depth": 0})
        from datahub.emitter.mcp_builder import NamespaceKey
        from datahub.metadata.schema_classes import SchemaMetadataClass

        ns_key = NamespaceKey(
            namespace="test",
            platform="aerospike",
            instance=None,
            env="PROD",
        )
        ds = source._generate_dataset(
            AerospikeSet(ns="test", set="demo", objects=10),
            namespace_key=ns_key,
            xdr_sets={},
        )

        # No schema inference -> client.query is never invoked and the
        # SchemaMetadata aspect is absent on the produced Dataset.
        client.query.assert_not_called()
        assert ds._get_aspect(SchemaMetadataClass) is None


# --------------------------------------------------------------------------- #
# 11. include_xdr toggle wires through to xdr_sets                            #
# --------------------------------------------------------------------------- #


class TestIncludeXdrToggle:
    def test_include_xdr_false_does_not_call_info(self, patched_aerospike_client):
        _, client = patched_aerospike_client
        client.query.return_value = _StrictQuery([])

        source = _make_source({"hosts": [("h", 3000)], "include_xdr": False})
        sets = [AerospikeSet(ns="test", set="demo", objects=1)]

        list(source._get_namespace_workunits("test", sets))

        # info_random_node should not be called for XDR config
        for call in client.info_random_node.call_args_list:
            assert "xdr" not in call.args[0]

    def test_include_xdr_true_invokes_xdr_lookup(self, patched_aerospike_client):
        _, client = patched_aerospike_client
        client.query.return_value = _StrictQuery([])
        client.info_random_node.return_value = "xdr\tdcs=;\n"

        source = _make_source({"hosts": [("h", 3000)], "include_xdr": True})
        sets = [AerospikeSet(ns="test", set="demo", objects=1)]

        list(source._get_namespace_workunits("test", sets))

        xdr_calls = [
            call
            for call in client.info_random_node.call_args_list
            if "xdr" in call.args[0]
        ]
        assert len(xdr_calls) >= 1
