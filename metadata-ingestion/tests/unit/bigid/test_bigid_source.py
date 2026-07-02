import json
import time
from typing import Any, Dict, List, cast
from unittest.mock import MagicMock, patch

import pytest
import time_machine

from datahub.emitter.mce_builder import datahub_guid, make_term_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import DomainKey
from datahub.ingestion.source.bigid.bigid_api import BigIDAPIError
from datahub.ingestion.source.bigid.bigid_source import BigIDSource
from datahub.ingestion.source.bigid.bigid_utils import (
    _is_idsor_attr,
    _tag_display_name,
)
from datahub.ingestion.source.bigid.config import BigIDSourceConfig
from datahub.ingestion.source.bigid.constants import (
    BIGID_IDSOR_GLOSSARY_NODE_URN,
    BIGID_PLATFORM_NAME,
)
from datahub.ingestion.source.bigid.models import (
    BigIDAttributeDetail,
    BigIDCatalogObject,
    BigIDColumn,
    BigIDConnection,
    BigIDGlossaryItem,
    IDSoRAttributeInfo,
)
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    GlossaryTermAssociationClass,
    MetadataAttributionClass,
    MetadataChangeProposalClass,
)

FROZEN_TIME = "2026-01-15T00:00:00Z"


def _term_urn(suffix: str) -> str:
    """Expected GlossaryTerm URN for a `bigid.<suffix>` path (opaque GUID; derived like the source)."""
    return make_term_urn(datahub_guid({"path": f"bigid.{suffix}"}))


# ---------------------------------------------------------------------------
# Typed accessors — workunit.metadata is a MCE|MCP|MCPW union and aspects are
# Optional. The connector emits both MCPW (SDK/typed aspects) and raw MCP
# (JSON-patch ops), so these narrow away the MCE case and expose the shared
# proposal attributes so mypy can check the assertion bodies.
# ---------------------------------------------------------------------------


def _mcp(wu: Any) -> Any:
    md = wu.metadata
    assert isinstance(md, (MetadataChangeProposalClass, MetadataChangeProposalWrapper))
    return md


def _aspect(wu: Any) -> Any:
    return _mcp(wu).aspect


def _aspect_name(wu: Any) -> Any:
    return _mcp(wu).aspectName


def _change_type(wu: Any) -> Any:
    return _mcp(wu).changeType


def _entity_urn(wu: Any) -> str:
    urn = _mcp(wu).entityUrn
    assert urn is not None
    return urn


def _attribution(assoc: GlossaryTermAssociationClass) -> MetadataAttributionClass:
    assert assoc.attribution is not None
    return assoc.attribution


def _client(source: BigIDSource) -> MagicMock:
    return cast(MagicMock, source.client)


# ---------------------------------------------------------------------------
# Model builders — the connector consumes typed models (validated at the
# BigIDClient boundary), so tests build the same models from raw dicts that
# mirror the BigID API JSON.
# ---------------------------------------------------------------------------


def _columns(raw: List[Dict[str, Any]]) -> List[BigIDColumn]:
    return [BigIDColumn.model_validate(item) for item in raw]


def _attr(raw: Dict[str, Any]) -> BigIDAttributeDetail:
    return BigIDAttributeDetail.model_validate(raw)


def _catalog_object(raw: Dict[str, Any]) -> BigIDCatalogObject:
    return BigIDCatalogObject.model_validate(raw)


def _catalog_objects(raw: List[Dict[str, Any]]) -> List[BigIDCatalogObject]:
    return [BigIDCatalogObject.model_validate(item) for item in raw]


def _connections(raw: List[Dict[str, Any]]) -> List[BigIDConnection]:
    return [BigIDConnection.model_validate(item) for item in raw]


def _glossary_item(raw: Dict[str, Any]) -> BigIDGlossaryItem:
    return BigIDGlossaryItem.model_validate(raw)


def _glossary_items(raw: List[Dict[str, Any]]) -> List[BigIDGlossaryItem]:
    return [BigIDGlossaryItem.model_validate(item) for item in raw]


def _patch_ops(workunits: list) -> List[Dict[str, Any]]:
    # Field enrichment is emitted as editableSchemaMetadata PATCH ops (JSON-Pointer list).
    ops: List[Dict[str, Any]] = []
    for wu in workunits:
        value = getattr(_aspect(wu), "value", None)
        if value is not None:
            ops.extend(json.loads(value.decode()))
    return ops


def _patched_field_terms(
    workunits: list, field_path: str
) -> List[GlossaryTermAssociationClass]:
    prefix = f"/editableSchemaFieldInfo/{field_path}/glossaryTerms/terms/"
    return [
        GlossaryTermAssociationClass.from_obj(op["value"])
        for op in _patch_ops(workunits)
        if op["path"].startswith(prefix)
    ]


def _patched_field_tag_urns(workunits: list, field_path: str) -> List[str]:
    prefix = f"/editableSchemaFieldInfo/{field_path}/globalTags/tags/"
    return [
        op["value"]["tag"]
        for op in _patch_ops(workunits)
        if op["path"].startswith(prefix)
    ]


@pytest.fixture(autouse=True)
def freeze_time():
    with time_machine.travel(FROZEN_TIME, tick=False):
        yield


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(**overrides: Any) -> Dict[str, Any]:
    base = {
        "bigid_url": "https://bigid.example.com",
        "access_token": "test-token",
        "env": "PROD",
    }
    base.update(overrides)
    return base


def _make_source(**config_overrides: Any) -> BigIDSource:
    """Create a BigIDSource with a mocked BigIDClient."""
    config_dict = _make_config(**config_overrides)
    ctx = MagicMock()
    ctx.pipeline_name = "test"
    ctx.run_id = "test-run"

    with patch("datahub.ingestion.source.bigid.bigid_source.BigIDClient") as MockClient:
        instance = MockClient.return_value
        instance.test_connection.return_value = True
        source = BigIDSource(BigIDSourceConfig.model_validate(config_dict), ctx)
        source.client = instance
    return source


# ---------------------------------------------------------------------------
# Platform auto-detection
# ---------------------------------------------------------------------------


def test_load_registries_populates_platform_map():
    source = _make_source()
    _client(source).get_connections.return_value = _connections(
        [
            {"name": "my_snowflake", "type": "snowflake"},
            {"name": "my_mysql", "type": "rdb-mysql"},
        ]
    )
    _client(source).get_all_classifications.return_value = []
    _client(source).get_glossary_items.return_value = []

    source._load_registries()

    assert source._platform_map["my_snowflake"] == "snowflake"
    assert source._platform_map["my_mysql"] == "mysql"


def test_load_registries_explicit_override_wins():
    source = _make_source(
        datasource_platform_mapping={
            "my_conn": {"platform": "bigquery", "env": "DEV"},
        }
    )
    _client(source).get_connections.return_value = _connections(
        [
            {"name": "my_conn", "type": "rdb-mysql"},  # would auto-detect as mysql
        ]
    )
    _client(source).get_all_classifications.return_value = []
    _client(source).get_glossary_items.return_value = []

    source._load_registries()

    assert source._platform_map["my_conn"] == "bigquery"


def test_load_registries_unknown_type_warns():
    source = _make_source()
    _client(source).get_connections.return_value = _connections(
        [
            {"name": "exotic_conn", "type": "some-unknown-type"},
        ]
    )
    _client(source).get_all_classifications.return_value = []
    _client(source).get_glossary_items.return_value = []

    source._load_registries()

    assert "exotic_conn" in list(source.report.connections_without_platform)
    # Warning must appear in the ingestion report (not just logger) so the UI surfaces it
    warning_contexts = [c for w in source.report.warnings for c in w.context]
    assert any("some-unknown-type" in c for c in warning_contexts), (
        f"Expected unknown-connection-type in report warning context; got: {warning_contexts}"
    )


# ---------------------------------------------------------------------------
# URN construction
# ---------------------------------------------------------------------------


def test_make_dataset_urn_mysql_three_part_fqn():
    source = _make_source()
    source._platform_map["employees_db"] = "mysql"

    urn = source._make_dataset_urn("employees_db.hr_schema.employees", "employees_db")
    assert urn == "urn:li:dataset:(urn:li:dataPlatform:mysql,hr_schema.employees,PROD)"


def test_make_dataset_urn_snowflake_lowercased():
    source = _make_source()
    source._platform_map["SNOWFLAKE_PROD"] = "snowflake"

    urn = source._make_dataset_urn(
        "SNOWFLAKE_PROD.MY_DB.MY_SCHEMA.MY_TABLE", "SNOWFLAKE_PROD"
    )
    assert (
        urn
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.my_table,PROD)"
    )


def test_make_dataset_urn_with_platform_instance():
    source = _make_source(
        datasource_platform_mapping={
            "sf_conn": {"platform": "snowflake", "platform_instance": "prod_acct"},
        }
    )
    source._platform_map["sf_conn"] = "snowflake"

    urn = source._make_dataset_urn("sf_conn.mydb.myschema.mytable", "sf_conn")
    assert urn is not None
    assert "prod_acct.mydb.myschema.mytable" in urn


def test_make_dataset_urn_connection_name_with_dots():
    """Connection names that contain dots are stripped correctly via prefix match."""
    source = _make_source()
    source._platform_map["sales.prod.db"] = "mysql"

    urn = source._make_dataset_urn("sales.prod.db.schema.table", "sales.prod.db")
    assert urn == "urn:li:dataset:(urn:li:dataPlatform:mysql,schema.table,PROD)"


def test_make_dataset_urn_empty_inputs():
    source = _make_source()
    assert source._make_dataset_urn("", "conn") is None
    assert source._make_dataset_urn("fqn", "") is None


def test_make_dataset_urn_encodes_parentheses_in_table_name():
    """Parentheses in table names must be percent-encoded to avoid breaking URN parsing."""
    source = _make_source()
    source._platform_map["my_db"] = "mysql"

    urn = source._make_dataset_urn("my_db.schema.table(v2)", "my_db")
    assert urn == "urn:li:dataset:(urn:li:dataPlatform:mysql,schema.table%28v2%29,PROD)"


def test_make_dataset_urn_encodes_reserved_chars_like_native_connectors():
    """Commas (tuple delimiters) are encoded but a literal colon is left untouched,
    matching the native connector's URN so enrichment lands on the same entity."""
    source = _make_source()
    source._platform_map["my_db"] = "mysql"

    urn = source._make_dataset_urn("my_db.schema,v2:prod.table", "my_db")
    assert urn is not None
    assert "%2C" in urn  # comma encoded
    assert "%3A" not in urn  # colon left literal, matching native connectors
    assert "schema%2Cv2:prod.table" in urn


# ---------------------------------------------------------------------------
# Classification resolution
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("glossary_id_map", "attr_name", "name_map", "expected"),
    [
        # classifier.* resolved via the all-classifications map
        (
            {"classifier.Email": "fn_ootb_email"},
            "classifier.Email",
            {},
            "fn_ootb_email",
        ),
        # MD:: metadata classifiers keep their full key in the map
        (
            {"classifier.MD::Postal Code - MD": "fn_postal_code"},
            "classifier.MD::Postal Code - MD",
            {},
            "fn_postal_code",
        ),
        # businessTerm.* resolved via the glossary name lookup
        ({}, "businessTerm.First Name", {"First Name": "bt_item_HK2A"}, "bt_item_HK2A"),
        ({}, "unknownPrefix.Something", {}, None),  # unrecognised prefix
        ({}, "classifier.NonExistent", {}, None),  # classifier absent from map
    ],
)
def test_resolve_attr_to_glossary_id(glossary_id_map, attr_name, name_map, expected):
    source = _make_source()
    source._glossary_id_map.update(glossary_id_map)
    assert source._resolve_attr_to_glossary_id(attr_name, name_map) == expected


# ---------------------------------------------------------------------------
# Confidence threshold filtering
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("threshold", "ranks", "expected_below", "expected_workunits"),
    [
        # HIGH threshold: the MEDIUM finding is filtered, HIGH passes (two findings
        # on one classifier so only the surviving HIGH yields an association)
        (0.75, ["MEDIUM", "HIGH"], 1, 1),
        (0.0, ["LOW"], 0, 1),  # threshold 0 lets even LOW through
        (0.50, ["MEDIUM"], 0, 1),  # MEDIUM (0.50) sits on the boundary — inclusive
        (0.51, ["MEDIUM"], 1, 0),  # just above MEDIUM's score filters it
    ],
)
def test_confidence_threshold_filtering(
    threshold, ranks, expected_below, expected_workunits
):
    source = _make_source(minimum_confidence_threshold=threshold)
    source._glossary_id_map["classifier.Email"] = "fn_email"

    columns = [
        {
            "columnName": "email",
            "fieldType": "varchar",
            "attributeDetails": [
                {"name": "classifier.Email", "ranks": [rank], "type": "Classification"}
                for rank in ranks
            ],
            "fieldClassifications": [],
            "columnProfile": {},
        }
    ]
    workunits = list(
        source._emit_schema_field_enrichment(
            "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)",
            _columns(columns),
            "my_conn",
            {},
            int(time.time() * 1000),
        )
    )
    assert source.report.findings_below_threshold == expected_below
    assert len(workunits) == expected_workunits


# ---------------------------------------------------------------------------
# GlossaryTerm emission
# ---------------------------------------------------------------------------


def test_glossary_term_urn_uses_glossary_id():
    source = _make_source()
    item = _glossary_item({"glossary_id": "bt_item_HK2A", "name": "First Name"})
    assert source._glossary_term_urn(item) == _term_urn("bt_item_HK2A")


def test_glossary_term_urn_falls_back_to_name_slug():
    source = _make_source()
    item = _glossary_item({"glossary_id": "", "name": "My Custom Term"})
    assert source._glossary_term_urn(item) == _term_urn("my_custom_term")


def test_should_include_item_respects_type_filter():
    source = _make_source(item_types=["Business Term"])
    assert source._should_include_item(
        _glossary_item({"type": "Business Term", "glossary_id": "x"})
    )
    assert not source._should_include_item(
        _glossary_item({"type": "Personal Data Category", "glossary_id": "y"})
    )


def test_should_include_ootb_personal_data_item_regardless_of_filter():
    """OOTB Personal Data Items are always included — column enrichment references their URNs."""
    source = _make_source(item_types=["Business Term"])  # PDI not in allow-list
    item = _glossary_item(
        {
            "type": "Personal Data Item",
            "is_ootb": True,
            "glossary_id": "fn_ootb_email",
        }
    )
    assert source._should_include_item(item)


def test_emit_glossary_terms_custom_properties():
    source = _make_source()
    source._glossary_items = _glossary_items(
        [
            {
                "_id": "abc123",
                "glossary_id": "bt_HK1",
                "type": "Business Term",
                "name": "Customer ID",
                "description": "Unique customer identifier",
                "is_ootb": False,
                "update_date": "2024-01-01T00:00:00Z",
            }
        ]
    )

    workunits = list(source._emit_glossary_terms())
    assert len(workunits) >= 2

    term_info_wu = next(
        wu
        for wu in workunits
        if hasattr(wu.metadata, "aspect")
        and "GlossaryTermInfo" in type(_aspect(wu)).__name__
    )
    aspect = _aspect(term_info_wu)
    assert aspect.name == "Customer ID"
    assert aspect.definition == "Unique customer identifier"
    assert aspect.customProperties["bigid_type"] == "Business Term"
    assert aspect.customProperties["bigid_is_ootb"] == "false"
    assert aspect.customProperties["bigid_glossary_id"] == "bt_HK1"
    assert aspect.customProperties["bigid_id"] == "abc123"


def test_emit_glossary_terms_emits_ownership_when_owner_set():
    source = _make_source(owner_type="user")
    source._glossary_items = _glossary_items(
        [
            {
                "_id": "abc",
                "glossary_id": "bt_1",
                "type": "Business Term",
                "name": "Term A",
                "owner": "alice",
            }
        ]
    )

    workunits = list(source._emit_glossary_terms())
    wu_types = [type(_aspect(wu)).__name__ for wu in workunits]
    assert "OwnershipClass" in wu_types


def test_emit_glossary_terms_no_ownership_when_owner_type_none():
    source = _make_source(owner_type="none")
    source._glossary_items = _glossary_items(
        [
            {
                "_id": "abc",
                "glossary_id": "bt_1",
                "type": "Business Term",
                "name": "Term A",
                "owner": "alice",
            }
        ]
    )

    workunits = list(source._emit_glossary_terms())
    wu_types = [type(_aspect(wu)).__name__ for wu in workunits]
    assert "OwnershipClass" not in wu_types


# ---------------------------------------------------------------------------
# Tag routing
# ---------------------------------------------------------------------------


def _tag_props(workunits):
    return next(_aspect(wu) for wu in workunits if _aspect_name(wu) == "tagProperties")


def test_tag_routing_sensitivity_classification_emits_tag():
    source = _make_source()
    workunits = list(
        source._emit_tag_entity(
            "system.sensitivityClassification.Sensitivity", "Confidential"
        )
    )
    assert len(workunits) == 2  # TagProperties + Status
    urn = "urn:li:tag:bigid.system.sensitivityClassification.Sensitivity:Confidential"
    assert all(_entity_urn(wu) == urn for wu in workunits)
    props = _tag_props(workunits)
    assert props.name == "Sensitivity : Confidential"
    assert props.description == "system.sensitivityClassification.Sensitivity"


def test_tag_urn_preserves_system_prefix():
    source = _make_source()
    workunits = list(source._emit_tag_entity("system.risk.riskGroup", "High"))
    assert all(
        "bigid.system.risk.riskGroup:High" in _entity_urn(wu) for wu in workunits
    )
    props = _tag_props(workunits)
    assert props.name == "riskGroup : High"
    assert props.description == "system.risk.riskGroup"


@pytest.mark.parametrize(
    ("tag_name", "tag_value", "expected"),
    [
        # system.* prefix stripped, leaf name retained
        (
            "system.sensitivityClassification.Sensitivity",
            "Restricted",
            "Sensitivity : Restricted",
        ),
        ("system.risk.riskGroup", "high", "riskGroup : high"),
        # user-defined names (not system.*) are preserved verbatim, dots and all
        ("Sen.Priority", "P3", "Sen.Priority : P3"),
        ("Sensitivity", "Confidential", "Sensitivity : Confidential"),
    ],
)
def test_tag_display_name(tag_name, tag_value, expected):
    assert _tag_display_name(tag_name, tag_value) == expected


# ---------------------------------------------------------------------------
# riskScore → StructuredProperty (PATCH)
# ---------------------------------------------------------------------------


def test_risk_score_emitted_as_patch_not_upsert():
    """riskScore must be emitted via PATCH so it doesn't overwrite other structured properties."""
    source = _make_source()
    source._platform_map["my_conn"] = "postgres"
    obj = {
        "fullyQualifiedName": "my_conn.schema.table",
        "source": "my_conn",
        "objectName": "table",
        "scanner_type_group": "structured",
        "tags": [
            {
                "tagName": "system.risk.riskScore",
                "tagValue": "72",
                "tagType": "OBJECT",
                "properties": {"hidden": False, "applicationType": "risk"},
            }
        ],
    }
    _client(source).get_catalog_objects.side_effect = lambda: iter(
        _catalog_objects([obj])
    )
    _client(source).get_columns.return_value = []

    workunits = list(source._process_catalog())
    risk_wus = [wu for wu in workunits if _aspect_name(wu) == "structuredProperties"]

    assert len(risk_wus) == 1, (
        "Expected exactly one structuredProperties workunit for riskScore"
    )
    assert _change_type(risk_wus[0]) == ChangeTypeClass.PATCH, (
        "riskScore must use PATCH to avoid clobbering user-defined structured properties"
    )


def test_risk_score_non_numeric_skipped():
    """Non-numeric riskScore tag values must be dropped with a warning."""
    source = _make_source()
    source._platform_map["my_conn"] = "postgres"
    obj = {
        "fullyQualifiedName": "my_conn.schema.table",
        "source": "my_conn",
        "objectName": "table",
        "scanner_type_group": "structured",
        "tags": [
            {
                "tagName": "system.risk.riskScore",
                "tagValue": "n/a",
                "tagType": "OBJECT",
                "properties": {"hidden": False, "applicationType": "risk"},
            }
        ],
    }
    _client(source).get_catalog_objects.side_effect = lambda: iter(
        _catalog_objects([obj])
    )
    _client(source).get_columns.return_value = []

    workunits = list(source._process_catalog())
    risk_wus = [wu for wu in workunits if _aspect_name(wu) == "structuredProperties"]

    assert risk_wus == [], (
        "Non-numeric riskScore must produce no structuredProperties workunit"
    )


# ---------------------------------------------------------------------------
# Domain resolution
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("config", "domain_val", "sub_domain_val", "expected"),
    [
        # auto_namespaced: sub-domain wins over domain when both present.
        # The hashed name is env-scoped (default env=PROD) so domains never merge
        # across environments; see test_domain_urn_scoped_by_env_and_instance.
        (
            {"domain_mode": "auto_namespaced"},
            "Customer",
            "Identity",
            DomainKey(name="PROD/Identity", platform=BIGID_PLATFORM_NAME).as_urn(),
        ),
        (
            {"domain_mode": "auto_namespaced"},
            "Customer",
            "",
            DomainKey(name="PROD/Customer", platform=BIGID_PLATFORM_NAME).as_urn(),
        ),
        ({"domain_mode": "none"}, "Customer", "Identity", None),
        # config_map: exact key mapping wins
        (
            {
                "domain_mode": "config_map",
                "domain_mapping": {"Identity": "urn:li:domain:existing-identity"},
            },
            "Customer",
            "Identity",
            "urn:li:domain:existing-identity",
        ),
        # config_map: keys absent from the mapping resolve to no domain
        (
            {
                "domain_mode": "config_map",
                "domain_mapping": {"Finance": "urn:li:domain:finance"},
            },
            "Customer",
            "",
            None,
        ),
        (
            {
                "domain_mode": "config_map",
                "domain_mapping": {"Finance": "urn:li:domain:finance"},
            },
            "Customer",
            "Identity",
            None,
        ),
    ],
)
def test_resolve_domain_urn(config, domain_val, sub_domain_val, expected):
    source = _make_source(**config)
    assert source._resolve_domain_urn(domain_val, sub_domain_val) == expected


def test_domain_urn_scoped_by_env_and_instance():
    """The same BigID domain name must resolve to distinct urn:li:domain entities
    across different env / platform_instance so they never silently merge."""
    prod = _make_source(env="PROD")
    dev = _make_source(env="DEV")
    prod_inst = _make_source(env="PROD", platform_instance="acct1")

    prod_urn = prod._domain_urn("Finance")
    dev_urn = dev._domain_urn("Finance")
    prod_inst_urn = prod_inst._domain_urn("Finance")

    # All three are distinct: env and platform_instance each change the GUID.
    assert len({prod_urn, dev_urn, prod_inst_urn}) == 3
    # Deterministic: same config → same URN.
    assert prod_urn == _make_source(env="PROD")._domain_urn("Finance")


def test_emit_glossary_terms_config_map_missing_key_no_domain_aspect():
    """A glossary term with an unmapped domain must not emit a DomainsClass aspect."""
    source = _make_source(
        domain_mode="config_map",
        domain_mapping={"Finance": "urn:li:domain:finance"},
    )
    source._glossary_items = _glossary_items(
        [
            {
                "_id": "x",
                "glossary_id": "bt_1",
                "type": "Business Term",
                "name": "Customer ID",
                "domain": "Customer",  # not in mapping
            }
        ]
    )

    workunits = list(source._emit_glossary_terms())
    aspect_names = {type(_aspect(wu)).__name__ for wu in workunits}
    assert "DomainsClass" not in aspect_names


# ---------------------------------------------------------------------------
# DatasetProfile derivation
# ---------------------------------------------------------------------------


def test_emit_dataset_profile_numeric_column():
    source = _make_source()
    columns = [
        {
            "columnName": "salary",
            "columnProfile": {
                "fieldCount": 1000,
                "distinctPct": 80.0,
                "emptyPct": 2.5,
                "inferredDataType": "numeric",
                "avgNum": 75000.0,
                "numDev": 12000.0,
                "minNum": 30000,
                "maxNum": 200000,
            },
        }
    ]
    workunits = list(
        source._emit_dataset_profile(
            "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)",
            _columns(columns),
            int(time.time() * 1000),
        )
    )
    assert len(workunits) == 1
    profile = _aspect(workunits[0])
    assert profile.rowCount is None  # fieldCount is scan sample size, not row count
    assert profile.columnCount == 1

    fp = profile.fieldProfiles[0]
    assert fp.fieldPath == "salary"
    assert abs(fp.uniqueProportion - 0.80) < 0.001
    assert fp.uniqueCount == 800
    assert abs(fp.nullProportion - 0.025) < 0.001
    assert fp.nullCount == 25
    assert fp.min == "30000"
    assert fp.max == "200000"
    assert fp.mean == "75000.0"
    assert fp.stdev == "12000.0"
    assert fp.sampleValues is None  # numeric columns have no sampleValues


def test_emit_dataset_profile_textual_column():
    source = _make_source()
    columns = [
        {
            "columnName": "first_name",
            "columnProfile": {
                "fieldCount": 500,
                "distinctPct": 95.0,
                "emptyPct": 0.0,
                "inferredDataType": "text",
                "minLexStr": "Aaron",
                "maxLexStr": "Zara",
            },
        }
    ]
    workunits = list(
        source._emit_dataset_profile(
            "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)",
            _columns(columns),
            int(time.time() * 1000),
        )
    )
    fp = _aspect(workunits[0]).fieldProfiles[0]
    assert fp.min == "Aaron"
    assert fp.max == "Zara"
    assert fp.sampleValues == ["Aaron", "Zara"]
    assert fp.mean is None
    assert fp.stdev is None


def test_emit_dataset_profile_empty_column_profile_skipped():
    source = _make_source()
    columns = [{"columnName": "col_no_profile", "columnProfile": {}}]
    workunits = list(
        source._emit_dataset_profile(
            "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)",
            _columns(columns),
            int(time.time() * 1000),
        )
    )
    # Profile is emitted but no fieldProfiles since columnProfile was empty
    assert len(workunits) == 1
    profile = _aspect(workunits[0])
    assert profile.fieldProfiles is None


# ---------------------------------------------------------------------------
# MetadataAttribution sourceDetail
# ---------------------------------------------------------------------------


def test_source_detail_includes_friendly_name_for_high_only():
    """HIGH-confidence findings get classifier_friendly_name in sourceDetail; MEDIUM does not.

    Uses two *distinct* classifiers (Email and Phone) so neither is deduplicated.
    """
    source = _make_source(minimum_confidence_threshold=0.0)
    source._glossary_id_map["classifier.Email"] = "fn_email"
    source._friendly_name_map["fn_email"] = "Email Address"
    source._glossary_id_map["classifier.Phone"] = "fn_phone"
    source._friendly_name_map["fn_phone"] = "Phone Number"

    columns = [
        {
            "columnName": "contact",
            "fieldType": "varchar",
            "attributeDetails": [
                {
                    "name": "classifier.Email",
                    "ranks": ["HIGH"],
                    "type": "Classification",
                },
                {
                    "name": "classifier.Phone",
                    "ranks": ["MEDIUM"],
                    "type": "Classification",
                },
            ],
            "fieldClassifications": [],
            "columnProfile": {},
        }
    ]
    now_ms = int(time.time() * 1000)
    workunits = list(
        source._emit_schema_field_enrichment(
            "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)",
            _columns(columns),
            "my_conn",
            {},
            now_ms,
        )
    )
    assert len(workunits) == 1
    terms = _patched_field_terms(workunits, "contact")
    assert len(terms) == 2

    high_assoc = next(
        t for t in terms if _attribution(t).sourceDetail["confidence_level"] == "HIGH"
    )
    med_assoc = next(
        t for t in terms if _attribution(t).sourceDetail["confidence_level"] == "MEDIUM"
    )

    assert (
        _attribution(high_assoc).sourceDetail["classifier_friendly_name"]
        == "Email Address"
    )
    assert "classifier_friendly_name" not in _attribution(med_assoc).sourceDetail


def test_source_detail_attribution_fields_populated():
    source = _make_source()
    source._glossary_id_map["classifier.Email"] = "fn_email"

    columns = [
        {
            "columnName": "email_col",
            "fieldType": "varchar",
            "attributeDetails": [
                {
                    "name": "classifier.Email",
                    "ranks": ["HIGH"],
                    "type": "Classification",
                },
            ],
            "fieldClassifications": [
                {
                    "classificationName": "Email",
                    "rowsOrFieldsCounter": 1500,
                    "distinctValueCount": 900,
                }
            ],
            "columnProfile": {},
        }
    ]
    now_ms = int(time.time() * 1000)
    workunits = list(
        source._emit_schema_field_enrichment(
            "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)",
            _columns(columns),
            "sales_conn",
            {},
            now_ms,
        )
    )
    assert len(workunits) == 1
    terms = _patched_field_terms(workunits, "email_col")
    sd = _attribution(terms[0]).sourceDetail

    assert sd["classifier_name"] == "classifier.Email"
    assert sd["classifier_type"] == "value"
    assert sd["confidence_level"] == "HIGH"
    assert sd["bigid_connection"] == "sales_conn"
    assert sd["row_count"] == "1500"
    assert sd["distinct_count"] == "900"
    assert _attribution(terms[0]).source == "urn:li:dataPlatform:bigid"
    assert _attribution(terms[0]).actor == "urn:li:corpuser:datahub"


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


def test_process_catalog_object_column_fetch_failure_continues():
    """A BigIDAPIError from get_columns must be warned and processing must continue."""
    source = _make_source()
    source._platform_map["my_conn"] = "postgres"
    _client(source).get_columns.side_effect = BigIDAPIError("connection timed out")

    obj = {
        "fullyQualifiedName": "my_conn.public.orders",
        "source": "my_conn",
        "objectName": "orders",
        "scanner_type_group": "structured",
        "tags": [],
    }

    workunits = list(source._process_catalog_object(_catalog_object(obj), {}))

    # Enrichment counter incremented — object was not skipped
    assert source.report.datasets_enriched == 1

    # Column-dependent aspects must not be emitted
    aspect_names = {type(_aspect(wu)).__name__ for wu in workunits}
    assert "EditableSchemaMetadataClass" not in aspect_names
    assert "DatasetProfileClass" not in aspect_names

    # dataPlatformInstance is not emitted without platform_instance config
    assert "DataPlatformInstanceClass" not in aspect_names


# ---------------------------------------------------------------------------
# _load_registries error paths
# ---------------------------------------------------------------------------


def test_load_registries_connections_api_error_warns():
    source = _make_source()
    _client(source).get_connections.side_effect = BigIDAPIError("timeout")
    _client(source).get_all_classifications.return_value = []
    _client(source).get_glossary_items.return_value = []

    source._load_registries()

    assert source._platform_map == {}
    assert any("ds-connections" in str(w) for w in source.report.warnings)


def test_load_registries_classifications_api_error_warns():
    source = _make_source()
    _client(source).get_connections.return_value = []
    _client(source).get_all_classifications.side_effect = BigIDAPIError("timeout")
    _client(source).get_glossary_items.return_value = []

    source._load_registries()

    assert source._glossary_id_map == {}
    assert any("all-classifications" in str(w) for w in source.report.warnings)


def test_load_registries_glossary_items_api_error_warns():
    source = _make_source()
    _client(source).get_connections.return_value = []
    _client(source).get_all_classifications.return_value = []
    _client(source).get_glossary_items.side_effect = BigIDAPIError("timeout")

    source._load_registries()

    assert source._glossary_items == []
    assert any("glossary-items" in str(w) for w in source.report.warnings)


def test_load_registries_idsor_attributes_api_error_warns():
    source = _make_source()
    _client(source).get_connections.return_value = []
    _client(source).get_all_classifications.return_value = []
    _client(source).get_glossary_items.return_value = []
    _client(source).get_idsor_attribute_map.side_effect = BigIDAPIError("timeout")

    source._load_registries()

    assert source._idsor_attr_map == {}
    assert any("idsor" in str(w) for w in source.report.warnings)


# ---------------------------------------------------------------------------
# _process_catalog pass-1 tag filtering
# ---------------------------------------------------------------------------


def test_process_catalog_excludes_hidden_tags():
    source = _make_source()
    source._platform_map["my_conn"] = "mysql"
    obj = {
        "fullyQualifiedName": "my_conn.schema.table",
        "source": "my_conn",
        "objectName": "table",
        "scanner_type_group": "structured",
        "tags": [
            {
                "tagName": "Classification",
                "tagValue": "PII",
                "properties": {"hidden": True, "applicationType": "classification"},
            }
        ],
    }
    _client(source).get_catalog_objects.side_effect = lambda: iter(
        _catalog_objects([obj])
    )
    _client(source).get_columns.return_value = []

    with patch.object(source, "_emit_tag_entity") as mock_emit:
        list(source._process_catalog())

    mock_emit.assert_not_called()


def test_process_catalog_excludes_wrong_application_type():
    source = _make_source()
    source._platform_map["my_conn"] = "mysql"
    obj = {
        "fullyQualifiedName": "my_conn.schema.table",
        "source": "my_conn",
        "objectName": "table",
        "scanner_type_group": "structured",
        "tags": [
            {
                "tagName": "Classification",
                "tagValue": "PII",
                "tagType": "OBJECT",
                "properties": {"hidden": False, "applicationType": "unknownType"},
            }
        ],
    }
    _client(source).get_catalog_objects.side_effect = lambda: iter(
        _catalog_objects([obj])
    )
    _client(source).get_columns.return_value = []

    with patch.object(source, "_emit_tag_entity") as mock_emit:
        list(source._process_catalog())

    mock_emit.assert_not_called()


def test_process_catalog_excludes_field_type_tags():
    """FIELD-scoped tags must not become dataset Tag entities.

    Column enrichment references confidence tags and terms from attributeDetails,
    never these tags, so emitting them would create orphaned TagProperties entities.
    """
    source = _make_source()
    source._platform_map["my_conn"] = "mysql"
    obj = {
        "fullyQualifiedName": "my_conn.schema.table",
        "source": "my_conn",
        "objectName": "table",
        "scanner_type_group": "structured",
        "tags": [
            {
                "tagName": "Sensitivity",
                "tagValue": "PII",
                "tagType": "FIELD",
                "properties": {
                    "hidden": False,
                    "applicationType": "sensitivityClassification",
                },
            }
        ],
    }
    _client(source).get_catalog_objects.side_effect = lambda: iter(
        _catalog_objects([obj])
    )
    _client(source).get_columns.return_value = []

    with patch.object(source, "_emit_tag_entity") as mock_emit:
        list(source._process_catalog())

    mock_emit.assert_not_called()


def test_connection_pattern_filters_denied_objects():
    """Objects whose source connection is denied by connection_pattern are skipped."""
    source = _make_source(connection_pattern={"deny": ["^sandbox-.*"]})
    source._platform_map["sandbox-db"] = "mysql"
    source._platform_map["prod-db"] = "mysql"

    denied = {
        "fullyQualifiedName": "sandbox-db.s.t",
        "source": "sandbox-db",
        "objectName": "t",
        "scanner_type_group": "structured",
        "tags": [],
    }
    allowed = {
        "fullyQualifiedName": "prod-db.s.t",
        "source": "prod-db",
        "objectName": "t",
        "scanner_type_group": "structured",
        "tags": [],
    }
    _client(source).get_catalog_objects.side_effect = lambda: iter(
        _catalog_objects([denied, allowed])
    )
    _client(source).get_columns.return_value = []

    list(source._process_catalog())

    assert source.report.objects_filtered_by_connection == 1
    assert "sandbox-db" in list(source.report.filtered_connections)
    # Only the allowed object was enriched
    assert source.report.datasets_enriched == 1


# ---------------------------------------------------------------------------
# Confidence-level tag deduplication (per field)
# ---------------------------------------------------------------------------


def test_confidence_level_tag_deduplicated_per_field():
    """Two same-rank findings on one field must yield a single confidence tag URN.

    GMS rejects a GlobalTagsClass containing duplicate tag URNs, so the field's
    tag list must be deduplicated even when multiple classifiers share a rank.
    """
    source = _make_source(minimum_confidence_threshold=0.0, confidence_level_tag=True)
    source._glossary_id_map["classifier.Email"] = "fn_email"
    source._glossary_id_map["classifier.Phone"] = "fn_phone"

    columns = [
        {
            "columnName": "contact",
            "fieldType": "varchar",
            "attributeDetails": [
                {
                    "name": "classifier.Email",
                    "ranks": ["HIGH"],
                    "type": "Classification",
                },
                {
                    "name": "classifier.Phone",
                    "ranks": ["HIGH"],
                    "type": "Classification",
                },
            ],
            "fieldClassifications": [],
            "columnProfile": {},
        }
    ]
    now_ms = int(time.time() * 1000)
    workunits = list(
        source._emit_schema_field_enrichment(
            "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)",
            _columns(columns),
            "my_conn",
            {},
            now_ms,
        )
    )
    editable_wus = [
        wu for wu in workunits if _aspect_name(wu) == "editableSchemaMetadata"
    ]
    assert len(editable_wus) == 1
    tag_urns = _patched_field_tag_urns(workunits, "contact")
    assert tag_urns == ["urn:li:tag:bigid.confidence:HIGH"]


# ---------------------------------------------------------------------------
# _make_dataset_urn fallback branch
# ---------------------------------------------------------------------------


def test_make_dataset_urn_fallback_when_fqn_does_not_start_with_source():
    source = _make_source()
    source._platform_map["my_conn"] = "mysql"

    urn = source._make_dataset_urn("other_conn.schema.table", "my_conn")

    assert urn == "urn:li:dataset:(urn:li:dataPlatform:mysql,schema.table,PROD)"


# ---------------------------------------------------------------------------
# _strip_classifier_prefix / _classifier_term_path
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("attr_name", "expected"),
    [
        ("classifier.PHONE", "classifier.phone"),
        # MD:: metadata prefix is stripped before slugifying
        ("classifier.MD::Postal Code", "classifier.postal_code"),
        ("classifier.", None),  # empty bare name → empty slug → None
    ],
)
def test_classifier_term_path(attr_name, expected):
    assert _make_source()._classifier_term_path(attr_name) == expected


# ---------------------------------------------------------------------------
# Unlinked classifier deduplication
# ---------------------------------------------------------------------------


def test_classifier_term_deduplication():
    """Two columns sharing the same unlinked classifier emit exactly one GlossaryTermInfo."""
    source = _make_source(minimum_confidence_threshold=0.0)
    # No glossary_id mapping → unlinked path
    columns = [
        {
            "columnName": "phone1",
            "fieldType": "varchar",
            "attributeDetails": [
                {"name": "classifier.PHONE", "ranks": ["HIGH"], "type": "classifier"}
            ],
            "fieldClassifications": [],
            "columnProfile": {},
        },
        {
            "columnName": "phone2",
            "fieldType": "varchar",
            "attributeDetails": [
                {"name": "classifier.PHONE", "ranks": ["HIGH"], "type": "classifier"}
            ],
            "fieldClassifications": [],
            "columnProfile": {},
        },
    ]
    now_ms = int(time.time() * 1000)
    workunits = list(
        source._emit_schema_field_enrichment(
            "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)",
            _columns(columns),
            "my_conn",
            {},
            now_ms,
        )
    )
    term_info_wus = [wu for wu in workunits if _aspect_name(wu) == "glossaryTermInfo"]
    assert len(term_info_wus) == 1, (
        "Expected exactly one GlossaryTermInfo for deduplicated classifier"
    )
    assert source.report.classifier_terms_emitted == 1


# ---------------------------------------------------------------------------
# Confidence threshold filters unlinked classifiers
# ---------------------------------------------------------------------------


def test_threshold_filters_medium_unlinked_classifier():
    """An unlinked classifier at MEDIUM confidence must be suppressed when threshold=0.75."""
    source = _make_source(minimum_confidence_threshold=0.75)
    columns = [
        {
            "columnName": "col",
            "fieldType": "varchar",
            "attributeDetails": [
                {"name": "classifier.PHONE", "ranks": ["MEDIUM"], "type": "classifier"}
            ],
            "fieldClassifications": [],
            "columnProfile": {},
        }
    ]
    now_ms = int(time.time() * 1000)
    workunits = list(
        source._emit_schema_field_enrichment(
            "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)",
            _columns(columns),
            "my_conn",
            {},
            now_ms,
        )
    )
    # No editableSchemaMetadata and no GlossaryTermInfo should be emitted
    assert workunits == []
    assert source.report.findings_below_threshold == 1


# ---------------------------------------------------------------------------
# Unlinked classifier friendly name in MetadataAttribution
# ---------------------------------------------------------------------------


def test_unlinked_classifier_friendly_name_in_attribution():
    """A friendly name stored in _classifier_friendly_names must appear in sourceDetail."""
    source = _make_source(minimum_confidence_threshold=0.0)
    source._classifier_friendly_names["classifier.PHONE"] = "Phone Number"
    columns = [
        {
            "columnName": "phone",
            "fieldType": "varchar",
            "attributeDetails": [
                {"name": "classifier.PHONE", "ranks": ["HIGH"], "type": "classifier"}
            ],
            "fieldClassifications": [],
            "columnProfile": {},
        }
    ]
    now_ms = int(time.time() * 1000)
    workunits = list(
        source._emit_schema_field_enrichment(
            "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)",
            _columns(columns),
            "my_conn",
            {},
            now_ms,
        )
    )
    editable_wus = [
        wu for wu in workunits if _aspect_name(wu) == "editableSchemaMetadata"
    ]
    assert len(editable_wus) == 1
    terms = _patched_field_terms(workunits, "phone")
    sd = _attribution(terms[0]).sourceDetail
    assert sd["classifier_friendly_name"] == "Phone Number"


# ---------------------------------------------------------------------------
# IDSoR — detection and three-path term resolution
# ---------------------------------------------------------------------------

IDSOR_ATTR = {
    "name": "customer_email",
    "count": 49000,
    "ranks": ["HIGH"],
    "type": ["IDSoR Attribute"],
}


def test_idsor_detection_true():
    assert _is_idsor_attr(_attr(IDSOR_ATTR)) is True


def test_idsor_detection_false_for_classifier():
    assert (
        _is_idsor_attr(
            _attr({"name": "classifier.EMAIL", "ranks": ["HIGH"], "type": "classifier"})
        )
        is False
    )


def test_idsor_path1_reuses_existing_term():
    """Path 1: glossaryId present → reuse term URN and emit GlossaryTermInfo with friendly name."""
    source = _make_source(minimum_confidence_threshold=0.0)
    source._idsor_attr_map["customer_email"] = IDSoRAttributeInfo(
        friendly_name="Email", glossary_id="bt_email"
    )
    columns = [
        {
            "columnName": "col",
            "fieldType": "varchar",
            "attributeDetails": [IDSOR_ATTR],
            "fieldClassifications": [],
            "columnProfile": {},
        }
    ]
    now_ms = int(time.time() * 1000)
    workunits = list(
        source._emit_schema_field_enrichment(
            "urn:li:dataset:(urn:li:dataPlatform:postgres,db.orders,PROD)",
            _columns(columns),
            "conn",
            {},
            now_ms,
        )
    )
    editable_wus = [
        wu for wu in workunits if _aspect_name(wu) == "editableSchemaMetadata"
    ]
    assert len(editable_wus) == 1
    terms = _patched_field_terms(workunits, "col")
    assert terms[0].urn == _term_urn("bt_email")
    # GlossaryTermInfo is now emitted for the linked term so the UI shows "Email", not "bt_email"
    term_info_wus = [wu for wu in workunits if _aspect_name(wu) == "glossaryTermInfo"]
    assert len(term_info_wus) == 1
    assert _entity_urn(term_info_wus[0]) == _term_urn("bt_email")
    assert _aspect(term_info_wus[0]).name == "Email"
    assert _aspect(term_info_wus[0]).parentNode == BIGID_IDSOR_GLOSSARY_NODE_URN
    assert _aspect(term_info_wus[0]).customProperties["bigid_glossary_id"] == "bt_email"
    assert source.report.idsor_terms_emitted == 1


def test_idsor_path2_autogenerates_under_idsor_node():
    """Path 2: in map but no glossaryId → auto-gen urn:li:glossaryTerm:bigid.idsor.<slug>."""
    source = _make_source(minimum_confidence_threshold=0.0)
    source._idsor_attr_map["full_name"] = IDSoRAttributeInfo(
        friendly_name="Full Name", glossary_id=None
    )
    columns = [
        {
            "columnName": "col",
            "fieldType": "varchar",
            "attributeDetails": [
                {
                    "name": "full_name",
                    "count": 30000,
                    "ranks": ["HIGH"],
                    "type": ["IDSoR Attribute"],
                }
            ],
            "fieldClassifications": [],
            "columnProfile": {},
        }
    ]
    now_ms = int(time.time() * 1000)
    workunits = list(
        source._emit_schema_field_enrichment(
            "urn:li:dataset:(urn:li:dataPlatform:postgres,db.orders,PROD)",
            _columns(columns),
            "conn",
            {},
            now_ms,
        )
    )
    term_info_wus = [wu for wu in workunits if _aspect_name(wu) == "glossaryTermInfo"]
    assert len(term_info_wus) == 1
    assert _entity_urn(term_info_wus[0]) == _term_urn("idsor.full_name")
    aspect = _aspect(term_info_wus[0])
    assert aspect.name == "Full Name"
    assert aspect.parentNode == BIGID_IDSOR_GLOSSARY_NODE_URN
    assert aspect.customProperties["bigid_type"] == "idsor_attribute"
    assert source.report.idsor_terms_emitted == 1


def test_idsor_path3_autogenerates_from_raw_name():
    """Path 3: not in map → auto-gen from raw attribute name."""
    source = _make_source(minimum_confidence_threshold=0.0)
    # _idsor_attr_map is empty — attribute not in map
    columns = [
        {
            "columnName": "col",
            "fieldType": "varchar",
            "attributeDetails": [
                {
                    "name": "country",
                    "count": 10255,
                    "ranks": ["HIGH"],
                    "type": ["IDSoR Attribute"],
                }
            ],
            "fieldClassifications": [],
            "columnProfile": {},
        }
    ]
    now_ms = int(time.time() * 1000)
    workunits = list(
        source._emit_schema_field_enrichment(
            "urn:li:dataset:(urn:li:dataPlatform:postgres,db.orders,PROD)",
            _columns(columns),
            "conn",
            {},
            now_ms,
        )
    )
    term_info_wus = [wu for wu in workunits if _aspect_name(wu) == "glossaryTermInfo"]
    assert len(term_info_wus) == 1
    assert _entity_urn(term_info_wus[0]) == _term_urn("idsor.country")
    assert _aspect(term_info_wus[0]).parentNode == BIGID_IDSOR_GLOSSARY_NODE_URN
    assert source.report.idsor_terms_emitted == 1


def test_idsor_source_detail_includes_row_count():
    """IDSoR count field must be propagated to sourceDetail[row_count]."""
    source = _make_source(minimum_confidence_threshold=0.0)
    columns = [
        {
            "columnName": "col",
            "fieldType": "varchar",
            "attributeDetails": [
                {
                    "name": "country",
                    "count": 10255,
                    "ranks": ["HIGH"],
                    "type": ["IDSoR Attribute"],
                }
            ],
            "fieldClassifications": [],
            "columnProfile": {},
        }
    ]
    now_ms = int(time.time() * 1000)
    workunits = list(
        source._emit_schema_field_enrichment(
            "urn:li:dataset:(urn:li:dataPlatform:postgres,db.orders,PROD)",
            _columns(columns),
            "conn",
            {},
            now_ms,
        )
    )
    editable_wus = [
        wu for wu in workunits if _aspect_name(wu) == "editableSchemaMetadata"
    ]
    assert len(editable_wus) == 1
    terms = _patched_field_terms(workunits, "col")
    sd = _attribution(terms[0]).sourceDetail
    assert sd["classifier_type"] == "idsor_attribute"
    assert sd["row_count"] == "10255"


def test_idsor_deduplication():
    """Two columns with the same IDSoR attribute emit exactly one GlossaryTermInfo."""
    source = _make_source(minimum_confidence_threshold=0.0)
    idsor_attr = {
        "name": "country",
        "count": 100,
        "ranks": ["HIGH"],
        "type": ["IDSoR Attribute"],
    }
    columns = [
        {
            "columnName": "col1",
            "fieldType": "varchar",
            "attributeDetails": [idsor_attr],
            "fieldClassifications": [],
            "columnProfile": {},
        },
        {
            "columnName": "col2",
            "fieldType": "varchar",
            "attributeDetails": [idsor_attr],
            "fieldClassifications": [],
            "columnProfile": {},
        },
    ]
    now_ms = int(time.time() * 1000)
    workunits = list(
        source._emit_schema_field_enrichment(
            "urn:li:dataset:(urn:li:dataPlatform:postgres,db.orders,PROD)",
            _columns(columns),
            "conn",
            {},
            now_ms,
        )
    )
    term_info_wus = [wu for wu in workunits if _aspect_name(wu) == "glossaryTermInfo"]
    assert len(term_info_wus) == 1
    assert source.report.idsor_terms_emitted == 1


def test_idsor_disabled_skips_all_idsor_findings():
    """sync_idsor=False must suppress all IDSoR attribute findings."""
    source = _make_source(minimum_confidence_threshold=0.0, sync_idsor=False)
    columns = [
        {
            "columnName": "col",
            "fieldType": "varchar",
            "attributeDetails": [IDSOR_ATTR],
            "fieldClassifications": [],
            "columnProfile": {},
        }
    ]
    now_ms = int(time.time() * 1000)
    workunits = list(
        source._emit_schema_field_enrichment(
            "urn:li:dataset:(urn:li:dataPlatform:postgres,db.orders,PROD)",
            _columns(columns),
            "conn",
            {},
            now_ms,
        )
    )
    assert workunits == []


def test_idsor_threshold_filter():
    """IDSoR findings below minimum_confidence_threshold must be skipped."""
    source = _make_source(minimum_confidence_threshold=0.75)  # HIGH only
    columns = [
        {
            "columnName": "col",
            "fieldType": "varchar",
            "attributeDetails": [
                {
                    "name": "country",
                    "count": 5000,
                    "ranks": ["MEDIUM"],
                    "type": ["IDSoR Attribute"],
                }
            ],
            "fieldClassifications": [],
            "columnProfile": {},
        }
    ]
    now_ms = int(time.time() * 1000)
    workunits = list(
        source._emit_schema_field_enrichment(
            "urn:li:dataset:(urn:li:dataPlatform:postgres,db.orders,PROD)",
            _columns(columns),
            "conn",
            {},
            now_ms,
        )
    )
    assert workunits == []
    assert source.report.findings_below_threshold == 1


# ---------------------------------------------------------------------------
# Error isolation: empty objectName guard
# ---------------------------------------------------------------------------


def test_process_catalog_object_empty_object_name_warns_and_skips_column_fetch():
    """An empty objectName must warn and skip get_columns rather than passing an empty string."""
    source = _make_source()
    source._platform_map["my_conn"] = "mysql"
    obj = {
        "fullyQualifiedName": "my_conn.schema.table",
        "source": "my_conn",
        "objectName": "",
        "scanner_type_group": "structured",
        "tags": [],
    }

    list(source._process_catalog_object(_catalog_object(obj), {}))

    _client(source).get_columns.assert_not_called()
    assert any("missing-object-name" in str(w) for w in source.report.warnings)


# ---------------------------------------------------------------------------
# Error isolation: catalog loop per-item exception isolation
# ---------------------------------------------------------------------------


def test_process_catalog_per_item_exception_does_not_abort_loop():
    """An exception from one catalog object must not stop processing of remaining objects."""
    source = _make_source()
    source._platform_map["my_conn"] = "mysql"

    bad_obj = {
        "fullyQualifiedName": "my_conn.schema.bad",
        "source": "my_conn",
        "objectName": "bad",
        "scanner_type_group": "structured",
        "tags": [],
    }
    good_obj = {
        "fullyQualifiedName": "my_conn.schema.good",
        "source": "my_conn",
        "objectName": "good",
        "scanner_type_group": "structured",
        "tags": [],
    }
    _client(source).get_catalog_objects.side_effect = lambda: iter(
        _catalog_objects([bad_obj, good_obj])
    )
    _client(source).get_columns.return_value = []

    call_count = 0
    original = source._process_catalog_object

    def raise_first(obj, name_map):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("simulated bad object")
        yield from original(obj, name_map)

    source._process_catalog_object = raise_first  # type: ignore[method-assign]

    list(source._process_catalog())

    assert any("catalog-object-failed" in str(w) for w in source.report.warnings)
    # good_obj was still processed (enriched counter incremented by the real _process_catalog_object)
    assert source.report.datasets_enriched >= 1


# ---------------------------------------------------------------------------
# Error isolation: catalog mid-iteration BigIDAPIError
# ---------------------------------------------------------------------------


def test_process_catalog_api_error_mid_iteration_warns():
    """A BigIDAPIError raised mid-iteration must warn and continue with buffered objects."""
    source = _make_source()
    source._platform_map["my_conn"] = "mysql"

    good_obj = {
        "fullyQualifiedName": "my_conn.schema.good",
        "source": "my_conn",
        "objectName": "good",
        "scanner_type_group": "structured",
        "tags": [],
    }

    def _fail_after_one():
        yield _catalog_object(good_obj)
        raise BigIDAPIError("network reset")

    _client(source).get_catalog_objects.side_effect = _fail_after_one
    _client(source).get_columns.return_value = []

    list(source._process_catalog())

    assert any("catalog-objects-partial" in str(w) for w in source.report.warnings)
    assert source.report.datasets_enriched >= 1
