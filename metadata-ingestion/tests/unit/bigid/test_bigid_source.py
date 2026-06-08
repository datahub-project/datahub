"""
Unit tests for BigIDSource — focused on business logic of the source class:
  - Platform registry loading (_load_registries)
  - FQN → URN construction (_make_dataset_urn)
  - Classification resolution (_resolve_attr_to_glossary_id)
  - GlossaryTerm emission (_emit_glossary_terms)
  - Tag routing (_emit_tag_entity)
  - Confidence threshold filtering (_emit_schema_field_enrichment)
  - Domain resolution (_resolve_domain_urn)
  - DatasetProfile field derivation (_emit_dataset_profile)
  - MetadataAttribution sourceDetail
  - Error handling (_process_catalog_object)
"""

from __future__ import annotations

import time
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import time_machine

from datahub.ingestion.source.bigid.bigid_api import BigIDAPIError
from datahub.ingestion.source.bigid.bigid_config import BigIDSourceConfig
from datahub.ingestion.source.bigid.bigid_source import BigIDSource, _is_idsor_attr
from datahub.ingestion.source.bigid.bigid_utils import IDSoRAttributeInfo
from datahub.metadata.schema_classes import ChangeTypeClass

FROZEN_TIME = "2026-01-15T00:00:00Z"


@pytest.fixture(autouse=True)
def freeze_time():
    with time_machine.travel(FROZEN_TIME, tick=False):
        yield


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(**overrides) -> dict[str, Any]:
    base = {
        "bigid_url": "https://bigid.example.com",
        "access_token": "test-token",
        "env": "PROD",
    }
    base.update(overrides)
    return base


def _make_source(**config_overrides) -> BigIDSource:
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
    source.client.get_connections.return_value = [
        {"name": "my_snowflake", "type": "snowflake"},
        {"name": "my_mysql", "type": "rdb-mysql"},
    ]
    source.client.get_all_classifications.return_value = []
    source.client.get_glossary_items.return_value = []

    source._load_registries()

    assert source._platform_map["my_snowflake"] == "snowflake"
    assert source._platform_map["my_mysql"] == "mysql"


def test_load_registries_explicit_override_wins():
    source = _make_source(
        datasource_platform_mapping={
            "my_conn": {"platform": "bigquery", "env": "DEV"},
        }
    )
    source.client.get_connections.return_value = [
        {"name": "my_conn", "type": "rdb-mysql"},  # would auto-detect as mysql
    ]
    source.client.get_all_classifications.return_value = []
    source.client.get_glossary_items.return_value = []

    source._load_registries()

    assert source._platform_map["my_conn"] == "bigquery"


def test_load_registries_unknown_type_warns():
    source = _make_source()
    source.client.get_connections.return_value = [
        {"name": "exotic_conn", "type": "some-unknown-type"},
    ]
    source.client.get_all_classifications.return_value = []
    source.client.get_glossary_items.return_value = []

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

    urn = source._make_dataset_urn("SNOWFLAKE_PROD.MY_DB.MY_SCHEMA.MY_TABLE", "SNOWFLAKE_PROD")
    assert urn == "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.my_table,PROD)"


def test_make_dataset_urn_with_platform_instance():
    source = _make_source(
        datasource_platform_mapping={
            "sf_conn": {"platform": "snowflake", "platform_instance": "prod_acct"},
        }
    )
    source._platform_map["sf_conn"] = "snowflake"

    urn = source._make_dataset_urn("sf_conn.mydb.myschema.mytable", "sf_conn")
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


def test_make_dataset_urn_encodes_comma_and_colon():
    """Commas and colons in the name component must be percent-encoded."""
    source = _make_source()
    source._platform_map["my_db"] = "mysql"

    urn = source._make_dataset_urn("my_db.schema,v2:prod.table", "my_db")
    assert "%2C" in urn  # comma encoded
    assert "%3A" in urn  # colon encoded
    # Raw delimiter characters must not appear in the name segment
    name_segment = urn[urn.index("mysql,") + len("mysql,"):]
    name_segment = name_segment.rsplit(",", 1)[0]  # strip trailing ,PROD
    assert "," not in name_segment
    assert ":" not in name_segment


# ---------------------------------------------------------------------------
# Classification resolution
# ---------------------------------------------------------------------------


def test_resolve_classifier_prefix_via_all_classifications():
    source = _make_source()
    source._glossary_id_map["classifier.Email"] = "fn_ootb_email"

    result = source._resolve_attr_to_glossary_id("classifier.Email", {})
    assert result == "fn_ootb_email"


def test_resolve_classifier_md_prefix():
    source = _make_source()
    source._glossary_id_map["classifier.MD::Postal Code - MD"] = "fn_postal_code"

    result = source._resolve_attr_to_glossary_id("classifier.MD::Postal Code - MD", {})
    assert result == "fn_postal_code"


def test_resolve_business_term_prefix_via_name_lookup():
    source = _make_source()
    name_map = {"First Name": "bt_item_HK2A"}

    result = source._resolve_attr_to_glossary_id("businessTerm.First Name", name_map)
    assert result == "bt_item_HK2A"


def test_resolve_unknown_prefix_returns_none():
    source = _make_source()
    result = source._resolve_attr_to_glossary_id("unknownPrefix.Something", {})
    assert result is None


def test_resolve_classifier_not_in_map_returns_none():
    source = _make_source()
    result = source._resolve_attr_to_glossary_id("classifier.NonExistent", {})
    assert result is None


# ---------------------------------------------------------------------------
# Confidence threshold filtering
# ---------------------------------------------------------------------------


def test_confidence_threshold_filters_low():
    """With threshold 0.75 (HIGH), MEDIUM and LOW findings are skipped."""
    source = _make_source(minimum_confidence_threshold=0.75)
    source._glossary_id_map["classifier.Email"] = "fn_email"

    columns = [
        {
            "columnName": "email",
            "fieldType": "varchar",
            "attributeDetails": [
                {"name": "classifier.Email", "ranks": ["MEDIUM"], "type": "Classification"},
                {"name": "classifier.Email", "ranks": ["HIGH"], "type": "Classification"},
            ],
            "fieldClassifications": [],
            "columnProfile": {},
        }
    ]
    now_ms = int(time.time() * 1000)
    workunits = list(
        source._emit_schema_field_enrichment(
            "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)",
            columns,
            "my_conn",
            {},
            now_ms,
        )
    )
    # Only HIGH passes threshold — one association
    assert source.report.findings_below_threshold == 1
    assert len(workunits) == 1


def test_confidence_threshold_zero_passes_all():
    source = _make_source(minimum_confidence_threshold=0.0)
    source._glossary_id_map["classifier.Email"] = "fn_email"

    columns = [
        {
            "columnName": "email",
            "fieldType": "varchar",
            "attributeDetails": [
                {"name": "classifier.Email", "ranks": ["LOW"], "type": "Classification"},
            ],
            "fieldClassifications": [],
            "columnProfile": {},
        }
    ]
    now_ms = int(time.time() * 1000)
    workunits = list(
        source._emit_schema_field_enrichment(
            "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)",
            columns,
            "my_conn",
            {},
            now_ms,
        )
    )
    assert source.report.findings_below_threshold == 0
    assert len(workunits) == 1


def test_confidence_threshold_medium_exactly_at_boundary_passes():
    """At threshold=0.50, MEDIUM findings (score=0.50) must pass — boundary is inclusive."""
    source = _make_source(minimum_confidence_threshold=0.50)
    source._glossary_id_map["classifier.Email"] = "fn_email"

    columns = [
        {
            "columnName": "email",
            "fieldType": "varchar",
            "attributeDetails": [
                {"name": "classifier.Email", "ranks": ["MEDIUM"], "type": "Classification"},
            ],
            "fieldClassifications": [],
            "columnProfile": {},
        }
    ]
    now_ms = int(time.time() * 1000)
    workunits = list(
        source._emit_schema_field_enrichment(
            "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)",
            columns,
            "my_conn",
            {},
            now_ms,
        )
    )
    assert source.report.findings_below_threshold == 0
    assert len(workunits) == 1


def test_confidence_threshold_just_above_medium_filters_it():
    """At threshold=0.51, MEDIUM findings (score=0.50) must be filtered out."""
    source = _make_source(minimum_confidence_threshold=0.51)
    source._glossary_id_map["classifier.Email"] = "fn_email"

    columns = [
        {
            "columnName": "email",
            "fieldType": "varchar",
            "attributeDetails": [
                {"name": "classifier.Email", "ranks": ["MEDIUM"], "type": "Classification"},
            ],
            "fieldClassifications": [],
            "columnProfile": {},
        }
    ]
    now_ms = int(time.time() * 1000)
    workunits = list(
        source._emit_schema_field_enrichment(
            "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)",
            columns,
            "my_conn",
            {},
            now_ms,
        )
    )
    assert source.report.findings_below_threshold == 1
    assert len(workunits) == 0


# ---------------------------------------------------------------------------
# GlossaryTerm emission
# ---------------------------------------------------------------------------


def test_glossary_term_urn_uses_glossary_id():
    source = _make_source()
    item = {"glossary_id": "bt_item_HK2A", "name": "First Name"}
    assert source._glossary_term_urn(item) == "urn:li:glossaryTerm:bigid.bt_item_HK2A"


def test_glossary_term_urn_falls_back_to_name_slug():
    source = _make_source()
    item = {"glossary_id": "", "name": "My Custom Term"}
    assert source._glossary_term_urn(item) == "urn:li:glossaryTerm:bigid.my_custom_term"


def test_should_include_item_respects_type_filter():
    source = _make_source(item_types=["Business Term"])
    assert source._should_include_item({"type": "Business Term", "glossary_id": "x"})
    assert not source._should_include_item({"type": "Personal Data Category", "glossary_id": "y"})


def test_should_include_ootb_personal_data_item_regardless_of_filter():
    """OOTB Personal Data Items are always included — column enrichment references their URNs."""
    source = _make_source(item_types=["Business Term"])  # PDI not in allow-list
    item = {"type": "Personal Data Item", "is_ootb": True, "glossary_id": "fn_ootb_email"}
    assert source._should_include_item(item)


def test_emit_glossary_terms_custom_properties():
    source = _make_source()
    source._glossary_items = [
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

    workunits = list(source._emit_glossary_terms())
    assert len(workunits) >= 2

    term_info_wu = next(
        wu for wu in workunits
        if hasattr(wu.metadata, "aspect") and "GlossaryTermInfo" in type(wu.metadata.aspect).__name__
    )
    aspect = term_info_wu.metadata.aspect
    assert aspect.name == "Customer ID"
    assert aspect.definition == "Unique customer identifier"
    assert aspect.customProperties["bigid_type"] == "Business Term"
    assert aspect.customProperties["bigid_is_ootb"] == "false"
    assert aspect.customProperties["bigid_glossary_id"] == "bt_HK1"
    assert aspect.customProperties["bigid_id"] == "abc123"


def test_emit_glossary_terms_emits_ownership_when_owner_set():
    source = _make_source(owner_type="user")
    source._glossary_items = [
        {
            "_id": "abc",
            "glossary_id": "bt_1",
            "type": "Business Term",
            "name": "Term A",
            "owner": "alice",
        }
    ]

    workunits = list(source._emit_glossary_terms())
    wu_types = [type(wu.metadata.aspect).__name__ for wu in workunits]
    assert "OwnershipClass" in wu_types


def test_emit_glossary_terms_no_ownership_when_owner_type_none():
    source = _make_source(owner_type="none")
    source._glossary_items = [
        {
            "_id": "abc",
            "glossary_id": "bt_1",
            "type": "Business Term",
            "name": "Term A",
            "owner": "alice",
        }
    ]

    workunits = list(source._emit_glossary_terms())
    wu_types = [type(wu.metadata.aspect).__name__ for wu in workunits]
    assert "OwnershipClass" not in wu_types


# ---------------------------------------------------------------------------
# Tag routing
# ---------------------------------------------------------------------------


def test_tag_routing_sensitivity_classification_emits_tag():
    source = _make_source()
    workunits = list(source._emit_tag_entity("system.sensitivityClassification.Sensitivity", "Confidential"))
    assert len(workunits) == 2  # TagProperties + Status
    aspect = workunits[0].metadata.aspect
    assert workunits[0].metadata.entityUrn == "urn:li:tag:bigid.system.sensitivityClassification.Sensitivity:Confidential"
    assert aspect.name == "Sensitivity : Confidential"
    assert aspect.description == "system.sensitivityClassification.Sensitivity"


def test_tag_urn_preserves_system_prefix():
    source = _make_source()
    workunits = list(source._emit_tag_entity("system.risk.riskGroup", "High"))
    aspect = workunits[0].metadata.aspect
    assert "bigid.system.risk.riskGroup:High" in workunits[0].metadata.entityUrn
    assert aspect.name == "riskGroup : High"
    assert aspect.description == "system.risk.riskGroup"


def test_tag_display_name_strips_system_prefix():
    assert BigIDSource._tag_display_name("system.sensitivityClassification.Sensitivity", "Restricted") == "Sensitivity : Restricted"
    assert BigIDSource._tag_display_name("system.risk.riskGroup", "high") == "riskGroup : high"


def test_tag_display_name_preserves_user_defined_name():
    assert BigIDSource._tag_display_name("Sen.Priority", "P3") == "Sen.Priority : P3"


def test_tag_display_name_no_dot_in_name():
    assert BigIDSource._tag_display_name("Sensitivity", "Confidential") == "Sensitivity : Confidential"


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
    source.client.get_catalog_objects.side_effect = lambda: iter([obj])
    source.client.get_columns.return_value = []

    workunits = list(source._process_catalog())
    risk_wus = [
        wu for wu in workunits
        if wu.metadata.aspectName == "structuredProperties"
    ]

    assert len(risk_wus) == 1, "Expected exactly one structuredProperties workunit for riskScore"
    assert risk_wus[0].metadata.changeType == ChangeTypeClass.PATCH, (
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
    source.client.get_catalog_objects.side_effect = lambda: iter([obj])
    source.client.get_columns.return_value = []

    workunits = list(source._process_catalog())
    risk_wus = [wu for wu in workunits if wu.metadata.aspectName == "structuredProperties"]

    assert risk_wus == [], "Non-numeric riskScore must produce no structuredProperties workunit"


# ---------------------------------------------------------------------------
# Domain resolution
# ---------------------------------------------------------------------------


def test_resolve_domain_urn_auto_namespaced_sub_domain_takes_priority():
    source = _make_source(domain_mode="auto_namespaced")
    urn = source._resolve_domain_urn("Customer", "Identity")
    assert urn == "urn:li:domain:bigid.identity"


def test_resolve_domain_urn_auto_namespaced_domain_only():
    source = _make_source(domain_mode="auto_namespaced")
    urn = source._resolve_domain_urn("Customer", "")
    assert urn == "urn:li:domain:bigid.customer"


def test_resolve_domain_urn_none_mode_returns_none():
    source = _make_source(domain_mode="none")
    assert source._resolve_domain_urn("Customer", "Identity") is None


def test_resolve_domain_urn_config_map():
    source = _make_source(
        domain_mode="config_map",
        domain_mapping={"Identity": "urn:li:domain:existing-identity"},
    )
    urn = source._resolve_domain_urn("Customer", "Identity")
    assert urn == "urn:li:domain:existing-identity"


def test_resolve_domain_urn_config_map_missing_key_returns_none():
    """An unmapped domain key in config_map mode must return None — no domain linked."""
    source = _make_source(
        domain_mode="config_map",
        domain_mapping={"Finance": "urn:li:domain:finance"},
    )
    assert source._resolve_domain_urn("Customer", "") is None
    assert source._resolve_domain_urn("Customer", "Identity") is None


def test_emit_glossary_terms_config_map_missing_key_no_domain_aspect():
    """A glossary term with an unmapped domain must not emit a DomainsClass aspect."""
    source = _make_source(
        domain_mode="config_map",
        domain_mapping={"Finance": "urn:li:domain:finance"},
    )
    source._glossary_items = [
        {
            "_id": "x",
            "glossary_id": "bt_1",
            "type": "Business Term",
            "name": "Customer ID",
            "domain": "Customer",  # not in mapping
        }
    ]

    workunits = list(source._emit_glossary_terms())
    aspect_names = {type(wu.metadata.aspect).__name__ for wu in workunits}
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
    workunits = list(source._emit_dataset_profile(
        "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)",
        columns,
        int(time.time() * 1000),
    ))
    assert len(workunits) == 1
    profile = workunits[0].metadata.aspect
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
    workunits = list(source._emit_dataset_profile(
        "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)",
        columns,
        int(time.time() * 1000),
    ))
    fp = workunits[0].metadata.aspect.fieldProfiles[0]
    assert fp.min == "Aaron"
    assert fp.max == "Zara"
    assert fp.sampleValues == ["Aaron", "Zara"]
    assert fp.mean is None
    assert fp.stdev is None


def test_emit_dataset_profile_empty_column_profile_skipped():
    source = _make_source()
    columns = [{"columnName": "col_no_profile", "columnProfile": {}}]
    workunits = list(source._emit_dataset_profile(
        "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)",
        columns,
        int(time.time() * 1000),
    ))
    # Profile is emitted but no fieldProfiles since columnProfile was empty
    assert len(workunits) == 1
    profile = workunits[0].metadata.aspect
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
                {"name": "classifier.Email", "ranks": ["HIGH"], "type": "Classification"},
                {"name": "classifier.Phone", "ranks": ["MEDIUM"], "type": "Classification"},
            ],
            "fieldClassifications": [],
            "columnProfile": {},
        }
    ]
    now_ms = int(time.time() * 1000)
    workunits = list(
        source._emit_schema_field_enrichment(
            "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)",
            columns,
            "my_conn",
            {},
            now_ms,
        )
    )
    assert len(workunits) == 1
    editable = workunits[0].metadata.aspect
    field_info = editable.editableSchemaFieldInfo[0]
    terms = field_info.glossaryTerms.terms
    assert len(terms) == 2

    high_assoc = next(t for t in terms if t.attribution.sourceDetail["confidence_level"] == "HIGH")
    med_assoc = next(t for t in terms if t.attribution.sourceDetail["confidence_level"] == "MEDIUM")

    assert high_assoc.attribution.sourceDetail["classifier_friendly_name"] == "Email Address"
    assert "classifier_friendly_name" not in med_assoc.attribution.sourceDetail


def test_source_detail_attribution_fields_populated():
    source = _make_source()
    source._glossary_id_map["classifier.Email"] = "fn_email"

    columns = [
        {
            "columnName": "email_col",
            "fieldType": "varchar",
            "attributeDetails": [
                {"name": "classifier.Email", "ranks": ["HIGH"], "type": "Classification"},
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
            columns,
            "sales_conn",
            {},
            now_ms,
        )
    )
    assert len(workunits) == 1
    terms = workunits[0].metadata.aspect.editableSchemaFieldInfo[0].glossaryTerms.terms
    sd = terms[0].attribution.sourceDetail

    assert sd["classifier_name"] == "classifier.Email"
    assert sd["classifier_type"] == "value"
    assert sd["confidence_level"] == "HIGH"
    assert sd["bigid_connection"] == "sales_conn"
    assert sd["row_count"] == "1500"
    assert sd["distinct_count"] == "900"
    assert terms[0].attribution.source == "urn:li:dataPlatform:bigid"
    assert terms[0].attribution.actor == "urn:li:corpuser:datahub"


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


def test_process_catalog_object_column_fetch_failure_continues():
    """A BigIDAPIError from get_columns must be warned and processing must continue."""
    source = _make_source()
    source._platform_map["my_conn"] = "postgres"
    source.client.get_columns.side_effect = BigIDAPIError("connection timed out")

    obj = {
        "fullyQualifiedName": "my_conn.public.orders",
        "source": "my_conn",
        "objectName": "orders",
        "scanner_type_group": "structured",
        "tags": [],
    }

    workunits = list(source._process_catalog_object(obj, {}))

    # Enrichment counter incremented — object was not skipped
    assert source.report.datasets_enriched == 1

    # Column-dependent aspects must not be emitted
    aspect_names = {type(wu.metadata.aspect).__name__ for wu in workunits}
    assert "EditableSchemaMetadataClass" not in aspect_names
    assert "DatasetProfileClass" not in aspect_names

    # dataPlatformInstance is not emitted without platform_instance config
    assert "DataPlatformInstanceClass" not in aspect_names


# ---------------------------------------------------------------------------
# _load_registries error paths
# ---------------------------------------------------------------------------


def test_load_registries_connections_api_error_warns():
    source = _make_source()
    source.client.get_connections.side_effect = BigIDAPIError("timeout")
    source.client.get_all_classifications.return_value = []
    source.client.get_glossary_items.return_value = []

    source._load_registries()

    assert source._platform_map == {}
    assert any("ds-connections" in str(w) for w in source.report.warnings)


def test_load_registries_classifications_api_error_warns():
    source = _make_source()
    source.client.get_connections.return_value = []
    source.client.get_all_classifications.side_effect = BigIDAPIError("timeout")
    source.client.get_glossary_items.return_value = []

    source._load_registries()

    assert source._glossary_id_map == {}
    assert any("all-classifications" in str(w) for w in source.report.warnings)


def test_load_registries_glossary_items_api_error_warns():
    source = _make_source()
    source.client.get_connections.return_value = []
    source.client.get_all_classifications.return_value = []
    source.client.get_glossary_items.side_effect = BigIDAPIError("timeout")

    source._load_registries()

    assert source._glossary_items == []
    assert any("glossary-items" in str(w) for w in source.report.warnings)


def test_load_registries_idsor_attributes_api_error_warns():
    source = _make_source()
    source.client.get_connections.return_value = []
    source.client.get_all_classifications.return_value = []
    source.client.get_glossary_items.return_value = []
    source.client.get_idsor_attribute_map.side_effect = BigIDAPIError("timeout")

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
    source.client.get_catalog_objects.side_effect = lambda: iter([obj])
    source.client.get_columns.return_value = []

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
                "properties": {"hidden": False, "applicationType": "unknownType"},
            }
        ],
    }
    source.client.get_catalog_objects.side_effect = lambda: iter([obj])
    source.client.get_columns.return_value = []

    with patch.object(source, "_emit_tag_entity") as mock_emit:
        list(source._process_catalog())

    mock_emit.assert_not_called()


# ---------------------------------------------------------------------------
# _make_dataset_urn fallback branch
# ---------------------------------------------------------------------------


def test_make_dataset_urn_fallback_when_fqn_does_not_start_with_source():
    source = _make_source()
    source._platform_map["my_conn"] = "mysql"

    urn = source._make_dataset_urn("other_conn.schema.table", "my_conn")

    assert urn == "urn:li:dataset:(urn:li:dataPlatform:mysql,schema.table,PROD)"


# ---------------------------------------------------------------------------
# _strip_classifier_prefix / _classifier_term_urn
# ---------------------------------------------------------------------------


def test_classifier_term_urn_plain():
    source = _make_source()
    assert source._classifier_term_urn("classifier.PHONE") == "urn:li:glossaryTerm:bigid.classifier.phone"


def test_classifier_term_urn_strips_md_prefix():
    source = _make_source()
    assert (
        source._classifier_term_urn("classifier.MD::Postal Code")
        == "urn:li:glossaryTerm:bigid.classifier.postal_code"
    )


def test_classifier_term_urn_returns_none_for_empty_suffix():
    source = _make_source()
    # "classifier." with empty bare name → empty slug → None
    assert source._classifier_term_urn("classifier.") is None


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
            "attributeDetails": [{"name": "classifier.PHONE", "ranks": ["HIGH"], "type": "classifier"}],
            "fieldClassifications": [],
            "columnProfile": {},
        },
        {
            "columnName": "phone2",
            "fieldType": "varchar",
            "attributeDetails": [{"name": "classifier.PHONE", "ranks": ["HIGH"], "type": "classifier"}],
            "fieldClassifications": [],
            "columnProfile": {},
        },
    ]
    now_ms = int(time.time() * 1000)
    workunits = list(
        source._emit_schema_field_enrichment(
            "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)",
            columns,
            "my_conn",
            {},
            now_ms,
        )
    )
    term_info_wus = [
        wu for wu in workunits
        if wu.metadata.aspectName == "glossaryTermInfo"
    ]
    assert len(term_info_wus) == 1, "Expected exactly one GlossaryTermInfo for deduplicated classifier"
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
            "attributeDetails": [{"name": "classifier.PHONE", "ranks": ["MEDIUM"], "type": "classifier"}],
            "fieldClassifications": [],
            "columnProfile": {},
        }
    ]
    now_ms = int(time.time() * 1000)
    workunits = list(
        source._emit_schema_field_enrichment(
            "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)",
            columns,
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
            "attributeDetails": [{"name": "classifier.PHONE", "ranks": ["HIGH"], "type": "classifier"}],
            "fieldClassifications": [],
            "columnProfile": {},
        }
    ]
    now_ms = int(time.time() * 1000)
    workunits = list(
        source._emit_schema_field_enrichment(
            "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)",
            columns,
            "my_conn",
            {},
            now_ms,
        )
    )
    editable_wus = [wu for wu in workunits if wu.metadata.aspectName == "editableSchemaMetadata"]
    assert len(editable_wus) == 1
    terms = editable_wus[0].metadata.aspect.editableSchemaFieldInfo[0].glossaryTerms.terms
    sd = terms[0].attribution.sourceDetail
    assert sd["classifier_friendly_name"] == "Phone Number"


# ---------------------------------------------------------------------------
# IDSoR — detection and three-path term resolution
# ---------------------------------------------------------------------------

IDSOR_ATTR = {"name": "customer_email", "count": 49000, "ranks": ["HIGH"], "type": ["IDSoR Attribute"]}


def test_idsor_detection_true():
    assert _is_idsor_attr(IDSOR_ATTR) is True


def test_idsor_detection_false_for_classifier():
    assert _is_idsor_attr({"name": "classifier.EMAIL", "ranks": ["HIGH"], "type": "classifier"}) is False


def test_idsor_path1_reuses_existing_term():
    """Path 1: glossaryId present → reuse term URN and emit GlossaryTermInfo with friendly name."""
    source = _make_source(minimum_confidence_threshold=0.0)
    source._idsor_attr_map["customer_email"] = IDSoRAttributeInfo(friendly_name="Email", glossary_id="bt_email")
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
            columns, "conn", {}, now_ms,
        )
    )
    editable_wus = [wu for wu in workunits if wu.metadata.aspectName == "editableSchemaMetadata"]
    assert len(editable_wus) == 1
    term_urn = editable_wus[0].metadata.aspect.editableSchemaFieldInfo[0].glossaryTerms.terms[0].urn
    assert term_urn == "urn:li:glossaryTerm:bigid.bt_email"
    # GlossaryTermInfo is now emitted for the linked term so the UI shows "Email", not "bt_email"
    term_info_wus = [wu for wu in workunits if wu.metadata.aspectName == "glossaryTermInfo"]
    assert len(term_info_wus) == 1
    assert term_info_wus[0].metadata.entityUrn == "urn:li:glossaryTerm:bigid.bt_email"
    assert term_info_wus[0].metadata.aspect.name == "Email"
    assert term_info_wus[0].metadata.aspect.parentNode == "urn:li:glossaryNode:bigid.idsor"
    assert term_info_wus[0].metadata.aspect.customProperties["bigid_glossary_id"] == "bt_email"
    assert source.report.idsor_terms_emitted == 1


def test_idsor_path2_autogenerates_under_idsor_node():
    """Path 2: in map but no glossaryId → auto-gen urn:li:glossaryTerm:bigid.idsor.<slug>."""
    source = _make_source(minimum_confidence_threshold=0.0)
    source._idsor_attr_map["full_name"] = IDSoRAttributeInfo(friendly_name="Full Name", glossary_id=None)
    columns = [
        {
            "columnName": "col",
            "fieldType": "varchar",
            "attributeDetails": [
                {"name": "full_name", "count": 30000, "ranks": ["HIGH"], "type": ["IDSoR Attribute"]}
            ],
            "fieldClassifications": [],
            "columnProfile": {},
        }
    ]
    now_ms = int(time.time() * 1000)
    workunits = list(
        source._emit_schema_field_enrichment(
            "urn:li:dataset:(urn:li:dataPlatform:postgres,db.orders,PROD)",
            columns, "conn", {}, now_ms,
        )
    )
    term_info_wus = [wu for wu in workunits if wu.metadata.aspectName == "glossaryTermInfo"]
    assert len(term_info_wus) == 1
    assert term_info_wus[0].metadata.entityUrn == "urn:li:glossaryTerm:bigid.idsor.full_name"
    aspect = term_info_wus[0].metadata.aspect
    assert aspect.name == "Full Name"
    assert aspect.parentNode == "urn:li:glossaryNode:bigid.idsor"
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
                {"name": "country", "count": 10255, "ranks": ["HIGH"], "type": ["IDSoR Attribute"]}
            ],
            "fieldClassifications": [],
            "columnProfile": {},
        }
    ]
    now_ms = int(time.time() * 1000)
    workunits = list(
        source._emit_schema_field_enrichment(
            "urn:li:dataset:(urn:li:dataPlatform:postgres,db.orders,PROD)",
            columns, "conn", {}, now_ms,
        )
    )
    term_info_wus = [wu for wu in workunits if wu.metadata.aspectName == "glossaryTermInfo"]
    assert len(term_info_wus) == 1
    assert term_info_wus[0].metadata.entityUrn == "urn:li:glossaryTerm:bigid.idsor.country"
    assert term_info_wus[0].metadata.aspect.parentNode == "urn:li:glossaryNode:bigid.idsor"
    assert source.report.idsor_terms_emitted == 1


def test_idsor_source_detail_includes_row_count():
    """IDSoR count field must be propagated to sourceDetail[row_count]."""
    source = _make_source(minimum_confidence_threshold=0.0)
    columns = [
        {
            "columnName": "col",
            "fieldType": "varchar",
            "attributeDetails": [
                {"name": "country", "count": 10255, "ranks": ["HIGH"], "type": ["IDSoR Attribute"]}
            ],
            "fieldClassifications": [],
            "columnProfile": {},
        }
    ]
    now_ms = int(time.time() * 1000)
    workunits = list(
        source._emit_schema_field_enrichment(
            "urn:li:dataset:(urn:li:dataPlatform:postgres,db.orders,PROD)",
            columns, "conn", {}, now_ms,
        )
    )
    editable_wus = [wu for wu in workunits if wu.metadata.aspectName == "editableSchemaMetadata"]
    sd = editable_wus[0].metadata.aspect.editableSchemaFieldInfo[0].glossaryTerms.terms[0].attribution.sourceDetail
    assert sd["classifier_type"] == "idsor_attribute"
    assert sd["row_count"] == "10255"


def test_idsor_deduplication():
    """Two columns with the same IDSoR attribute emit exactly one GlossaryTermInfo."""
    source = _make_source(minimum_confidence_threshold=0.0)
    idsor_attr = {"name": "country", "count": 100, "ranks": ["HIGH"], "type": ["IDSoR Attribute"]}
    columns = [
        {"columnName": "col1", "fieldType": "varchar", "attributeDetails": [idsor_attr],
         "fieldClassifications": [], "columnProfile": {}},
        {"columnName": "col2", "fieldType": "varchar", "attributeDetails": [idsor_attr],
         "fieldClassifications": [], "columnProfile": {}},
    ]
    now_ms = int(time.time() * 1000)
    workunits = list(
        source._emit_schema_field_enrichment(
            "urn:li:dataset:(urn:li:dataPlatform:postgres,db.orders,PROD)",
            columns, "conn", {}, now_ms,
        )
    )
    term_info_wus = [wu for wu in workunits if wu.metadata.aspectName == "glossaryTermInfo"]
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
            columns, "conn", {}, now_ms,
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
                {"name": "country", "count": 5000, "ranks": ["MEDIUM"], "type": ["IDSoR Attribute"]}
            ],
            "fieldClassifications": [],
            "columnProfile": {},
        }
    ]
    now_ms = int(time.time() * 1000)
    workunits = list(
        source._emit_schema_field_enrichment(
            "urn:li:dataset:(urn:li:dataPlatform:postgres,db.orders,PROD)",
            columns, "conn", {}, now_ms,
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

    list(source._process_catalog_object(obj, {}))

    source.client.get_columns.assert_not_called()
    assert any("missing-object-name" in str(w) for w in source.report.warnings)


# ---------------------------------------------------------------------------
# Error isolation: catalog loop per-item exception isolation
# ---------------------------------------------------------------------------


def test_process_catalog_per_item_exception_does_not_abort_loop():
    """An exception from one catalog object must not stop processing of remaining objects."""
    source = _make_source()
    source._platform_map["my_conn"] = "mysql"

    bad_obj = {"fullyQualifiedName": "my_conn.schema.bad", "source": "my_conn",
               "objectName": "bad", "scanner_type_group": "structured", "tags": []}
    good_obj = {"fullyQualifiedName": "my_conn.schema.good", "source": "my_conn",
                "objectName": "good", "scanner_type_group": "structured", "tags": []}
    source.client.get_catalog_objects.side_effect = lambda: iter([bad_obj, good_obj])
    source.client.get_columns.return_value = []

    call_count = 0
    original = source._process_catalog_object

    def raise_first(obj, name_map):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("simulated bad object")
        yield from original(obj, name_map)

    source._process_catalog_object = raise_first

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
        yield good_obj
        raise BigIDAPIError("network reset")

    source.client.get_catalog_objects.side_effect = _fail_after_one
    source.client.get_columns.return_value = []

    list(source._process_catalog())

    assert any("catalog-objects-partial" in str(w) for w in source.report.warnings)
    assert source.report.datasets_enriched >= 1
