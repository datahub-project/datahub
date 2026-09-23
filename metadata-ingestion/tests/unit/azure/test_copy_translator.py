from typing import Any, Dict, List, Optional

import pytest

from datahub.ingestion.source.azure.copy_translator import (
    CopyColumnMapping,
    get_translator_type,
    make_copy_fine_grained_lineage,
    parse_translator_mappings,
)
from datahub.metadata.schema_classes import (
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
)

SOURCE_URN = "urn:li:dataset:(urn:li:dataPlatform:mssql,dbo.customers,PROD)"
SINK_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales.customers,PROD)"


@pytest.mark.parametrize(
    "translator, expected",
    [
        ({"type": "TabularTranslator"}, "TabularTranslator"),
        ({"translatorType": "TabularTranslator"}, "TabularTranslator"),
        (
            {"value": "@json(pipeline().parameters.m)", "type": "Expression"},
            "Expression",
        ),
        ({}, None),
    ],
)
def test_get_translator_type(
    translator: Dict[str, Any], expected: Optional[str]
) -> None:
    assert get_translator_type(translator) == expected


@pytest.mark.parametrize(
    "translator, expected",
    [
        pytest.param(
            {
                "type": "TabularTranslator",
                "mappings": [
                    {"source": {"name": "id"}, "sink": {"name": "customer_id"}},
                    {
                        "source": {"name": "name", "type": "String"},
                        "sink": {"name": "name"},
                    },
                ],
            },
            [
                CopyColumnMapping("id", "customer_id"),
                CopyColumnMapping("name", "name"),
            ],
            id="mappings_by_name",
        ),
        pytest.param(
            {
                "type": "TabularTranslator",
                "mappings": [
                    {
                        "source": {"path": "$['customer']['id']"},
                        "sink": {"name": "customer_id"},
                    },
                    {
                        "source": {"path": "['email']"},
                        "sink": {"path": "$.contact.email"},
                    },
                ],
                "collectionReference": "$['orders']",
            },
            [
                CopyColumnMapping("customer.id", "customer_id"),
                CopyColumnMapping("email", "contact.email"),
            ],
            id="mappings_by_path",
        ),
        pytest.param(
            {
                "type": "TabularTranslator",
                "mappings": [
                    {"source": {"ordinal": 1}, "sink": {"name": "id"}},
                    {"source": {"name": ""}, "sink": {"name": "x"}},
                    "not-a-dict",
                    {"source": {"name": "ok"}, "sink": {"name": "ok"}},
                ],
            },
            [CopyColumnMapping("ok", "ok")],
            id="mappings_skip_unusable_entries",
        ),
        pytest.param(
            {"type": "TabularTranslator", "columnMappings": {"id": "customer_id"}},
            [CopyColumnMapping("id", "customer_id")],
            id="legacy_dict",
        ),
        pytest.param(
            {
                "type": "TabularTranslator",
                "columnMappings": "id: customer_id, name : full_name,broken",
            },
            [
                CopyColumnMapping("id", "customer_id"),
                CopyColumnMapping("name", "full_name"),
            ],
            id="legacy_string",
        ),
        pytest.param(
            {
                "columnMappings": {"a": "b"},
                "mappings": [{"source": {"name": "x"}, "sink": {"name": "y"}}],
            },
            [CopyColumnMapping("a", "b")],
            id="legacy_takes_precedence",
        ),
        pytest.param({"type": "TabularTranslator"}, [], id="no_explicit_mappings"),
        pytest.param(
            {"value": "@json(pipeline().parameters.m)", "type": "Expression"},
            [],
            id="expression",
        ),
    ],
)
def test_parse_translator_mappings(
    translator: Dict[str, Any], expected: List[CopyColumnMapping]
) -> None:
    assert parse_translator_mappings(translator) == expected


def test_make_copy_fine_grained_lineage() -> None:
    fgl = make_copy_fine_grained_lineage(SOURCE_URN, "id", SINK_URN, "customer_id")
    assert fgl.upstreamType == FineGrainedLineageUpstreamTypeClass.FIELD_SET
    assert fgl.downstreamType == FineGrainedLineageDownstreamTypeClass.FIELD
    assert fgl.upstreams == [f"urn:li:schemaField:({SOURCE_URN},id)"]
    assert fgl.downstreams == [f"urn:li:schemaField:({SINK_URN},customer_id)"]
    assert fgl.transformOperation == "COPY"
