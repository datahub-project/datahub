import datetime
from typing import Dict, List, Optional
from unittest import mock

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake import snowflake_config
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeSemanticView,
    SnowflakeTableIdentifier,
)
from datahub.ingestion.source.snowflake.snowflake_semantic_view_verified_queries import (
    SnowflakeVerifiedQueriesExtractor,
    SnowflakeVerifiedQuery,
    parse_verified_queries_from_ddl,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)
from datahub.metadata.schema_classes import (
    AiContextClass,
    QueryPropertiesClass,
    QuerySourceClass,
    QuerySubjectsClass,
    StatusClass,
)
from tests.unit.snowflake import conftest
from tests.unit.snowflake.conftest import DB, SCHEMA, make_gen

_DDL_HEAD = """create or replace semantic view SALES
	tables (
		ORDERS as MY_DB.MY_SCHEMA.ORDERS_T primary key (ID),
		MY_DB.MY_SCHEMA.CUSTOMERS
	)
	comment='Sales'
"""

_DDL = (
    _DDL_HEAD
    + """	ai_verified_queries (
		TOTAL_BY_TYPE AS (
QUESTION 'What is the total amount by order type?'
VERIFIED_BY 'Sample Analyst'
VERIFIED_AT 1714497970
ONBOARDING_QUESTION true
SQL 'SELECT o.kind, SUM(o.amount) FROM __ORDERS o WHERE o.status <> ''VOID'' GROUP BY o.kind'),
		CUSTOMER_COUNT AS (
QUESTION 'How many customers are there?'
SQL 'SELECT COUNT(*) FROM MY_DB.MY_SCHEMA.CUSTOMERS')
	);"""
)

_MODEL_URN = (
    "urn:li:semanticModel:(urn:li:dataPlatform:snowflake,my_db.my_schema,sales)"
)
_ORDERS_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.orders_t,PROD)"
)
_CUSTOMERS_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.customers,PROD)"
)


def test_parse_entries_with_escaped_quotes_and_optional_fields() -> None:
    result = parse_verified_queries_from_ddl(_DDL)

    assert result.skipped == []
    assert result.queries == [
        SnowflakeVerifiedQuery(
            name="TOTAL_BY_TYPE",
            question="What is the total amount by order type?",
            verified_by="Sample Analyst",
            verified_at=1714497970,
            onboarding_question=True,
            sql="SELECT o.kind, SUM(o.amount) FROM __ORDERS o WHERE o.status <> 'VOID' GROUP BY o.kind",
        ),
        SnowflakeVerifiedQuery(
            name="CUSTOMER_COUNT",
            question="How many customers are there?",
            sql="SELECT COUNT(*) FROM MY_DB.MY_SCHEMA.CUSTOMERS",
        ),
    ]


def test_parse_ddl_without_block() -> None:
    result = parse_verified_queries_from_ddl(_DDL_HEAD + ";")
    assert result.queries == []
    assert result.skipped == []


def test_parse_ignores_keyword_inside_strings() -> None:
    ddl = _DDL_HEAD.replace("'Sales'", "'see ai_verified_queries ( x )'") + ";"
    assert parse_verified_queries_from_ddl(ddl).queries == []


def test_parse_skips_entry_without_sql() -> None:
    ddl = (
        _DDL_HEAD
        + """ai_verified_queries (
  NO_SQL AS ( QUESTION 'Unanswered?' ),
  OK AS ( SQL 'SELECT 1' ONBOARDING_QUESTION false )
);"""
    )
    result = parse_verified_queries_from_ddl(ddl)
    assert [q.name for q in result.queries] == ["OK"]
    assert result.queries[0].onboarding_question is False
    assert result.queries[0].question is None
    assert len(result.skipped) == 1


def _semantic_view(view_definition: Optional[str]) -> SnowflakeSemanticView:
    return SnowflakeSemanticView(
        name="SALES",
        created=datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
        comment=None,
        view_definition=view_definition,
        last_altered=datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc),
        base_tables=[
            SnowflakeTableIdentifier("MY_DB", "MY_SCHEMA", "ORDERS_T"),
            SnowflakeTableIdentifier("MY_DB", "MY_SCHEMA", "CUSTOMERS"),
        ],
        logical_to_physical_table={
            "ORDERS": ("MY_DB", "MY_SCHEMA", "ORDERS_T"),
            "CUSTOMERS": ("MY_DB", "MY_SCHEMA", "CUSTOMERS"),
        },
    )


def _extractor(
    graph: Optional[mock.MagicMock] = None,
) -> SnowflakeVerifiedQueriesExtractor:
    config = SnowflakeV2Config.model_validate(
        {"account_id": "test_account", "username": "u", "password": "p"}
    )
    report = SnowflakeV2Report()
    return SnowflakeVerifiedQueriesExtractor(
        report=report,
        identifiers=SnowflakeIdentifierBuilder(
            identifier_config=config, structured_reporter=report
        ),
        graph=graph,
    )


def _run(
    extractor: SnowflakeVerifiedQueriesExtractor,
    view: SnowflakeSemanticView,
    semantic_model_urn: Optional[str] = _MODEL_URN,
) -> List[MetadataChangeProposalWrapper]:
    workunits: List[MetadataWorkUnit] = list(
        extractor.gen_workunits(
            semantic_view=view,
            schema_name="MY_SCHEMA",
            db_name="MY_DB",
            view_urn=semantic_model_urn or _ORDERS_URN,
            semantic_model_urn=semantic_model_urn,
        )
    )
    mcps = []
    for wu in workunits:
        assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
        mcps.append(wu.metadata)
    return mcps


def test_emits_manual_queries_with_stable_urns_and_resolved_subjects() -> None:
    extractor = _extractor()
    mcps = _run(extractor, _semantic_view(_DDL))

    properties = {
        m.entityUrn: m.aspect
        for m in mcps
        if isinstance(m.aspect, QueryPropertiesClass)
    }
    subjects = {
        m.entityUrn: [s.entity for s in m.aspect.subjects]
        for m in mcps
        if isinstance(m.aspect, QuerySubjectsClass)
    }
    total_urn = extractor.query_urn("my_db.my_schema.sales", "TOTAL_BY_TYPE")
    count_urn = extractor.query_urn("my_db.my_schema.sales", "CUSTOMER_COUNT")
    assert set(properties) == {total_urn, count_urn}
    # Deterministic: the same view + name always gives the same URN.
    assert total_urn == _extractor().query_urn("my_db.my_schema.sales", "TOTAL_BY_TYPE")

    total = properties[total_urn]
    assert total.source == QuerySourceClass.MANUAL
    assert total.name == "What is the total amount by order type?"
    assert total.created.time == 1714497970 * 1000
    assert total.customProperties == {
        "semantic_view": "my_db.my_schema.sales",
        "verified_query_name": "TOTAL_BY_TYPE",
        "verified_by": "Sample Analyst",
        "verified_at": "1714497970",
        "use_as_onboarding_question": "true",
    }
    # `__ORDERS` is the logical table; it resolves to its physical base table.
    assert subjects[total_urn] == [_MODEL_URN, _ORDERS_URN]
    assert subjects[count_urn] == [_MODEL_URN, _CUSTOMERS_URN]

    ai_contexts = [m.aspect for m in mcps if isinstance(m.aspect, AiContextClass)]
    assert ai_contexts == [
        AiContextClass(
            examples=[
                "What is the total amount by order type?",
                "How many customers are there?",
            ]
        )
    ]
    assert extractor.report.num_semantic_view_verified_queries_emitted == 2


def test_unparseable_sql_links_all_base_tables() -> None:
    ddl = _DDL_HEAD + "ai_verified_queries ( BAD AS ( SQL 'SELECT FROM (((' ) );"
    extractor = _extractor()
    mcps = _run(extractor, _semantic_view(ddl))
    subjects = [m.aspect for m in mcps if isinstance(m.aspect, QuerySubjectsClass)]
    assert [s.entity for s in subjects[0].subjects] == [
        _MODEL_URN,
        _ORDERS_URN,
        _CUSTOMERS_URN,
    ]
    assert extractor.report.num_semantic_view_verified_query_parse_failures == 1


def test_ai_context_merges_with_existing_aspect() -> None:
    graph = mock.MagicMock()
    graph.get_aspect.return_value = AiContextClass(
        synonyms=["revenue model"],
        instructions="Use net amounts.",
        examples=["How many customers are there?", "A user-entered example"],
    )
    mcps = _run(_extractor(graph), _semantic_view(_DDL))
    ai_contexts = [m.aspect for m in mcps if isinstance(m.aspect, AiContextClass)]
    assert ai_contexts == [
        AiContextClass(
            synonyms=["revenue model"],
            instructions="Use net amounts.",
            examples=[
                "How many customers are there?",
                "A user-entered example",
                "What is the total amount by order type?",
            ],
        )
    ]


def test_ai_context_not_written_when_existing_unreadable() -> None:
    graph = mock.MagicMock()
    graph.get_aspect.side_effect = RuntimeError("GMS down")
    extractor = _extractor(graph)
    mcps = _run(extractor, _semantic_view(_DDL))
    assert not any(isinstance(m.aspect, AiContextClass) for m in mcps)
    # The queries themselves are still emitted, and the failure is a warning.
    assert extractor.report.num_semantic_view_verified_queries_emitted == 2
    assert len(extractor.report.warnings) == 1


def test_legacy_mode_emits_queries_without_ai_context() -> None:
    mcps = _run(_extractor(), _semantic_view(_DDL), semantic_model_urn=None)
    assert not any(isinstance(m.aspect, AiContextClass) for m in mcps)
    assert sum(isinstance(m.aspect, QueryPropertiesClass) for m in mcps) == 2


@pytest.mark.parametrize("ddl", [None, "create semantic view x 'unterminated"])
def test_unreadable_definition_is_a_warning(ddl: Optional[str]) -> None:
    extractor = _extractor()
    assert _run(extractor, _semantic_view(ddl)) == []
    assert len(extractor.report.warnings) == 1
    assert extractor.report.num_semantic_view_verified_queries_skipped == 1


def test_parse_tolerates_odd_entries_and_value_types() -> None:
    ddl = (
        _DDL_HEAD
        + """ai_verified_queries (
  'not_an_entry',
  STRINGLY AS (
    QUESTION 'Typed as strings?'
    VERIFIED_AT 'yesterday'
    ONBOARDING_QUESTION 'TRUE'
    SOME_FLAG
    SQL 'SELECT 1'
  ),
  UNSURE AS ( SQL 'SELECT 2' ONBOARDING_QUESTION 'maybe' ),
);"""
    )
    result = parse_verified_queries_from_ddl(ddl)

    # The trailing comma's empty entry is ignored; the string entry is skipped.
    assert len(result.skipped) == 1
    stringly, unsure = result.queries
    # A keyword without a value doesn't swallow the next key.
    assert stringly.sql == "SELECT 1"
    assert stringly.verified_at is None
    assert stringly.onboarding_question is True
    assert unsure.onboarding_question is None


def test_skipped_entries_and_duplicate_names_are_reported() -> None:
    ddl = (
        _DDL_HEAD
        + """ai_verified_queries (
  NO_SQL AS ( QUESTION 'Unanswered?' ),
  DUP AS ( QUESTION 'First?' SQL 'SELECT 1' ),
  DUP AS ( QUESTION 'Second?' SQL 'SELECT 2' )
);"""
    )
    extractor = _extractor()
    mcps = _run(extractor, _semantic_view(ddl), semantic_model_urn=None)

    names = [m.aspect.name for m in mcps if isinstance(m.aspect, QueryPropertiesClass)]
    assert names == ["First?"]
    assert extractor.report.num_semantic_view_verified_queries_skipped == 1
    assert len(extractor.report.warnings) == 1


def test_subjects_skip_ctes_stages_and_resolve_partial_names() -> None:
    ddl = (
        _DDL_HEAD
        + """ai_verified_queries (
  MIXED AS ( SQL 'WITH recent AS (SELECT * FROM customers)
    SELECT * FROM recent JOIN OTHER_SCHEMA.EVENTS e ON TRUE
    JOIN @my_stage s ON TRUE;;
    SELECT * FROM "MY_DB"."MY_SCHEMA"."ORDERS_T"' )
);"""
    )
    mcps = _run(_extractor(), _semantic_view(ddl))
    (subjects,) = [m.aspect for m in mcps if isinstance(m.aspect, QuerySubjectsClass)]

    # `customers` is a logical table, `recent` a CTE, `@my_stage` a stage, and
    # the 2-part name resolves against the view's database.
    entities = [s.entity for s in subjects.subjects]
    assert entities[0] == _MODEL_URN
    assert sorted(entities[1:]) == sorted(
        [
            _CUSTOMERS_URN,
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.other_schema.events,PROD)",
            _ORDERS_URN,
        ]
    )


def test_timestamp_falls_back_to_view_last_altered() -> None:
    ddl = _DDL_HEAD + "ai_verified_queries ( Q AS ( SQL 'SELECT 1' ) );"
    mcps = _run(_extractor(), _semantic_view(ddl), semantic_model_urn=None)
    (props,) = [m.aspect for m in mcps if isinstance(m.aspect, QueryPropertiesClass)]
    assert props.name == "Q"
    assert props.created.time == int(
        datetime.datetime(2024, 2, 1, tzinfo=datetime.timezone.utc).timestamp() * 1000
    )


def test_ai_context_without_graph_is_reported_once() -> None:
    extractor = _extractor()
    _run(extractor, _semantic_view(_DDL))
    _run(extractor, _semantic_view(_DDL))
    assert len(extractor.report.infos) == 1


def test_ai_context_created_when_model_has_none() -> None:
    graph = mock.MagicMock()
    graph.get_aspect.return_value = None
    mcps = _run(_extractor(graph), _semantic_view(_DDL))
    ai_contexts = [m.aspect for m in mcps if isinstance(m.aspect, AiContextClass)]
    assert ai_contexts == [
        AiContextClass(
            examples=[
                "What is the total amount by order type?",
                "How many customers are there?",
            ]
        )
    ]


def test_ai_context_not_rewritten_when_questions_already_present() -> None:
    graph = mock.MagicMock()
    graph.get_aspect.return_value = AiContextClass(
        examples=[
            "How many customers are there?",
            "What is the total amount by order type?",
        ]
    )
    mcps = _run(_extractor(graph), _semantic_view(_DDL))
    assert not any(isinstance(m.aspect, AiContextClass) for m in mcps)


def test_no_ai_context_when_no_query_has_a_question() -> None:
    ddl = _DDL_HEAD + "ai_verified_queries ( Q AS ( SQL 'SELECT 1' ) );"
    graph = mock.MagicMock()
    mcps = _run(_extractor(graph), _semantic_view(ddl))
    assert not any(isinstance(m.aspect, AiContextClass) for m in mcps)
    graph.get_aspect.assert_not_called()


@pytest.mark.parametrize(
    "overrides",
    [
        {"semantic_views": {"include_verified_queries": True}},
        {
            "include_technical_schema": False,
            "semantic_views": {"enabled": True, "include_verified_queries": True},
        },
    ],
)
def test_config_warns_when_verified_queries_cannot_run(
    overrides: Dict[str, object],
) -> None:
    # The config module's logger is not attached to the logging hierarchy, so
    # caplog can't see it.
    with mock.patch.object(snowflake_config, "logger") as config_logger:
        SnowflakeV2Config.model_validate(
            {
                "account_id": "test_account",
                "username": "u",
                "password": "p",
                **overrides,
            }
        )
    assert any(
        "include_verified_queries" in call.args[0]
        for call in config_logger.warning.call_args_list
    )


_SCHEMA_GEN_DDL = (
    "create semantic view SV tables ( BASE as TEST_DB.PUBLIC.BASE ) "
    "ai_verified_queries ( Q AS ( QUESTION 'How many?' SQL 'SELECT COUNT(*) FROM __BASE' ) );"
)


def _schema_gen_semantic_view() -> SnowflakeSemanticView:
    return conftest.semantic_view(
        "SV",
        view_definition=_SCHEMA_GEN_DDL,
        base_tables=[SnowflakeTableIdentifier(DB, SCHEMA, "BASE")],
        logical_to_physical_table={"BASE": (DB, SCHEMA, "BASE")},
    )


def test_schema_gen_legacy_mode_attaches_queries_to_view_dataset() -> None:
    gen = make_gen(semantic_views={"enabled": True, "include_verified_queries": True})
    sv = _schema_gen_semantic_view()

    workunits = list(gen._process_semantic_view(sv, conftest.schema(), DB))

    (subjects,) = conftest.aspects(workunits, QuerySubjectsClass).values()
    assert [s.entity for s in subjects.subjects] == [
        conftest.dataset_urn(gen, "SV"),
        conftest.dataset_urn(gen, "BASE"),
    ]
    assert gen.report.num_semantic_view_verified_queries_emitted == 1


def test_schema_gen_extractor_failure_is_a_warning() -> None:
    gen = make_gen(semantic_views={"enabled": True, "include_verified_queries": True})
    sv = _schema_gen_semantic_view()

    with mock.patch.object(
        gen.verified_queries_extractor,
        "gen_workunits",
        side_effect=RuntimeError("boom"),
    ):
        workunits = list(gen._process_semantic_view(sv, conftest.schema(), DB))

    # The semantic view itself is still emitted.
    assert conftest.dataset_urn(gen, "SV") in conftest.aspects(workunits, StatusClass)
    assert "Failed to import semantic view verified queries" in conftest.log_keys(
        gen.report.warnings
    )


def test_schema_gen_semantic_model_mode_adds_questions_to_model() -> None:
    gen = make_gen(
        semantic_views={
            "enabled": True,
            "emit_semantic_model_entities": True,
            "include_verified_queries": True,
        }
    )
    sv = _schema_gen_semantic_view()

    workunits = list(gen._process_semantic_view(sv, conftest.schema(), DB))

    model_urn = gen.identifiers.gen_semantic_model_urn("SV", SCHEMA, DB)
    (subjects,) = conftest.aspects(workunits, QuerySubjectsClass).values()
    assert subjects.subjects[0].entity == model_urn
    assert conftest.aspects(workunits, AiContextClass)[model_urn].examples == [
        "How many?"
    ]


def test_schema_gen_emits_no_queries_when_disabled() -> None:
    gen = make_gen(semantic_views={"enabled": True})
    workunits = list(
        gen._process_semantic_view(_schema_gen_semantic_view(), conftest.schema(), DB)
    )
    assert conftest.aspects(workunits, QueryPropertiesClass) == {}
