from datahub.configuration.datetimes import parse_absolute_time
from datahub.metadata.urns import CorpUserUrn
from datahub.sql_parsing.sql_parsing_aggregator import PreparsedQuery
from datahub.sql_parsing.tool_meta_extractor import (
    ToolMetaExtractor,
    ToolMetaExtractorReport,
)


def test_extract_mode_metadata() -> None:
    extractor = ToolMetaExtractor(report=ToolMetaExtractorReport())
    query = """\
select * from LONG_TAIL_COMPANIONS.ADOPTION.PET_PROFILES
LIMIT 100
-- {"user":"@foo","email":"foo@acryl.io","url":"https://modeanalytics.com/acryltest/reports/6234ff78bc7d/runs/662b21949629/queries/f0aad24d5b37","scheduled":false}
"""

    entry = PreparsedQuery(
        query_id=None,
        query_text=query,
        upstreams=[],
        downstream=None,
        column_lineage=None,
        column_usage=None,
        inferred_schema=None,
        user=CorpUserUrn("mode"),
        timestamp=parse_absolute_time("2021-08-01T01:02:03Z"),
    )

    assert extractor.extract_bi_metadata(entry)
    assert entry.user == CorpUserUrn("foo")

    assert extractor.report.num_queries_meta_extracted["mode"] == 1


def test_extract_looker_metadata() -> None:
    extractor = ToolMetaExtractor(
        report=ToolMetaExtractorReport(), looker_user_mapping={"7": "john.doe@xyz.com"}
    )
    looker_query = """\
SELECT
    all_entities_extended_sibling."ENTITY"  AS "all_entities_extended_sibling.entity_type",
    COUNT(DISTINCT ( all_entities_extended_sibling."URN" )) AS "all_entities_extended_sibling.distinct_count"
FROM "PUBLIC"."ALL_ENTITIES"
     AS all_entities_extended_sibling
GROUP BY
    1
ORDER BY
    1
FETCH NEXT 50 ROWS ONLY
-- Looker Query Context '{"user_id":7,"history_slug":"264797031bc403cf382cbefbe3700849","instance_slug":"32654f2ffadf10b1949d4009e52fc6a4"}'
"""

    entry = PreparsedQuery(
        query_id=None,
        query_text=looker_query,
        upstreams=[],
        downstream=None,
        column_lineage=None,
        column_usage=None,
        inferred_schema=None,
        user=CorpUserUrn("mode"),
        timestamp=parse_absolute_time("2021-08-01T01:02:03Z"),
    )
    assert extractor.extract_bi_metadata(entry)
    assert entry.user == CorpUserUrn("john.doe")
    assert extractor.report.num_queries_meta_extracted["looker"] == 1


def test_extract_no_metadata() -> None:
    extractor = ToolMetaExtractor(report=ToolMetaExtractorReport())
    query = """\
select * from LONG_TAIL_COMPANIONS.ADOPTION.PET_PROFILES
LIMIT 100
-- random comment on query
"""

    entry = PreparsedQuery(
        query_id=None,
        query_text=query,
        upstreams=[],
        downstream=None,
        column_lineage=None,
        column_usage=None,
        inferred_schema=None,
        user=CorpUserUrn("mode"),
        timestamp=parse_absolute_time("2021-08-01T01:02:03Z"),
    )

    assert not extractor.extract_bi_metadata(entry)

    assert extractor.report.num_queries_meta_extracted["mode"] == 0
    assert extractor.report.num_queries_meta_extracted["looker"] == 0
