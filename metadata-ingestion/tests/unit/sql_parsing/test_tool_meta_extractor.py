from datahub.configuration.datetimes import parse_absolute_time
from datahub.metadata.urns import CorpUserUrn
from datahub.sql_parsing.sql_parsing_aggregator import PreparsedQuery
from datahub.sql_parsing.tool_meta_extractor import ToolMetaExtractor


def test_extract_mode_metadata() -> None:
    extractor = ToolMetaExtractor()
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


def test_extract_no_metadata() -> None:
    extractor = ToolMetaExtractor()
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
