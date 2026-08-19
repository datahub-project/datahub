import io
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest
from tableauserverclient import Server

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.tableau.tableau import (
    SiteIdContentUrl,
    TableauConfig,
    TableauSiteSource,
    TableauSourceReport,
)
from datahub.ingestion.source.tableau.tableau_common import (
    _normalize_parsed_upstream_urn,
    make_upstream_class,
)
from datahub.ingestion.source.tableau.tableau_initial_sql import (
    extract_definition_bytes,
    extract_initial_sql_by_datasource,
    extract_initial_sql_connections,
    extract_tds_bytes,
)
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.sql_parsing.sqlglot_lineage import SqlParsingResult

# Synthetic TDS: a federated wrapper + one Snowflake named-connection that
# declares Initial SQL via the `one-time-sql` attribute. No real customer values.
TDS_WITH_INITIAL_SQL = b"""<?xml version='1.0' encoding='utf-8' ?>
<datasource formatted-name='federated.test' inline='true' version='18.1'>
  <connection class='federated'>
    <named-connections>
      <named-connection caption='example' name='snowflake.test'>
        <connection authentication='Username Password' class='snowflake' dbname='ANALYTICS_DB' one-time-sql='CREATE TEMPORARY TABLE TMP AS SELECT * FROM ANALYTICS_DB.PUBLIC.SOURCE_TABLE' schema='PUBLIC' server='example.snowflakecomputing.com' username='svc_user' warehouse='WH' />
      </named-connection>
    </named-connections>
  </connection>
</datasource>"""

TDS_EMPTY_INITIAL_SQL = TDS_WITH_INITIAL_SQL.replace(
    b"one-time-sql='CREATE TEMPORARY TABLE TMP AS SELECT * FROM ANALYTICS_DB.PUBLIC.SOURCE_TABLE'",
    b"one-time-sql=''",
)

# Multi-statement Initial SQL on a single connection: two CREATE ... AS SELECT
# statements referencing two distinct sources. Both must become upstreams.
TDS_MULTI_STATEMENT_INITIAL_SQL = TDS_WITH_INITIAL_SQL.replace(
    b"one-time-sql='CREATE TEMPORARY TABLE TMP AS SELECT * FROM ANALYTICS_DB.PUBLIC.SOURCE_TABLE'",
    b"one-time-sql='CREATE TEMPORARY TABLE T1 AS SELECT * FROM ANALYTICS_DB.PUBLIC.SOURCE_ONE;"
    b" CREATE TEMPORARY TABLE T2 AS SELECT * FROM ANALYTICS_DB.PUBLIC.SOURCE_TWO'",
)

# Initial SQL that references a Tableau parameter (templating). The angle brackets
# are XML-escaped in the .tds attribute and unescape to <[Parameters].[Region]>.
# It is not valid SQL until the parameter is resolved.
TDS_PARAM_INITIAL_SQL = TDS_WITH_INITIAL_SQL.replace(
    b"one-time-sql='CREATE TEMPORARY TABLE TMP AS SELECT * FROM ANALYTICS_DB.PUBLIC.SOURCE_TABLE'",
    b"one-time-sql='SELECT * FROM ANALYTICS_DB.PUBLIC.SOURCE_TABLE"
    b" WHERE REGION = &lt;[Parameters].[Region]&gt;'",
)


def _make_tdsx(tds_bytes: bytes) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("example.tds", tds_bytes)
    return buf.getvalue()


def _make_site_source(ingest_initial_sql: bool = True) -> TableauSiteSource:
    config = TableauConfig.model_validate(
        {
            "connect_uri": "https://test-tableau-server.com",
            "site": "site1",
            "username": "u",
            "password": "p",
            "ingest_initial_sql": ingest_initial_sql,
            "initial_sql_as_lineage": True,
            "initial_sql_as_custom_property": True,
        }
    )
    return TableauSiteSource(
        config=config,
        ctx=PipelineContext(run_id="0", pipeline_name="test_tableau"),
        platform="tableau",
        site=SiteIdContentUrl(site_id="id1", site_content_url="site1"),
        report=TableauSourceReport(),
        server=Server("https://test-tableau-server.com"),
    )


def test_extract_initial_sql_connections_finds_sql():
    conns = extract_initial_sql_connections(TDS_WITH_INITIAL_SQL)
    assert len(conns) == 1
    conn = conns[0]
    assert conn.connection_type == "snowflake"
    assert conn.database == "ANALYTICS_DB"
    assert conn.schema == "PUBLIC"
    assert "CREATE TEMPORARY TABLE" in conn.initial_sql


def test_extract_initial_sql_connections_skips_empty():
    assert extract_initial_sql_connections(TDS_EMPTY_INITIAL_SQL) == []


def test_extract_initial_sql_connections_malformed_xml_is_safe():
    assert extract_initial_sql_connections(b"not valid xml <<<") == []


def test_extract_tds_bytes_from_tdsx():
    tdsx = _make_tdsx(TDS_WITH_INITIAL_SQL)
    assert extract_tds_bytes(tdsx) == TDS_WITH_INITIAL_SQL


def test_extract_tds_bytes_from_bare_tds():
    assert extract_tds_bytes(TDS_WITH_INITIAL_SQL) == TDS_WITH_INITIAL_SQL


def test_extract_tds_bytes_corrupt_zip_returns_none():
    # Starts with the zip magic bytes but is not a valid archive.
    assert extract_tds_bytes(b"PK\x03\x04 garbage not a real zip") is None


def test_extract_tds_bytes_tdsx_without_tds_returns_none():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("data.hyper", b"binary-extract-data")
    assert extract_tds_bytes(buf.getvalue()) is None


def test_get_initial_sql_lineage_emits_upstream():
    site_source = _make_site_source()
    tdsx = _make_tdsx(TDS_WITH_INITIAL_SQL)

    def fake_download(luid, filepath=None, include_extract=True):
        path = Path(filepath) / "ds.tdsx"
        path.write_bytes(tdsx)
        return str(path)

    with mock.patch.object(
        site_source.server.datasources, "download", side_effect=fake_download
    ):
        result = site_source.get_initial_sql_lineage(
            datasource={"luid": "abc-123", "id": "dsid"},
            datasource_urn="urn:li:dataset:(urn:li:dataPlatform:tableau,dsid,PROD)",
        )
        upstreams, text = result.upstreams, result.raw_sql

    assert text is not None and "CREATE TEMPORARY TABLE" in text
    assert [u.dataset for u in upstreams] == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics_db.public.source_table,PROD)"
    ]
    assert site_source.report.num_initial_sql_connections_found == 1
    assert site_source.report.num_initial_sql_lineage_upstreams == 1


def test_get_initial_sql_lineage_no_luid_returns_empty():
    site_source = _make_site_source()
    result = site_source.get_initial_sql_lineage(
        datasource={"id": "dsid"},
        datasource_urn="urn:li:dataset:(urn:li:dataPlatform:tableau,dsid,PROD)",
    )
    assert result.upstreams == []
    assert result.raw_sql is None


def test_get_initial_sql_lineage_download_failure_counts():
    site_source = _make_site_source()
    with mock.patch.object(
        site_source.server.datasources,
        "download",
        side_effect=RuntimeError("boom"),
    ):
        result = site_source.get_initial_sql_lineage(
            datasource={"luid": "abc-123", "id": "dsid"},
            datasource_urn="urn:li:dataset:(urn:li:dataPlatform:tableau,dsid,PROD)",
        )
    assert result.upstreams == []
    assert result.raw_sql is None
    assert site_source.report.num_initial_sql_download_failures == 1


def test_get_initial_sql_lineage_as_lineage_disabled_collects_text_only():
    site_source = _make_site_source()
    site_source.config.initial_sql_as_lineage = False
    tdsx = _make_tdsx(TDS_WITH_INITIAL_SQL)

    def fake_download(luid, filepath=None, include_extract=True):
        path = Path(filepath) / "ds.tdsx"
        path.write_bytes(tdsx)
        return str(path)

    with mock.patch.object(
        site_source.server.datasources, "download", side_effect=fake_download
    ):
        result = site_source.get_initial_sql_lineage(
            datasource={"luid": "abc-123", "id": "dsid"},
            datasource_urn="urn:li:dataset:(urn:li:dataPlatform:tableau,dsid,PROD)",
        )
    assert result.upstreams == []
    assert result.raw_sql is not None and "CREATE TEMPORARY TABLE" in result.raw_sql
    assert site_source.report.num_initial_sql_connections_found == 1


def test_emit_datasource_includes_initial_sql_upstream_and_property():
    site_source = _make_site_source()
    tdsx = _make_tdsx(TDS_WITH_INITIAL_SQL)

    def fake_download(luid, filepath=None, include_extract=True):
        path = Path(filepath) / "ds.tdsx"
        path.write_bytes(tdsx)
        return str(path)

    datasource = {
        "id": "dsid",
        "luid": "abc-123",
        "name": "ds",
        "isUnsupportedCustomSql": False,
    }

    with (
        mock.patch.object(
            site_source.server.datasources, "download", side_effect=fake_download
        ),
        mock.patch.object(
            site_source, "_get_datasource_project_luid", return_value="proj-luid"
        ),
        mock.patch.object(
            site_source, "_get_project_browse_path_name", return_value=None
        ),
        mock.patch.object(
            site_source, "_get_datasource_container_key", return_value=None
        ),
    ):
        workunits = list(site_source.emit_datasource(datasource, is_embedded_ds=False))

    upstream_aspects = [
        wu.metadata.aspect
        for wu in workunits
        if hasattr(wu.metadata, "aspect")
        and isinstance(wu.metadata.aspect, UpstreamLineageClass)
    ]
    assert len(upstream_aspects) == 1
    assert [u.dataset for u in upstream_aspects[0].upstreams] == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics_db.public.source_table,PROD)"
    ]

    props_aspects = []
    for wu in workunits:
        if hasattr(wu.metadata, "proposedSnapshot"):
            for aspect in wu.metadata.proposedSnapshot.aspects:
                if isinstance(aspect, DatasetPropertiesClass):
                    props_aspects.append(aspect)
    assert len(props_aspects) == 1
    assert "CREATE TEMPORARY TABLE" in props_aspects[0].customProperties["initialSql"]


def test_get_initial_sql_lineage_multi_statement():
    site_source = _make_site_source()
    tdsx = _make_tdsx(TDS_MULTI_STATEMENT_INITIAL_SQL)

    def fake_download(luid, filepath=None, include_extract=True):
        path = Path(filepath) / "ds.tdsx"
        path.write_bytes(tdsx)
        return str(path)

    with mock.patch.object(
        site_source.server.datasources, "download", side_effect=fake_download
    ):
        result = site_source.get_initial_sql_lineage(
            datasource={"luid": "abc-123", "id": "dsid"},
            datasource_urn="urn:li:dataset:(urn:li:dataPlatform:tableau,dsid,PROD)",
        )

    # Both statements' source tables are extracted as upstreams.
    assert [u.dataset for u in result.upstreams] == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics_db.public.source_one,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics_db.public.source_two,PROD)",
    ]
    assert (
        result.raw_sql is not None
        and "SOURCE_ONE" in result.raw_sql
        and "SOURCE_TWO" in result.raw_sql
    )
    assert site_source.report.num_initial_sql_parse_failures == 0


def test_emit_datasource_upstream_lineage_merges_and_dedups_initial_sql():
    # The single UpstreamLineage aspect must merge GraphQL upstreams with Initial
    # SQL upstreams, de-duplicated by dataset urn.
    site_source = _make_site_source()
    site_source.config.ingest_virtual_connections = False

    shared_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.sch.shared,PROD)"
    initial_only_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.sch.initial_only,PROD)"
    )
    graphql_upstream = UpstreamClass(
        dataset=shared_urn, type=DatasetLineageTypeClass.TRANSFORMED
    )
    initial_sql_upstreams = [
        UpstreamClass(dataset=shared_urn, type=DatasetLineageTypeClass.TRANSFORMED),
        UpstreamClass(
            dataset=initial_only_urn, type=DatasetLineageTypeClass.TRANSFORMED
        ),
    ]

    with mock.patch.object(
        site_source,
        "_create_upstream_table_lineage",
        return_value=SimpleNamespace(
            upstream_tables=[graphql_upstream], fine_grained_lineages=[]
        ),
    ):
        workunits = list(
            site_source._emit_datasource_upstream_lineage(
                {"id": "dsid", "upstreamTables": [{"x": 1}]},
                "urn:li:dataset:(urn:li:dataPlatform:tableau,dsid,PROD)",
                browse_path=None,
                is_embedded_ds=False,
                initial_sql_upstreams=initial_sql_upstreams,
            )
        )

    aspects = [
        wu.metadata.aspect
        for wu in workunits
        if hasattr(wu.metadata, "aspect")
        and isinstance(wu.metadata.aspect, UpstreamLineageClass)
    ]
    assert len(aspects) == 1
    # shared_urn appears once (de-duped), plus the Initial-SQL-only upstream.
    assert [u.dataset for u in aspects[0].upstreams] == [shared_urn, initial_only_urn]


def test_get_initial_sql_lineage_parse_failure_skips_only_bad_statement():
    site_source = _make_site_source()
    bad_then_good = (
        b"one-time-sql='SELECT FROM WHERE GROUP;"
        b" CREATE TEMPORARY TABLE T AS SELECT * FROM ANALYTICS_DB.PUBLIC.SOURCE_GOOD'"
    )
    tds = TDS_WITH_INITIAL_SQL.replace(
        b"one-time-sql='CREATE TEMPORARY TABLE TMP AS SELECT * FROM ANALYTICS_DB.PUBLIC.SOURCE_TABLE'",
        bad_then_good,
    )
    tdsx = _make_tdsx(tds)

    def fake_download(luid, filepath=None, include_extract=True):
        path = Path(filepath) / "ds.tdsx"
        path.write_bytes(tdsx)
        return str(path)

    with mock.patch.object(
        site_source.server.datasources, "download", side_effect=fake_download
    ):
        result = site_source.get_initial_sql_lineage(
            datasource={"luid": "abc-123", "id": "dsid"},
            datasource_urn="urn:li:dataset:(urn:li:dataPlatform:tableau,dsid,PROD)",
        )

    # The unparseable statement is skipped and counted; the valid one still emits.
    assert [u.dataset for u in result.upstreams] == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics_db.public.source_good,PROD)"
    ]
    assert site_source.report.num_initial_sql_parse_failures == 1


def test_emit_datasource_embedded_emits_initial_sql_via_workbook():
    site_source = _make_site_source()
    site_source.config.ingest_virtual_connections = False
    twbx = _make_twbx(TWB_EMBEDDED)

    def fake_wb_download(luid, filepath=None, include_extract=True):
        path = Path(filepath) / "wb.twbx"
        path.write_bytes(twbx)
        return str(path)

    datasource = {
        "id": "dsid",
        "name": "Sales",
        "workbook": {"luid": "wb-1"},
        "isUnsupportedCustomSql": False,
    }

    with (
        mock.patch.object(
            site_source.server.workbooks, "download", side_effect=fake_wb_download
        ),
        mock.patch.object(site_source.server.datasources, "download") as ds_download,
        mock.patch.object(
            site_source, "_get_datasource_project_luid", return_value="proj-luid"
        ),
        mock.patch.object(
            site_source, "_get_project_browse_path_name", return_value=None
        ),
        mock.patch.object(
            site_source, "_get_datasource_container_key", return_value=None
        ),
    ):
        workunits = list(
            site_source.emit_datasource(
                datasource, workbook={"name": "wb"}, is_embedded_ds=True
            )
        )

    # Embedded path uses the workbook download, never the datasource download.
    ds_download.assert_not_called()

    props = [
        aspect
        for wu in workunits
        if hasattr(wu.metadata, "proposedSnapshot")
        for aspect in wu.metadata.proposedSnapshot.aspects
        if isinstance(aspect, DatasetPropertiesClass)
    ]
    assert len(props) == 1
    assert "SALES_SRC" in props[0].customProperties["initialSql"]

    upstream_aspects = [
        wu.metadata.aspect
        for wu in workunits
        if hasattr(wu.metadata, "aspect")
        and isinstance(wu.metadata.aspect, UpstreamLineageClass)
    ]
    assert len(upstream_aspects) == 1
    assert [u.dataset for u in upstream_aspects[0].upstreams] == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics_db.public.sales_src,PROD)"
    ]


def test_config_rejects_initial_sql_with_no_outputs():
    with pytest.raises(ValueError):
        TableauConfig.model_validate(
            {
                "connect_uri": "https://test-tableau-server.com",
                "site": "site1",
                "username": "u",
                "password": "p",
                "ingest_initial_sql": True,
                "initial_sql_as_lineage": False,
                "initial_sql_as_custom_property": False,
            }
        )


def _make_twbx(twb_bytes: bytes) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("wb.twb", twb_bytes)
    return buf.getvalue()


def test_extract_definition_bytes_from_twbx():
    twb = b"<workbook><datasources/></workbook>"
    assert extract_definition_bytes(_make_twbx(twb)) == twb


def test_extract_definition_bytes_from_bare_tds():
    assert extract_definition_bytes(TDS_WITH_INITIAL_SQL) == TDS_WITH_INITIAL_SQL


def test_extract_definition_bytes_prefers_twb_over_tds_in_zip():
    twb = b"<workbook/>"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("a.tds", b"<datasource/>")
        z.writestr("b.twb", twb)
    assert extract_definition_bytes(buf.getvalue()) == twb


# A workbook .twb mirroring real structure: a top-level <datasources> with two
# captioned Snowflake datasources (one with Initial SQL, one without), a captionless
# Parameters block, plus a *reference* <datasource> copy inside <worksheets> that must
# be ignored (only the top-level block counts).
TWB_EMBEDDED = b"""<?xml version='1.0' encoding='utf-8' ?>
<workbook version='18.1'>
  <datasources>
    <datasource caption='Sales' name='federated.aaa' inline='true'>
      <connection class='federated'>
        <named-connections>
          <named-connection caption='sf' name='snowflake.aaa'>
            <connection class='snowflake' dbname='ANALYTICS_DB' schema='PUBLIC' server='x.snowflakecomputing.com' one-time-sql='CREATE TEMPORARY TABLE T AS SELECT * FROM ANALYTICS_DB.PUBLIC.SALES_SRC' />
          </named-connection>
        </named-connections>
      </connection>
    </datasource>
    <datasource caption='Inventory' name='federated.bbb' inline='true'>
      <connection class='federated'>
        <named-connections>
          <named-connection caption='sf2' name='snowflake.bbb'>
            <connection class='snowflake' dbname='ANALYTICS_DB' schema='PUBLIC' server='x.snowflakecomputing.com' />
          </named-connection>
        </named-connections>
      </connection>
    </datasource>
    <datasource name='Parameters' hasconnection='false' inline='true'>
      <connection class='sqlproxy' />
    </datasource>
  </datasources>
  <worksheets>
    <worksheet name='sheet1'>
      <table>
        <view>
          <datasources>
            <datasource caption='Sales' name='federated.aaa' />
          </datasources>
        </view>
      </table>
    </worksheet>
  </worksheets>
</workbook>"""


def test_by_datasource_attributes_per_caption():
    m = extract_initial_sql_by_datasource(TWB_EMBEDDED)
    assert set(m) == {"Sales", "Inventory"}
    assert len(m["Sales"]) == 1
    assert m["Sales"][0].connection_type == "snowflake"
    assert "SALES_SRC" in m["Sales"][0].initial_sql
    assert m["Inventory"] == []


def test_by_datasource_ignores_reference_nodes():
    # Put one-time-sql on the worksheet reference copy of 'Sales'; the parser must
    # ignore reference nodes (only the top-level <datasources> block counts), so it
    # must not appear in the result.
    twb = TWB_EMBEDDED.replace(
        b"<datasource caption='Sales' name='federated.aaa' />",
        b"<datasource caption='Sales' name='federated.aaa' one-time-sql='SELECT 1 FROM REF_ONLY' />",
    )
    m = extract_initial_sql_by_datasource(twb)
    assert len(m["Sales"]) == 1
    assert "REF_ONLY" not in m["Sales"][0].initial_sql


def test_by_datasource_duplicate_top_level_caption_is_omitted():
    twb = TWB_EMBEDDED.replace(b"caption='Inventory'", b"caption='Sales'")
    m = extract_initial_sql_by_datasource(twb)
    assert "Sales" not in m


def test_by_datasource_malformed_xml_is_safe():
    assert extract_initial_sql_by_datasource(b"not valid xml <<<") == {}


def test_by_datasource_no_datasources_block():
    assert extract_initial_sql_by_datasource(b"<workbook/>") == {}


def test_embedded_initial_sql_matches_by_caption():
    site_source = _make_site_source()
    twbx = _make_twbx(TWB_EMBEDDED)

    def fake_wb_download(luid, filepath=None, include_extract=True):
        path = Path(filepath) / "wb.twbx"
        path.write_bytes(twbx)
        return str(path)

    with mock.patch.object(
        site_source.server.workbooks, "download", side_effect=fake_wb_download
    ):
        result = site_source.get_initial_sql_lineage_embedded(
            datasource={"name": "Sales", "workbook": {"luid": "wb-1"}},
            datasource_urn="urn:li:dataset:(urn:li:dataPlatform:tableau,dsid,PROD)",
        )

    assert result.raw_sql is not None and "SALES_SRC" in result.raw_sql
    assert [u.dataset for u in result.upstreams] == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics_db.public.sales_src,PROD)"
    ]
    assert site_source.report.num_initial_sql_connections_found == 1


def test_embedded_initial_sql_caches_workbook_download():
    site_source = _make_site_source()
    twbx = _make_twbx(TWB_EMBEDDED)

    def fake_wb_download(luid, filepath=None, include_extract=True):
        path = Path(filepath) / "wb.twbx"
        path.write_bytes(twbx)
        return str(path)

    with mock.patch.object(
        site_source.server.workbooks, "download", side_effect=fake_wb_download
    ) as m:
        site_source.get_initial_sql_lineage_embedded(
            {"name": "Sales", "workbook": {"luid": "wb-1"}}, "urn:1"
        )
        site_source.get_initial_sql_lineage_embedded(
            {"name": "Inventory", "workbook": {"luid": "wb-1"}}, "urn:2"
        )
    assert m.call_count == 1
    # Two datasources processed off one cached workbook download.
    assert site_source.report.num_initial_sql_datasources_processed == 2


def test_embedded_initial_sql_unmatched_name_counts_and_skips():
    site_source = _make_site_source()
    twbx = _make_twbx(TWB_EMBEDDED)

    def fake_wb_download(luid, filepath=None, include_extract=True):
        path = Path(filepath) / "wb.twbx"
        path.write_bytes(twbx)
        return str(path)

    with mock.patch.object(
        site_source.server.workbooks, "download", side_effect=fake_wb_download
    ):
        result = site_source.get_initial_sql_lineage_embedded(
            {"name": "DoesNotExist", "workbook": {"luid": "wb-1"}}, "urn:x"
        )
    assert result.upstreams == [] and result.raw_sql is None
    assert site_source.report.num_initial_sql_embedded_datasources_unmatched == 1


def test_embedded_initial_sql_no_workbook_luid_returns_empty():
    site_source = _make_site_source()
    result = site_source.get_initial_sql_lineage_embedded(
        {"name": "Sales", "workbook": {}}, "urn:x"
    )
    assert result.upstreams == [] and result.raw_sql is None


def test_embedded_initial_sql_download_failure_counts():
    site_source = _make_site_source()
    with mock.patch.object(
        site_source.server.workbooks, "download", side_effect=RuntimeError("boom")
    ):
        result = site_source.get_initial_sql_lineage_embedded(
            {"name": "Sales", "workbook": {"luid": "wb-1"}}, "urn:x"
        )
    assert result.upstreams == [] and result.raw_sql is None
    assert site_source.report.num_initial_sql_download_failures == 1


def test_embedded_initial_sql_failed_workbook_download_not_retried():
    # A failed workbook download is negatively cached: a second embedded datasource
    # from the same workbook must NOT trigger another download.
    site_source = _make_site_source()
    with mock.patch.object(
        site_source.server.workbooks, "download", side_effect=RuntimeError("boom")
    ) as m:
        site_source.get_initial_sql_lineage_embedded(
            {"name": "Sales", "workbook": {"luid": "wb-1"}}, "urn:1"
        )
        site_source.get_initial_sql_lineage_embedded(
            {"name": "Inventory", "workbook": {"luid": "wb-1"}}, "urn:2"
        )
    assert m.call_count == 1
    assert site_source.report.num_initial_sql_download_failures == 1


def test_extract_initial_sql_connections_skips_whitespace_only():
    # one-time-sql present but whitespace-only must be treated as "no Initial SQL".
    tds = TDS_WITH_INITIAL_SQL.replace(
        b"one-time-sql='CREATE TEMPORARY TABLE TMP AS SELECT * FROM ANALYTICS_DB.PUBLIC.SOURCE_TABLE'",
        b"one-time-sql='   '",
    )
    assert extract_initial_sql_connections(tds) == []


def test_get_initial_sql_lineage_zip_without_tds_counts_definition_missing():
    # Download succeeds but the archive has no .tds: silent skip, NOT a download
    # failure — counted distinctly as a missing definition.
    site_source = _make_site_source()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("data.hyper", b"extract-data")
    empty_zip = buf.getvalue()

    def fake_download(luid, filepath=None, include_extract=True):
        path = Path(filepath) / "ds.tdsx"
        path.write_bytes(empty_zip)
        return str(path)

    with mock.patch.object(
        site_source.server.datasources, "download", side_effect=fake_download
    ):
        result = site_source.get_initial_sql_lineage(
            datasource={"luid": "abc-123", "id": "dsid"},
            datasource_urn="urn:li:dataset:(urn:li:dataPlatform:tableau,dsid,PROD)",
        )

    assert result.upstreams == [] and result.raw_sql is None
    assert site_source.report.num_initial_sql_download_failures == 0
    assert site_source.report.num_initial_sql_definitions_missing == 1
    assert site_source.report.num_initial_sql_datasources_processed == 0


def test_emit_datasource_does_not_download_when_ingest_initial_sql_disabled():
    # When ingest_initial_sql=False, emit_datasource must not touch either download API.
    site_source = _make_site_source(ingest_initial_sql=False)
    site_source.config.ingest_virtual_connections = False

    datasource = {
        "id": "dsid",
        "luid": "abc-123",
        "name": "ds",
        "isUnsupportedCustomSql": False,
    }

    with (
        mock.patch.object(site_source.server.datasources, "download") as ds_download,
        mock.patch.object(site_source.server.workbooks, "download") as wb_download,
        mock.patch.object(
            site_source, "_get_datasource_project_luid", return_value="proj-luid"
        ),
        mock.patch.object(
            site_source, "_get_project_browse_path_name", return_value=None
        ),
        mock.patch.object(
            site_source, "_get_datasource_container_key", return_value=None
        ),
    ):
        list(site_source.emit_datasource(datasource, is_embedded_ds=False))

    ds_download.assert_not_called()
    wb_download.assert_not_called()


def test_emit_datasource_suppresses_custom_property_when_disabled():
    # initial_sql_as_custom_property=False must not write the 'initialSql' custom property,
    # while lineage (still enabled) is unaffected.
    site_source = _make_site_source()
    site_source.config.initial_sql_as_custom_property = False
    tdsx = _make_tdsx(TDS_WITH_INITIAL_SQL)

    def fake_download(luid, filepath=None, include_extract=True):
        path = Path(filepath) / "ds.tdsx"
        path.write_bytes(tdsx)
        return str(path)

    with (
        mock.patch.object(
            site_source.server.datasources, "download", side_effect=fake_download
        ),
        mock.patch.object(
            site_source, "_get_datasource_project_luid", return_value="proj-luid"
        ),
        mock.patch.object(
            site_source, "_get_project_browse_path_name", return_value=None
        ),
        mock.patch.object(
            site_source, "_get_datasource_container_key", return_value=None
        ),
    ):
        workunits = list(
            site_source.emit_datasource(
                {
                    "id": "dsid",
                    "luid": "abc-123",
                    "name": "ds",
                    "isUnsupportedCustomSql": False,
                },
                is_embedded_ds=False,
            )
        )

    props = [
        aspect
        for wu in workunits
        if hasattr(wu.metadata, "proposedSnapshot")
        for aspect in wu.metadata.proposedSnapshot.aspects
        if isinstance(aspect, DatasetPropertiesClass)
    ]
    assert len(props) == 1
    assert "initialSql" not in (props[0].customProperties or {})
    # Lineage is still emitted (initial_sql_as_lineage remains True).
    upstream_aspects = [
        wu.metadata.aspect
        for wu in workunits
        if hasattr(wu.metadata, "aspect")
        and isinstance(wu.metadata.aspect, UpstreamLineageClass)
    ]
    assert len(upstream_aspects) == 1


def test_clean_tableau_query_parameters_resolves_params_and_vars():
    clean = TableauSiteSource._clean_tableau_query_parameters
    # Tableau parameter templating is resolved to a literal so the SQL can parse.
    assert "<" not in clean("SELECT * FROM t WHERE region = <[Parameters].[Region]>")
    # T-SQL / session variables (incl. digit-leading and @@system) are neutralized.
    out = clean("SELECT * FROM t WHERE d >= @since AND d < @2years AND s = @@spid")
    assert "@" not in out


def test_make_upstream_class_drops_temp_table_upstreams():
    # Per-statement parsing can surface SQL Server #/## temp tables as upstreams;
    # they are session-local pseudo-tables and must be dropped (real tables kept).
    real = "urn:li:dataset:(urn:li:dataPlatform:mssql,db.dbo.real_table,PROD)"
    local_temp = "urn:li:dataset:(urn:li:dataPlatform:mssql,db.dbo.#tmp_stage,PROD)"
    global_temp = "urn:li:dataset:(urn:li:dataPlatform:mssql,db.dbo.##shared_tmp,PROD)"
    parsed = SqlParsingResult(in_tables=[real, local_temp, global_temp], out_tables=[])
    upstreams = make_upstream_class(parsed)
    assert [u.dataset for u in upstreams] == [real]


def test_make_upstream_class_trims_linked_server_prefix():
    # A SQL Server four-part name [linked_server].db.schema.table is parsed with all
    # four parts, but a DataHub mssql dataset is database.schema.table -- the leading
    # linked-server segment must be dropped so the upstream matches the real dataset.
    four_part = "urn:li:dataset:(urn:li:dataPlatform:mssql,linked_srv.app_db.dbo.fact_table,PROD)"
    three_part = "urn:li:dataset:(urn:li:dataPlatform:mssql,app_db.dbo.fact_table,PROD)"
    parsed = SqlParsingResult(in_tables=[four_part], out_tables=[])
    assert [u.dataset for u in make_upstream_class(parsed)] == [three_part]


def test_make_upstream_class_preserves_platform_instance_when_trimming():
    # With a platform instance, the URN name is instance.<parsed-name>; trimming the
    # over-qualified parsed name must keep the instance segment intact (the linked
    # server sits between the instance and the database).
    four_part = "urn:li:dataset:(urn:li:dataPlatform:mssql,inst.linked_srv.app_db.dbo.fact_table,PROD)"
    expected = (
        "urn:li:dataset:(urn:li:dataPlatform:mssql,inst.app_db.dbo.fact_table,PROD)"
    )
    parsed = SqlParsingResult(in_tables=[four_part], out_tables=[])
    upstreams = make_upstream_class(parsed, platform_instance="inst")
    assert [u.dataset for u in upstreams] == [expected]


def test_make_upstream_class_leaves_well_formed_names_unchanged():
    # Three-part mssql and two-part two-tier names (with or without instance) are
    # already at the right depth and must not be altered.
    mssql_3 = "urn:li:dataset:(urn:li:dataPlatform:mssql,db.dbo.tbl,PROD)"
    mysql_2 = "urn:li:dataset:(urn:li:dataPlatform:mysql,db.tbl,PROD)"
    mysql_2_inst = "urn:li:dataset:(urn:li:dataPlatform:mysql,inst.db.tbl,PROD)"
    parsed = SqlParsingResult(in_tables=[mssql_3, mysql_2, mysql_2_inst], out_tables=[])
    upstreams = make_upstream_class(parsed, platform_instance="inst")
    assert [u.dataset for u in upstreams] == [mssql_3, mysql_2, mysql_2_inst]


def test_initial_sql_with_tableau_parameter_extracts_lineage():
    # A Tableau parameter in Initial SQL must be resolved before parsing so the real
    # source table is still captured; the raw SQL is preserved for the custom property.
    site_source = _make_site_source()
    tdsx = _make_tdsx(TDS_PARAM_INITIAL_SQL)

    def fake_download(luid, filepath=None, include_extract=True):
        path = Path(filepath) / "ds.tdsx"
        path.write_bytes(tdsx)
        return str(path)

    with mock.patch.object(
        site_source.server.datasources, "download", side_effect=fake_download
    ):
        result = site_source.get_initial_sql_lineage(
            datasource={"luid": "abc-123", "id": "dsid"},
            datasource_urn="urn:li:dataset:(urn:li:dataPlatform:tableau,dsid,PROD)",
        )

    assert [u.dataset for u in result.upstreams] == [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics_db.public.source_table,PROD)"
    ]
    assert site_source.report.num_initial_sql_parse_failures == 0
    # Raw, un-cleaned parameter text is preserved for the initialSql custom property.
    assert result.raw_sql is not None and "<[Parameters].[Region]>" in result.raw_sql


# A SQL Server (mssql) Initial SQL batch: comment header + DECLARE/SET + two
# SELECT ... INTO #temp statements, with NO semicolons (idiomatic T-SQL). Generic,
# synthetic names only.
TDS_TSQL_BATCH_INITIAL_SQL = b"""<?xml version='1.0' encoding='utf-8' ?>
<datasource formatted-name='federated.test' inline='true' version='18.1'>
  <connection class='federated'>
    <named-connections>
      <named-connection caption='mssql' name='sqlserver.test'>
        <connection class='sqlserver' dbname='MYDB' schema='dbo' server='sql.example.com' one-time-sql='DECLARE @since as DATE SET @since = DATEADD(yy, -1, GETDATE()) SELECT a.[id] INTO #stage FROM [MYDB].[dbo].[table_a] as a WHERE a.[d] >= @since SELECT b.[id] INTO #stage2 FROM #stage as s LEFT JOIN [MYDB].[dbo].[table_b] as b ON s.[id] = b.[id]' />
      </named-connection>
    </named-connections>
  </connection>
</datasource>"""


def test_initial_sql_tsql_batch_splits_and_extracts_real_tables():
    # End-to-end: a no-semicolon T-SQL batch is split (mssql -> tsql -> keyword
    # boundaries), each statement cleaned + parsed; both real source tables are
    # captured and the #temp tables are dropped.
    site_source = _make_site_source()
    tdsx = _make_tdsx(TDS_TSQL_BATCH_INITIAL_SQL)

    def fake_download(luid, filepath=None, include_extract=True):
        path = Path(filepath) / "ds.tdsx"
        path.write_bytes(tdsx)
        return str(path)

    with mock.patch.object(
        site_source.server.datasources, "download", side_effect=fake_download
    ):
        result = site_source.get_initial_sql_lineage(
            datasource={"luid": "abc-123", "id": "dsid"},
            datasource_urn="urn:li:dataset:(urn:li:dataPlatform:tableau,dsid,PROD)",
        )

    datasets = {u.dataset for u in result.upstreams}
    assert (
        "urn:li:dataset:(urn:li:dataPlatform:mssql,mydb.dbo.table_a,PROD)" in datasets
    )
    assert (
        "urn:li:dataset:(urn:li:dataPlatform:mssql,mydb.dbo.table_b,PROD)" in datasets
    )
    # The #stage / #stage2 temp tables must be filtered out.
    assert not any("#" in u for u in datasets)


@pytest.mark.parametrize(
    "platform,parsed_name,expected_name",
    [
        # Two-tier platforms: SQL parsing keeps db.schema.table, but the GraphQL path
        # yields schema.table (get_overridden_info nulls upstream_db). Normalization
        # must trim to 2 parts so both lineage paths emit the same URN.
        # teradata is the regression case — it is nulled by get_overridden_info and so
        # must also be in _TWO_TIER_PLATFORMS.
        ("teradata", "db.schema.tbl", "schema.tbl"),
        ("mysql", "db.schema.tbl", "schema.tbl"),
        ("clickhouse", "db.schema.tbl", "schema.tbl"),
        # Three-tier platforms keep all three segments.
        ("snowflake", "db.schema.tbl", "db.schema.tbl"),
    ],
)
def test_normalize_parsed_upstream_urn_trims_by_tier(
    platform: str, parsed_name: str, expected_name: str
) -> None:
    urn = f"urn:li:dataset:(urn:li:dataPlatform:{platform},{parsed_name},PROD)"
    expected = f"urn:li:dataset:(urn:li:dataPlatform:{platform},{expected_name},PROD)"
    assert _normalize_parsed_upstream_urn(urn, None) == expected
