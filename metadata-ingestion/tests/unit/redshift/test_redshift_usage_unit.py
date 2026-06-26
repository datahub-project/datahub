from datetime import datetime, timezone
from typing import Dict, List, Optional, Union
from unittest.mock import MagicMock

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.ingestion.source.redshift.config import RedshiftConfig
from datahub.ingestion.source.redshift.redshift_schema import (
    RedshiftTable,
    RedshiftView,
)
from datahub.ingestion.source.redshift.report import RedshiftReport
from datahub.ingestion.source.redshift.usage import (
    RedshiftAccessEvent,
    RedshiftUsageExtractor,
)
from datahub.metadata.schema_classes import DatasetUsageStatisticsClass

ALL_TABLES: Dict[str, Dict[str, List[Union[RedshiftView, RedshiftTable]]]] = {
    "dev": {
        "public": [
            RedshiftTable(
                name="users",
                schema="public",
                type="BASE TABLE",
                created=None,
                comment="",
            ),
            RedshiftTable(
                name="orders",
                schema="public",
                type="BASE TABLE",
                created=None,
                comment="",
            ),
        ]
    }
}


def _usage_extractor(
    *,
    include_usage_statistics: bool = True,
    include_column_usage_stats: bool = False,
    include_operational_stats: bool = False,
    email_domain: Optional[str] = "acryl.io",
) -> RedshiftUsageExtractor:
    config = RedshiftConfig(
        host_port="test:1234",
        database="dev",
        email_domain=email_domain,
        include_usage_statistics=include_usage_statistics,
        include_column_usage_stats=include_column_usage_stats,
        include_operational_stats=include_operational_stats,
        start_time="2024-01-01T00:00:00Z",
        end_time="2024-01-02T00:00:00Z",
    )
    return RedshiftUsageExtractor(
        config=config,
        connection=MagicMock(),
        report=RedshiftReport(),
        dataset_urn_builder=lambda table: make_dataset_urn("redshift", table),
    )


def _access_event(table: str) -> RedshiftAccessEvent:
    return RedshiftAccessEvent(
        userid=1,
        username="alice",
        query=1,
        querytxt=f"select col_a from public.{table}",
        tbl=1,
        database="dev",
        schema="public",
        table=table,
        starttime=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        endtime=datetime(2024, 1, 1, 12, 0, 1, tzinfo=timezone.utc),
    )


def _usage_aspect_urns(extractor: RedshiftUsageExtractor) -> List[str]:
    wus = list(extractor.get_usage_workunits(all_tables=ALL_TABLES))
    return [
        str(getattr(wu.metadata, "entityUrn", ""))
        for wu in wus
        if isinstance(getattr(wu.metadata, "aspect", None), DatasetUsageStatisticsClass)
    ]


def test_default_usage_path_emits_usage_and_auto_empty(monkeypatch):
    # Default (stl_scan) path: a read on `users` is attributed via add_preparsed_query,
    # and the unused `orders` table still gets an (empty) usage aspect via the
    # auto_empty backfill.
    extractor = _usage_extractor(include_column_usage_stats=False)
    monkeypatch.setattr(
        extractor,
        "_gen_access_events_from_history_query",
        lambda *a, **k: iter([_access_event("users")]),
    )
    urns = _usage_aspect_urns(extractor)
    assert any("public.users" in u for u in urns)
    assert any("public.orders" in u for u in urns)  # auto_empty backfill


def test_v2_usage_path_emits_no_usage_aspects_from_extractor():
    # In column-usage (v2) mode the lineage aggregator owns usage, so this
    # extractor emits no usage aspects and skips the auto_empty backfill.
    extractor = _usage_extractor(include_column_usage_stats=True)
    assert _usage_aspect_urns(extractor) == []


def test_access_event_to_preparsed_query():
    extractor = _usage_extractor()
    pq = extractor._access_event_to_preparsed_query(_access_event("users"))
    assert pq.upstreams == [make_dataset_urn("redshift", "dev.public.users")]
    assert pq.query_count == 1
    assert str(pq.user) == "urn:li:corpuser:alice"


def test_usage_user_urn_blank_username_falls_back_to_unknown():
    assert str(_usage_extractor()._user_urn("")) == "urn:li:corpuser:unknown"


def test_usage_user_urn_strips_domain_when_email_already_present():
    urn = _usage_extractor(email_domain="company.com")._user_urn("alice@company.com")
    assert str(urn) == "urn:li:corpuser:alice"


def test_usage_user_urn_without_email_domain_uses_bare_username():
    # email_domain is required when usage is enabled, so disable usage here to
    # exercise the no-domain guard path of _user_urn.
    extractor = _usage_extractor(email_domain=None, include_usage_statistics=False)
    assert str(extractor._user_urn("bob")) == "urn:li:corpuser:bob"


def test_config_lineage_enabled_property():
    base = dict(host_port="test:1234", database="dev", email_domain="acryl.io")
    all_off = RedshiftConfig(
        **base,
        include_table_lineage=False,
        include_view_lineage=False,
        include_copy_lineage=False,
        include_unload_lineage=False,
        include_share_lineage=False,
        include_table_rename_lineage=False,
    )
    assert all_off.lineage_enabled is False
    assert RedshiftConfig(**base, include_view_lineage=True).lineage_enabled is True
