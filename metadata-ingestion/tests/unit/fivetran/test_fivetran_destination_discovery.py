"""Unit tests for REST-API destination discovery."""

from typing import Any, Optional, cast
from unittest.mock import MagicMock, patch

import pydantic
import pytest

from datahub.ingestion.source.fivetran.config import (
    FivetranAPIConfig,
    FivetranSourceConfig,
    PlatformDetail,
)
from datahub.ingestion.source.fivetran.fivetran import FivetranSource
from datahub.ingestion.source.fivetran.fivetran_rest_api import FivetranAPIClient
from datahub.ingestion.source.fivetran.response_models import (
    FivetranDestinationConfig,
    FivetranDestinationDetails,
)


def _make_client() -> FivetranAPIClient:
    return FivetranAPIClient(FivetranAPIConfig(api_key="key", api_secret="secret"))


class TestFivetranDestinationDetailsParsing:
    """Pin the response model against fields we actually consume.
    Unknown fields must be ignored so future Fivetran additions don't break parsing."""

    def test_managed_data_lake_response_parses(self):
        # Real-shape response for an MDL destination with Glue toggle on.
        raw = {
            "id": "test_destination_id",
            "service": "managed_data_lake",
            "region": "AWS_US_EAST_1",
            "group_id": "g_123",
            "setup_status": "CONNECTED",
            "config": {
                "bucket": "my-lake-bucket",
                "prefix_path": "fivetran",
                "region": "us-east-1",
                "table_format": "ICEBERG",
            },
            # extra unknown field — must be ignored
            "future_field_we_dont_know_about": {"x": 1},
        }
        details = FivetranDestinationDetails.model_validate(raw)
        assert details.id == "test_destination_id"
        assert details.service == "managed_data_lake"
        assert details.config.bucket == "my-lake-bucket"
        assert details.config.region == "us-east-1"

    def test_snowflake_response_parses_with_partial_config(self):
        # Snowflake destinations expose different config keys; the model
        # must tolerate config fields it doesn't know about.
        raw = {
            "id": "snowflake_dest_1",
            "service": "snowflake",
            "region": "AWS_US_WEST_2",
            "group_id": "g_456",
            "setup_status": "CONNECTED",
            "config": {
                "host": "abc.snowflakecomputing.com",
                "port": 443,
                "database": "ANALYTICS_DB",
                "user": "fivetran_user",
            },
        }
        details = FivetranDestinationDetails.model_validate(raw)
        assert details.service == "snowflake"
        assert details.config.database == "ANALYTICS_DB"
        # bucket is None for non-MDL destinations
        assert details.config.bucket is None

    def test_unknown_top_level_field_does_not_break(self):
        raw = {
            "id": "x",
            "service": "snowflake",
            "region": "X",
            "group_id": "g",
            "setup_status": "CONNECTED",
            "config": {},
            "some_new_field_added_by_fivetran": [1, 2, 3],
        }
        FivetranDestinationDetails.model_validate(raw)  # must not raise


class TestGetDestinationDetailsByID:
    """`get_destination_details_by_id` is the per-destination REST lookup.
    Must: (1) parse the success envelope, (2) cache per id, (3) raise on
    non-success codes so callers can decide whether to fall back."""

    def test_success_response_parses_and_caches(self):
        client = _make_client()
        fake_resp = MagicMock()
        fake_resp.json.return_value = {
            "code": "Success",
            "data": {
                "id": "dest_1",
                "service": "managed_data_lake",
                "region": "AWS_US_EAST_1",
                "group_id": "g",
                "setup_status": "CONNECTED",
                "config": {"bucket": "b"},
            },
        }
        fake_resp.raise_for_status = MagicMock()

        with patch.object(client._session, "get", return_value=fake_resp) as mocked:
            first = client.get_destination_details_by_id("dest_1")
            second = client.get_destination_details_by_id("dest_1")

        assert first.service == "managed_data_lake"
        assert first is second  # same cached instance
        assert mocked.call_count == 1  # only one HTTP call

    def test_distinct_ids_each_make_one_call(self):
        client = _make_client()

        def _resp(dest_id: str) -> MagicMock:
            r = MagicMock()
            r.json.return_value = {
                "code": "Success",
                "data": {
                    "id": dest_id,
                    "service": "snowflake",
                    "region": "X",
                    "group_id": "g",
                    "setup_status": "CONNECTED",
                    "config": {},
                },
            }
            r.raise_for_status = MagicMock()
            return r

        with patch.object(
            client._session,
            "get",
            side_effect=lambda url, **_: _resp(url.rsplit("/", 1)[-1]),
        ) as mocked:
            client.get_destination_details_by_id("a")
            client.get_destination_details_by_id("b")
            client.get_destination_details_by_id("a")  # cache hit

        assert mocked.call_count == 2

    def test_non_success_code_raises_value_error(self):
        client = _make_client()
        fake_resp = MagicMock()
        fake_resp.json.return_value = {"code": "NotFound", "message": "no such id"}
        fake_resp.raise_for_status = MagicMock()
        with (
            patch.object(client._session, "get", return_value=fake_resp),
            pytest.raises(ValueError, match="NotFound"),
        ):
            client.get_destination_details_by_id("missing")

    def test_invalid_payload_raises_validation_error(self):
        client = _make_client()
        fake_resp = MagicMock()
        # Missing required field `id`.
        fake_resp.json.return_value = {
            "code": "Success",
            "data": {"service": "snowflake", "region": "X", "config": {}},
        }
        fake_resp.raise_for_status = MagicMock()
        with (
            patch.object(client._session, "get", return_value=fake_resp),
            pytest.raises(pydantic.ValidationError),
        ):
            client.get_destination_details_by_id("dest_x")


def _details(service: str, **config_kwargs: Any) -> FivetranDestinationDetails:
    return FivetranDestinationDetails(
        id="d",
        service=service,
        region="X",
        group_id="g",
        setup_status="CONNECTED",
        config=FivetranDestinationConfig(**config_kwargs),
    )


class TestApplyDiscoveredDestination:
    """`apply_discovered_destination` enriches a base PlatformDetail with
    REST-discovered fields. Declarative fields on the base must always win
    so that user overrides remain authoritative."""

    def test_snowflake_service_sets_platform_and_database(self):
        base = PlatformDetail()  # no overrides
        result = FivetranSource.apply_discovered_destination(
            base, _details("snowflake", database="ANALYTICS_DB")
        )
        assert result.platform == "snowflake"
        assert result.database == "ANALYTICS_DB"

    def test_bigquery_service_uses_project_id_as_database(self):
        base = PlatformDetail()
        result = FivetranSource.apply_discovered_destination(
            base, _details("bigquery", project_id="my-bq-project")
        )
        assert result.platform == "bigquery"
        assert result.database == "my-bq-project"

    def test_databricks_service_uses_catalog_as_database(self):
        base = PlatformDetail()
        result = FivetranSource.apply_discovered_destination(
            base, _details("databricks", catalog="main")
        )
        assert result.platform == "databricks"
        assert result.database == "main"

    def test_declarative_platform_wins_over_discovery(self):
        # User said `platform="my_custom_warehouse"` on the override; we must
        # not clobber it with the discovered service.
        base = PlatformDetail(platform="my_custom_warehouse", database="X")
        result = FivetranSource.apply_discovered_destination(
            base, _details("snowflake", database="ANALYTICS_DB")
        )
        assert result.platform == "my_custom_warehouse"
        assert result.database == "X"

    def test_unknown_service_returns_base_unchanged(self):
        # If Fivetran ships a new destination type we don't know about, the
        # caller will use the base default. Surface a warning at the call
        # site, but the helper itself is a pure function — no logging here.
        base = PlatformDetail(platform="snowflake", database="WH")
        result = FivetranSource.apply_discovered_destination(
            base, _details("brand_new_destination_type")
        )
        assert result.platform == "snowflake"
        assert result.database == "WH"

    def test_unknown_service_without_override_leaves_platform_none(self):
        # Regression: previously this synthesized the URN platform from the
        # raw `discovered.service` string, producing junk like
        # `urn:li:dataPlatform:aurora_postgres_warehouse_v2`. The helper
        # now leaves `platform=None` so `build_destination_urn` raises and
        # the caller surfaces a once-per-destination warning telling the
        # user to set `destination_to_platform_instance.<id>.platform`.
        base = PlatformDetail()
        result = FivetranSource.apply_discovered_destination(
            base,
            _details("aurora_postgres_warehouse_v2", database="some_db"),
        )
        assert result.platform is None
        # Discovered database is still mirrored so the user only has to
        # supply `platform` (not both) to fix the recipe.
        assert result.database == "some_db"

    def test_managed_data_lake_defaults_to_iceberg(self):
        # No platform override → modern Polaris / Iceberg-REST default.
        base = PlatformDetail()
        result = FivetranSource.apply_discovered_destination(
            base, _details("managed_data_lake", bucket="b")
        )
        assert result.platform == "iceberg"

    def test_managed_data_lake_glue_when_caller_pins_glue(self):
        # User opts the destination into Glue routing via
        # `destination_to_platform_instance.<id>.platform: glue`. They're
        # also expected to pin `database` (the actual Glue database name)
        # — exercised in TestBuildDestinationUrnGlueMdl.
        base = PlatformDetail(platform="glue")
        result = FivetranSource.apply_discovered_destination(
            base, _details("managed_data_lake", bucket="b")
        )
        assert result.platform == "glue"

    def test_managed_data_lake_auto_detects_glue_from_toggle(self):
        # No user override → auto-detect kicks in. Fivetran's
        # `should_maintain_tables_in_glue: true` means the customer is
        # registering Iceberg tables in Glue, so default URN routing to
        # `glue` saves them from also pinning `platform: glue`. They
        # still need to set `database` separately (the actual Glue
        # database name from their AWS Glue console — REST doesn't
        # expose it).
        base = PlatformDetail()
        result = FivetranSource.apply_discovered_destination(
            base,
            _details(
                "managed_data_lake", bucket="b", should_maintain_tables_in_glue=True
            ),
        )
        assert result.platform == "glue"

    def test_managed_data_lake_user_override_beats_auto_detect(self):
        # User pins `platform: iceberg` on a destination whose MDL config
        # has `should_maintain_tables_in_glue: true`. Override wins —
        # auto-detect must not clobber it.
        base = PlatformDetail(platform="iceberg")
        result = FivetranSource.apply_discovered_destination(
            base,
            _details(
                "managed_data_lake", bucket="b", should_maintain_tables_in_glue=True
            ),
        )
        assert result.platform == "iceberg"

    def test_managed_data_lake_glue_toggle_false_falls_back_to_iceberg(self):
        # Toggle explicitly false (or any falsy value) → no auto-detect
        # → fall through to iceberg default.
        base = PlatformDetail()
        result = FivetranSource.apply_discovered_destination(
            base,
            _details(
                "managed_data_lake", bucket="b", should_maintain_tables_in_glue=False
            ),
        )
        assert result.platform == "iceberg"

    def test_managed_data_lake_user_database_implies_glue(self):
        # User pinned `database` but not `platform`. Treat the database
        # as a glue-intent signal — only Glue routing among MDL platforms
        # uses `database`, so iceberg/s3/gcs/abs would silently drop it.
        # This protects against the silent foot-gun where the customer's
        # destination doesn't have `should_maintain_tables_in_glue: true`
        # set but they intend Glue routing anyway.
        base = PlatformDetail(database="my_glue_db")
        result = FivetranSource.apply_discovered_destination(
            base,
            _details("managed_data_lake", bucket="b"),
        )
        assert result.platform == "glue"
        assert result.database == "my_glue_db"

    def test_managed_data_lake_user_database_overrides_iceberg_default(self):
        # Same as above but the destination has the toggle off. Still
        # routes to glue because `database` is explicit user intent.
        base = PlatformDetail(database="my_glue_db")
        result = FivetranSource.apply_discovered_destination(
            base,
            _details(
                "managed_data_lake", bucket="b", should_maintain_tables_in_glue=False
            ),
        )
        assert result.platform == "glue"
        assert result.database == "my_glue_db"

    def test_managed_data_lake_s3_pins_platform_and_fills_database(self):
        # User opts into S3 path URNs by pinning `platform: s3`. Discovery
        # populates `database` with `<bucket>/<prefix_path>` so
        # build_destination_urn can prepend it.
        base = PlatformDetail(platform="s3")
        result = FivetranSource.apply_discovered_destination(
            base,
            _details(
                "managed_data_lake",
                bucket="example-fivetran-lake",
                prefix_path="fivetran",
            ),
        )
        assert result.platform == "s3"
        assert result.database == "example-fivetran-lake/fivetran"

    def test_managed_data_lake_s3_without_prefix_path(self):
        # Some MDL destinations lack a prefix_path (data lands at the
        # bucket root). The URN-prefix should be just the bucket — no
        # trailing slash, no `None` literal.
        base = PlatformDetail(platform="s3")
        result = FivetranSource.apply_discovered_destination(
            base, _details("managed_data_lake", bucket="my-bucket")
        )
        assert result.platform == "s3"
        assert result.database == "my-bucket"

    def test_managed_data_lake_s3_declarative_database_wins(self):
        # User override wins: if `destination_to_platform_instance` sets
        # `database` for this destination, discovery must not clobber it.
        # Lets users point lineage at a different bucket layout (e.g.,
        # mounted via a different prefix in DataHub's S3 source).
        base = PlatformDetail(platform="s3", database="custom-bucket/custom-prefix")
        result = FivetranSource.apply_discovered_destination(
            base,
            _details(
                "managed_data_lake",
                bucket="discovered-bucket",
                prefix_path="fivetran",
            ),
        )
        assert result.platform == "s3"
        assert result.database == "custom-bucket/custom-prefix"

    def test_managed_data_lake_gcs_pins_platform_and_fills_database(self):
        # GCS-backed MDL exposes the same `service: managed_data_lake` as
        # AWS, distinguished by `gcs_project_id` on the request side. The
        # URN-prefix shape is identical to S3: `<bucket>/<prefix_path>`.
        base = PlatformDetail(platform="gcs")
        result = FivetranSource.apply_discovered_destination(
            base,
            _details(
                "managed_data_lake",
                bucket="example-gcs-lake",
                prefix_path="fivetran",
                gcs_project_id="my-gcp-project",
            ),
        )
        assert result.platform == "gcs"
        assert result.database == "example-gcs-lake/fivetran"

    def test_managed_data_lake_abs_pins_platform_and_fills_database(self):
        # ADLS Gen2 uses `storage_account_name` + `container_name` instead
        # of a single bucket. The URN-prefix is composed as
        # `<storage_account>/<container>/<prefix_path>` so it aligns with
        # DataHub's ABS source which treats the storage account as the
        # top-level container.
        base = PlatformDetail(platform="abs")
        result = FivetranSource.apply_discovered_destination(
            base,
            _details(
                "managed_data_lake",
                storage_account_name="myadlsaccount",
                container_name="datalake",
                prefix_path="fivetran",
            ),
        )
        assert result.platform == "abs"
        assert result.database == "myadlsaccount/datalake/fivetran"

    def test_managed_data_lake_abs_without_prefix_path(self):
        # ADLS data landing at the container root (no prefix_path).
        base = PlatformDetail(platform="abs")
        result = FivetranSource.apply_discovered_destination(
            base,
            _details(
                "managed_data_lake",
                storage_account_name="acct",
                container_name="datalake",
            ),
        )
        assert result.platform == "abs"
        assert result.database == "acct/datalake"


def _make_source_with_discovery(
    api_client: Optional[MagicMock] = None,
    overrides: Optional[dict] = None,
) -> FivetranSource:
    """Build a FivetranSource without booting the log API (mocks the engine).

    REST discovery now activates implicitly whenever an `api_client` is set
    and the per-destination `PlatformDetail.platform` isn't already populated,
    so no opt-in flag is required.
    """
    cfg = FivetranSourceConfig.model_validate(
        {
            "fivetran_log_config": {
                "destination_platform": "snowflake",
                "snowflake_destination_config": {
                    "account_id": "x",
                    "username": "u",
                    "password": "p",
                    "warehouse": "w",
                    "database": "d",
                    "log_schema": "s",
                },
            },
            "api_config": {"api_key": "k", "api_secret": "s"},
            "destination_to_platform_instance": overrides or {},
        }
    )
    # Bypass the FivetranLogDbReader engine setup — we don't need it for this test.
    src = FivetranSource.__new__(FivetranSource)
    src.config = cfg
    src.report = MagicMock()
    src.log_reader = MagicMock()
    src.log_reader.fivetran_log_database = "fivetran_log_db"
    src.api_client = api_client
    src._failed_destination_ids = set()
    src._destinations_with_urn_warning = set()
    return src


class TestResolveDestinationDetails:
    """`resolve_destination_details` is the new entry point that combines
    declarative override + REST discovery into a single PlatformDetail."""

    def test_declarative_override_wins_over_rest_discovery(self):
        # User-pinned `platform` and `database` survive REST discovery.
        # Discovery still runs (one cached call per unique destination per
        # ingest) — the per-field merge in `apply_discovered_destination`
        # is what protects the override, not skipping the call.
        api_client = MagicMock()
        api_client.get_destination_details_by_id.return_value = (
            FivetranDestinationDetails(
                id="dest_a",
                service="snowflake",
                region="X",
                group_id="g",
                setup_status="CONNECTED",
                config=FivetranDestinationConfig(database="DISCOVERED_DB"),
            )
        )
        src = _make_source_with_discovery(
            api_client=api_client,
            overrides={
                "dest_a": {"platform": "snowflake", "database": "PROD"},
            },
        )
        result = src.resolve_destination_details("dest_a")
        assert result.platform == "snowflake"
        assert result.database == "PROD"  # override wins over DISCOVERED_DB

    def test_rest_discovery_used_when_no_override(self):
        api_client = MagicMock()
        api_client.get_destination_details_by_id.return_value = (
            FivetranDestinationDetails(
                id="dest_b",
                service="snowflake",
                region="X",
                group_id="g",
                setup_status="CONNECTED",
                config=FivetranDestinationConfig(database="ANALYTICS_DB"),
            )
        )
        src = _make_source_with_discovery(api_client=api_client)
        result = src.resolve_destination_details("dest_b")
        assert result.platform == "snowflake"
        assert result.database == "ANALYTICS_DB"
        api_client.get_destination_details_by_id.assert_called_once_with("dest_b")

    def test_rest_discovery_for_managed_data_lake(self):
        # MDL destinations default to iceberg URN routing (Polaris / Iceberg
        # REST). Users with non-Iceberg backings override via
        # destination_to_platform_instance.
        api_client = MagicMock()
        api_client.get_destination_details_by_id.return_value = (
            FivetranDestinationDetails(
                id="dest_c",
                service="managed_data_lake",
                region="AWS_US_WEST_2",
                group_id="g",
                setup_status="CONNECTED",
                config=FivetranDestinationConfig(bucket="b"),
            )
        )
        src = _make_source_with_discovery(api_client=api_client)
        result = src.resolve_destination_details("dest_c")
        assert result.platform == "iceberg"

    def test_rest_failure_emits_warning_and_falls_back_to_default(self):
        api_client = MagicMock()
        api_client.get_destination_details_by_id.side_effect = ValueError("boom")
        src = _make_source_with_discovery(api_client=api_client)
        result = src.resolve_destination_details("dest_d")
        # Falls back to fivetran_log_config.destination_platform
        assert result.platform == "snowflake"
        # And the report records the warning. (`report` is a MagicMock —
        # cast for mypy because the real type-annotation is the concrete report.)
        report_mock = cast(MagicMock, src.report)
        report_mock.warning.assert_called_once()

    def test_failed_destination_id_is_not_retried(self):
        # `FivetranAPIClient` only caches successful destination lookups, so
        # without a negative cache every connector on a broken destination
        # would re-issue the API call and emit a fresh warning. This pins
        # the negative-cache behavior: one API call, one warning per
        # destination, regardless of how many connectors live on it.
        api_client = MagicMock()
        api_client.get_destination_details_by_id.side_effect = ValueError("boom")
        src = _make_source_with_discovery(api_client=api_client)

        # Three connectors, all on the same broken destination.
        for _ in range(3):
            result = src.resolve_destination_details("broken_dest")
            assert result.platform == "snowflake"  # falls back to default

        # Only one API call (first connector); subsequent calls short-circuit.
        api_client.get_destination_details_by_id.assert_called_once_with("broken_dest")
        # Exactly one warning, not three.
        report_mock = cast(MagicMock, src.report)
        report_mock.warning.assert_called_once()

    def test_no_api_client_skips_rest(self):
        # Without an API client (DB-only mode), no discovery happens — the
        # connector falls back to `fivetran_log_config.destination_platform`.
        src = _make_source_with_discovery(api_client=None)
        result = src.resolve_destination_details("dest_e")
        assert result.platform == "snowflake"  # default

    def test_mdl_glue_routing_via_platform_override(self):
        # End-to-end: an MDL destination with `platform: glue` pinned in
        # `destination_to_platform_instance` resolves to platform=glue. The
        # `_NON_RELATIONAL_DESTINATION_PLATFORMS` membership prevents the
        # DB-mode `database = fivetran_log_db` fallback from polluting the
        # resolved details.
        api_client = MagicMock()
        api_client.get_destination_details_by_id.return_value = (
            FivetranDestinationDetails(
                id="glue_warehouse_a",
                service="managed_data_lake",
                region="AWS_US_WEST_2",
                group_id="g",
                setup_status="CONNECTED",
                config=FivetranDestinationConfig(bucket="b"),
            )
        )
        src = _make_source_with_discovery(
            api_client=api_client,
            overrides={"glue_warehouse_a": {"platform": "glue"}},
        )
        result = src.resolve_destination_details("glue_warehouse_a")
        assert result.platform == "glue"
        # DB-mode fallback skipped for non-relational platforms.
        assert result.database is None

    def test_mdl_s3_routing_via_platform_override(self):
        # End-to-end: an MDL destination with `platform: s3` pinned →
        # discovery still fires and populates `database` with
        # `<bucket>/<prefix_path>` for downstream URN construction. This is
        # the case `mdl_destinations_to_catalog_type` originally existed
        # for; it's now subsumed by the platform override.
        api_client = MagicMock()
        api_client.get_destination_details_by_id.return_value = (
            FivetranDestinationDetails(
                id="s3_lake_a",
                service="managed_data_lake",
                region="AWS_US_WEST_2",
                group_id="g",
                setup_status="CONNECTED",
                config=FivetranDestinationConfig(
                    bucket="example-fivetran-lake", prefix_path="fivetran"
                ),
            )
        )
        src = _make_source_with_discovery(
            api_client=api_client,
            overrides={"s3_lake_a": {"platform": "s3"}},
        )
        result = src.resolve_destination_details("s3_lake_a")
        assert result.platform == "s3"
        assert result.database == "example-fivetran-lake/fivetran"


class TestBuildDestinationUrnGlueMdl:
    """Glue-routed Managed Data Lake destinations are treated as relational:
    URN shape `glue.<database>.<schema>.<table>` where `<database>` is the
    user-supplied actual Glue database name (Fivetran shares one Glue
    database per region; the REST API does not expose its name). The
    customer is responsible for verifying that Fivetran's Glue tables are
    literally named `<schema>.<table>` so this URN aligns with DataHub's
    Glue source.
    """

    def test_glue_requires_user_supplied_database(self):
        # No `database` set → ValueError. Caller catches and skips the
        # lineage edge with a structured warning. Pin this so the
        # failure mode stays well-defined.
        with pytest.raises(ValueError, match="database must be set"):
            FivetranSource.build_destination_urn(
                destination_table="public.employee",
                destination_details=PlatformDetail(platform="glue"),
            )

    def test_glue_with_user_supplied_database(self):
        # Customer inspects their Glue console, finds the actual database
        # (e.g., `fivetran_managed_data_lake_us_west_2`), and pins it on
        # `destination_to_platform_instance.<id>.database`. URN follows
        # the relational shape with `<schema>.<table>` as the literal
        # Glue table name.
        urn = FivetranSource.build_destination_urn(
            destination_table="public.employee",
            destination_details=PlatformDetail(
                platform="glue",
                database="fivetran_managed_data_lake_us_west_2",
            ),
        )
        assert (
            str(urn)
            == "urn:li:dataset:(urn:li:dataPlatform:glue,fivetran_managed_data_lake_us_west_2.public.employee,PROD)"
        )

    def test_glue_database_is_lowercased(self):
        # Same as the relational platforms: `database` is lowercased to
        # match DataHub's Glue source URN convention. AWS Glue itself
        # normalises database names to lowercase, so this is safe.
        urn = FivetranSource.build_destination_urn(
            destination_table="public.employee",
            destination_details=PlatformDetail(
                platform="glue",
                database="Fivetran_DataLake",
            ),
        )
        assert (
            str(urn)
            == "urn:li:dataset:(urn:li:dataPlatform:glue,fivetran_datalake.public.employee,PROD)"
        )

    def test_glue_database_case_preserved_when_opted_out(self):
        # `database_lowercase=False` is the per-destination opt-out for
        # users whose paired DataHub Glue source URN preserves database
        # casing. Default (True) keeps the long-standing behaviour
        # exercised by `test_glue_database_is_lowercased`.
        urn = FivetranSource.build_destination_urn(
            destination_table="public.employee",
            destination_details=PlatformDetail(
                platform="glue",
                database="Fivetran_DataLake",
                database_lowercase=False,
            ),
        )
        assert (
            str(urn)
            == "urn:li:dataset:(urn:li:dataPlatform:glue,Fivetran_DataLake.public.employee,PROD)"
        )


class TestDatabaseForUrn:
    """`PlatformDetail.database_for_urn` centralises the `database` case
    rule for URN construction. Default lowercases (matches DataHub's
    standard URN convention and the long-standing Fivetran behaviour);
    `database_lowercase=False` on the matching `PlatformDetail` is the
    per-destination opt-out. Exposed as a `@property` (not a Pydantic
    `@computed_field`) so it stays out of `model_dump()` and doesn't
    leak into `_compose_custom_properties` — the customProperties
    aspect intentionally surfaces the user-typed `database` verbatim."""

    def test_returns_none_when_database_unset(self):
        assert PlatformDetail().database_for_urn is None

    def test_default_lowercases(self):
        assert (
            PlatformDetail(database="ANALYTICS_DB").database_for_urn == "analytics_db"
        )

    def test_opted_out_preserves_case(self):
        assert (
            PlatformDetail(
                database="ANALYTICS_DB", database_lowercase=False
            ).database_for_urn
            == "ANALYTICS_DB"
        )

    def test_property_does_not_appear_in_model_dump(self):
        # Regression: derive-on-read must NOT show up as a serialized field
        # — `_compose_custom_properties` in `fivetran.py` calls
        # `model_dump()` to build the `destination.*` customProperties
        # aspect, and we don't want a derived `database_for_urn: ...`
        # entry there alongside the user's `database` field.
        details = PlatformDetail(database="ANALYTICS_DB")
        dumped = details.model_dump()
        assert "database" in dumped
        assert "database_for_urn" not in dumped


class TestBuildDestinationUrnS3Mdl:
    """S3-routed Managed Data Lake destinations construct path-style URNs
    aligned with DataHub's S3 source: `<bucket>/<prefix>/<schema>/<table>`.
    The bucket/prefix is carried in `database`, set by REST discovery.
    """

    def test_s3_with_bucket_and_prefix(self):
        # Illustrative example: bucket `example-fivetran-lake` with prefix
        # `fivetran`, schema `sales`, table `orders` → flat S3 path URN.
        urn = FivetranSource.build_destination_urn(
            destination_table="sales.orders",
            destination_details=PlatformDetail(
                platform="s3", database="example-fivetran-lake/fivetran"
            ),
        )
        assert (
            str(urn)
            == "urn:li:dataset:(urn:li:dataPlatform:s3,example-fivetran-lake/fivetran/sales/orders,PROD)"
        )

    def test_s3_with_bucket_only(self):
        # Bucket-root layouts: no prefix between bucket and schema/table.
        urn = FivetranSource.build_destination_urn(
            destination_table="sales.orders",
            destination_details=PlatformDetail(platform="s3", database="my-bucket"),
        )
        assert (
            str(urn)
            == "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/sales/orders,PROD)"
        )

    def test_s3_with_trailing_slash_in_database(self):
        # Defensive: callers (or future overrides) might supply a trailing
        # slash. Don't double up the slashes.
        urn = FivetranSource.build_destination_urn(
            destination_table="sales.orders",
            destination_details=PlatformDetail(
                platform="s3", database="example-fivetran-lake/fivetran/"
            ),
        )
        assert (
            str(urn)
            == "urn:li:dataset:(urn:li:dataPlatform:s3,example-fivetran-lake/fivetran/sales/orders,PROD)"
        )

    def test_s3_without_database_raises(self):
        # Discovery must populate the bucket/prefix; if it didn't we refuse
        # to emit a half-baked URN. The caller catches ValueError and skips
        # this lineage edge with a structured warning.
        with pytest.raises(ValueError, match="path prefix"):
            FivetranSource.build_destination_urn(
                destination_table="sales.orders",
                destination_details=PlatformDetail(platform="s3"),
            )

    def test_s3_respects_include_schema_in_urn_false(self):
        # When the user has set include_schema_in_urn=False, the schema
        # part is stripped from `destination_table` before URN construction.
        # The S3 path then becomes `<bucket>/<prefix>/<table>` with no
        # intermediate schema directory.
        urn = FivetranSource.build_destination_urn(
            destination_table="sales.orders",
            destination_details=PlatformDetail(
                platform="s3",
                database="my-bucket/fivetran",
                include_schema_in_urn=False,
            ),
        )
        assert (
            str(urn)
            == "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/fivetran/orders,PROD)"
        )


class TestBuildDestinationUrnGcsAndAbsMdl:
    """GCS and ADLS Gen2 path-style URNs share the s3 branch in
    build_destination_urn — same `<prefix>/<schema>/<table>` shape, just a
    different platform_id and a different prefix-composition rule
    (storage_account/container/prefix for ABS vs bucket/prefix for GCS)."""

    def test_gcs_with_bucket_and_prefix(self):
        urn = FivetranSource.build_destination_urn(
            destination_table="sales.orders",
            destination_details=PlatformDetail(
                platform="gcs", database="example-gcs-lake/fivetran"
            ),
        )
        assert (
            str(urn)
            == "urn:li:dataset:(urn:li:dataPlatform:gcs,example-gcs-lake/fivetran/sales/orders,PROD)"
        )

    def test_abs_with_storage_account_container_and_prefix(self):
        # ABS prefix carries the storage account and container before the
        # prefix path: `<storage_account>/<container>/<prefix>/<schema>/<table>`.
        urn = FivetranSource.build_destination_urn(
            destination_table="sales.orders",
            destination_details=PlatformDetail(
                platform="abs", database="myadlsaccount/datalake/fivetran"
            ),
        )
        assert (
            str(urn)
            == "urn:li:dataset:(urn:li:dataPlatform:abs,myadlsaccount/datalake/fivetran/sales/orders,PROD)"
        )

    def test_gcs_without_database_raises(self):
        # Same failure mode as s3: caller catches ValueError and skips
        # this lineage edge with a structured warning.
        with pytest.raises(ValueError, match="path prefix"):
            FivetranSource.build_destination_urn(
                destination_table="sales.orders",
                destination_details=PlatformDetail(platform="gcs"),
            )

    def test_abs_without_database_raises(self):
        with pytest.raises(ValueError, match="path prefix"):
            FivetranSource.build_destination_urn(
                destination_table="sales.orders",
                destination_details=PlatformDetail(platform="abs"),
            )
