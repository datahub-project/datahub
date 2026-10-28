"""Small Tableau helpers: connection-type to platform mapping, upstream table
reference parsing, credential handling in TableauConfig, the user-role
capability check and nameless-field handling in schema construction.
"""

from typing import Dict, List
from unittest import mock

import pytest
from pydantic import ValidationError
from tableauserverclient import PersonalAccessTokenAuth, TableauAuth
from tableauserverclient.models import UserItem
from tableauserverclient.server.endpoint.exceptions import ServerResponseError

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.tableau import tableau_constant as c
from datahub.ingestion.source.tableau.tableau import (
    SiteIdContentUrl,
    TableauConfig,
    TableauSiteSource,
    TableauSourceReport,
)
from datahub.ingestion.source.tableau.tableau_common import (
    TableauUpstreamReference,
    get_filter_pages,
    get_platform,
)
from datahub.ingestion.source.tableau.tableau_server_wrapper import UserInfo
from datahub.ingestion.source.tableau.tableau_validation import check_user_role
from tests.unit.tableau.test_tableau_config import default_config

PAT_CONFIG: Dict[str, object] = {
    "connect_uri": default_config["connect_uri"],
    "site": "acryl",
    "token_name": "ingestion",
    "token_value": "secret",
}


@pytest.mark.parametrize(
    ("connection_type", "platform"),
    [
        ("excel", "external"),
        ("hortonworkshadoophive", "hive"),
        ("mysql_odbc", "mysql"),
        ("webdata-direct:oracle-eloqua", "oracle"),
        ("tbio", "teradata"),
        ("sqlserver", "mssql"),
        ("googlebigquery", "bigquery"),
        ("trino_jdbc", "trino"),
        ("postgres", "postgres"),
    ],
)
def test_tableau_connection_types_map_to_datahub_platforms(
    connection_type: str, platform: str
) -> None:
    assert get_platform(connection_type) == platform


def test_upstream_reference_takes_schema_and_table_from_a_two_part_full_name() -> None:
    ref = TableauUpstreamReference.create(
        {
            c.ID: "t-1",
            c.NAME: "orders",
            c.SCHEMA: None,
            c.FULL_NAME: "[public].[orders]",
            c.DATABASE: {c.NAME: "dvdrental", c.ID: "db-1"},
            c.CONNECTION_TYPE: "postgres",
        }
    )

    assert (ref.database, ref.schema, ref.table) == ("dvdrental", "public", "orders")
    assert ref.database_id == "db-1"


def test_upstream_reference_fills_a_missing_schema_from_the_default_schema_map() -> (
    None
):
    ref = TableauUpstreamReference.create(
        {
            c.ID: "t-1",
            c.NAME: "orders",
            c.SCHEMA: None,
            c.FULL_NAME: None,
            c.DATABASE: {c.NAME: "dvdrental"},
            c.CONNECTION_TYPE: "postgres",
        },
        default_schema_map={"dvdrental": "public"},
    )

    assert (ref.database, ref.schema, ref.table) == ("dvdrental", "public", "orders")


def test_upstream_reference_drops_a_schema_already_embedded_in_the_table_name() -> None:
    # Redshift reports the table as "schema.table" with the schema repeated.
    ref = TableauUpstreamReference.create(
        {
            c.ID: "t-1",
            c.NAME: "public.orders",
            c.SCHEMA: "public",
            c.FULL_NAME: "public.orders",
            c.DATABASE: {c.NAME: "dev"},
            c.CONNECTION_TYPE: "redshift",
        }
    )

    assert (ref.database, ref.schema, ref.table) == ("dev", "", "orders")


def test_filter_pages_only_split_single_key_id_filters() -> None:
    mixed = {c.ID_WITH_IN: ["a", "b", "c"], c.PROJECT_NAME_WITH_IN: ["p"]}

    assert get_filter_pages(mixed, page_size=2) == [mixed]
    assert get_filter_pages({c.ID_WITH_IN: ["a", "b", "c"]}, page_size=2) == [
        {c.ID_WITH_IN: ["a", "b"]},
        {c.ID_WITH_IN: ["c"]},
    ]


def test_personal_access_token_is_used_when_no_password_is_configured() -> None:
    config = TableauConfig.model_validate(PAT_CONFIG)

    assert isinstance(config.get_tableau_auth("acryl"), PersonalAccessTokenAuth)
    assert isinstance(
        TableauConfig.model_validate(default_config).get_tableau_auth("acryl"),
        TableauAuth,
    )
    with pytest.raises(ConfigurationError):
        TableauConfig.model_validate(
            {"connect_uri": PAT_CONFIG["connect_uri"], "site": "acryl"}
        ).get_tableau_auth("acryl")


def test_sign_in_failures_surface_as_value_errors() -> None:
    config = TableauConfig.model_validate(PAT_CONFIG)
    with mock.patch("datahub.ingestion.source.tableau.tableau.Server") as server_cls:
        server_cls.return_value.auth.sign_in.side_effect = ServerResponseError(
            code="401002", summary="Unauthorized", detail="Invalid token"
        )
        with pytest.raises(ValueError, match="expire"):
            config.make_tableau_client("acryl")

        server_cls.return_value.auth.sign_in.side_effect = OSError("no route")
        with pytest.raises(ValueError):
            config.make_tableau_client("acryl")


def test_tags_for_hidden_assets_require_tag_ingestion() -> None:
    with pytest.raises(ValidationError, match="tags_for_hidden_assets"):
        TableauConfig.model_validate(
            {**default_config, "ingest_tags": False, "tags_for_hidden_assets": ["x"]}
        )


def test_user_role_check_reports_a_lookup_failure_as_not_capable() -> None:
    admin = UserInfo(user_name="u", site_role=c.ROLE_SITE_ADMIN_EXPLORER, site_id="s")
    assert check_user_role(admin)[c.SITE_PERMISSION].capable

    broken = mock.MagicMock(spec=UserInfo)
    broken.has_site_administrator_explorer_privileges.side_effect = RuntimeError(
        "role lookup failed"
    )
    report = check_user_role(broken)[c.SITE_PERMISSION]
    assert not report.capable
    assert report.mitigation_message is not None


def _make_source() -> TableauSiteSource:
    server = mock.MagicMock()
    server.user_id = "user-1"
    server.site_id = "site-1"
    server.users.get_by_id.return_value = UserItem(
        name="user", site_role=UserItem.Roles.SiteAdministratorExplorer
    )
    return TableauSiteSource(
        config=TableauConfig.model_validate(default_config),
        ctx=PipelineContext(run_id="test"),
        platform="tableau",
        site=SiteIdContentUrl(site_id="site-1", site_content_url="site-a"),
        report=TableauSourceReport(),
        server=server,
    )


def test_datasource_schema_skips_nameless_fields_and_queues_custom_sql() -> None:
    source = _make_source()
    fields: List[dict] = [
        {
            c.ID: "f-1",
            c.NAME: "amount",
            c.TYPE_NAME: c.COLUMN_FIELD,
            c.UPSTREAM_COLUMNS: [
                {
                    c.NAME: "amount",
                    c.TABLE: {c.ID: "csql-1", c.TYPE_NAME: c.CUSTOM_SQL_TABLE},
                },
                {
                    c.NAME: "amount",
                    c.TABLE: {c.ID: "csql-1", c.TYPE_NAME: c.CUSTOM_SQL_TABLE},
                },
                {c.NAME: "other", c.TABLE: {c.ID: "t-1", c.TYPE_NAME: "DatabaseTable"}},
            ],
        },
        {c.ID: "f-2", c.NAME: None, c.TYPE_NAME: c.COLUMN_FIELD},
    ]

    schema = source._get_schema_metadata_for_datasource(fields)

    assert schema is not None
    assert [f.fieldPath for f in schema.fields] == ["amount"]
    assert source.report.num_upstream_table_skipped_no_name == 1
    # The custom SQL backing the field is ingested once, later in the site run.
    assert source.custom_sql_ids_being_used == ["csql-1"]


def test_custom_sql_schema_skips_nameless_columns() -> None:
    source = _make_source()

    schema = source.get_schema_metadata_for_custom_sql(
        [
            {c.ID: "col-1", c.NAME: None},
            {c.ID: "col-2", c.NAME: "id", c.REMOTE_TYPE: "I8"},
        ]
    )

    assert schema is not None
    assert [f.fieldPath for f in schema.fields] == ["id"]
    assert source.report.num_csql_field_skipped_no_name == 1
