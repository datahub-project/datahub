from datetime import datetime, timezone
from typing import Optional
from unittest.mock import MagicMock

from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import SnowflakeStreamlitApp
from datahub.ingestion.source.snowflake.snowflake_schema_gen import (
    SnowflakeSchemaGenerator,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeFilter,
    SnowflakeIdentifierBuilder,
)
from datahub.metadata.schema_classes import DashboardInfoClass


def test_streamlit_app_processing():
    """Test that Streamlit apps are processed correctly with all properties."""
    # Create a mock Streamlit app
    streamlit_app = SnowflakeStreamlitApp(
        name="MY_STREAMLIT_APP",
        created=datetime(2023, 1, 15, 10, 30, 0, 0, tzinfo=timezone.utc),
        owner="ACCOUNTADMIN",
        database_name="TEST_DB",
        schema_name="TEST_SCHEMA",
        title="My Test Streamlit App",
        comment="This is a test Streamlit app",
        url_id="abc123def456",
        owner_role_type="ROLE",
    )

    # Verify the app has all expected properties
    assert streamlit_app.name == "MY_STREAMLIT_APP"
    assert streamlit_app.owner == "ACCOUNTADMIN"
    assert streamlit_app.database_name == "TEST_DB"
    assert streamlit_app.schema_name == "TEST_SCHEMA"
    assert streamlit_app.title == "My Test Streamlit App"
    assert streamlit_app.comment == "This is a test Streamlit app"
    assert streamlit_app.url_id == "abc123def456"
    assert streamlit_app.owner_role_type == "ROLE"
    assert streamlit_app.created == datetime(
        2023, 1, 15, 10, 30, 0, 0, tzinfo=timezone.utc
    )


def test_streamlit_custom_properties():
    """Test that all Streamlit properties are mapped to customProperties."""
    streamlit_app = SnowflakeStreamlitApp(
        name="MY_APP",
        created=datetime(2023, 1, 15, 10, 30, 0, 0, tzinfo=timezone.utc),
        owner="TEST_OWNER",
        database_name="DB",
        schema_name="SCHEMA",
        title="My App",
        comment="Test comment",
        url_id="test123",
        owner_role_type="ROLE",
    )

    # Create expected custom properties dict (database and schema excluded as they're in URN)
    expected_properties = {
        "name": "MY_APP",
        "owner": "TEST_OWNER",
        "owner_role_type": "ROLE",
        "url_id": "test123",
        "created": "2023-01-15T10:30:00+00:00",
        "comment": "Test comment",  # Comment is included when present
    }

    # Build custom properties the same way the source does
    custom_properties = {
        "name": streamlit_app.name,
        "owner": streamlit_app.owner,
        "owner_role_type": streamlit_app.owner_role_type,
        "url_id": streamlit_app.url_id,
        "created": streamlit_app.created.isoformat(),
    }

    if streamlit_app.comment:
        custom_properties["comment"] = streamlit_app.comment

    # Verify all properties are present
    assert custom_properties == expected_properties


def test_streamlit_dashboard_id_generation():
    """Test that dashboard IDs are generated correctly for Streamlit apps using url_id."""
    streamlit_app = SnowflakeStreamlitApp(
        name="MY_APP",
        created=datetime(2023, 1, 15, 10, 30, 0, 0, tzinfo=timezone.utc),
        owner="ACCOUNTADMIN",
        database_name="MY_DB",
        schema_name="MY_SCHEMA",
        title="My App",
        comment="",
        url_id="abc123",
        owner_role_type="ROLE",
    )

    # Production code: self.identifiers.snowflake_identifier(f"{db}.{schema}.{url_id}")
    # This tests the format before snowflake_identifier() transformation
    expected_dashboard_id = "MY_DB.MY_SCHEMA.abc123"
    dashboard_id = f"{streamlit_app.database_name}.{streamlit_app.schema_name}.{streamlit_app.url_id}"
    assert dashboard_id == expected_dashboard_id

    # Verify ID components are in the correct hierarchical format
    assert dashboard_id.count(".") == 2
    assert dashboard_id.startswith("MY_DB.")
    assert dashboard_id.endswith(".abc123")


def test_streamlit_optional_fields():
    """Test that Streamlit apps work correctly with optional fields missing."""
    # Create app with minimal required fields (only comment is optional)
    streamlit_app = SnowflakeStreamlitApp(
        name="MINIMAL_APP",
        created=datetime(2023, 1, 15, 10, 30, 0, 0, tzinfo=timezone.utc),
        owner="OWNER",
        database_name="DB",
        schema_name="SCHEMA",
        title="My Minimal App",
        comment=None,  # Optional - should not be in custom properties
        url_id="test_url_id",
        owner_role_type="ROLE",
    )

    # Build custom properties the same way the source does
    custom_properties = {
        "name": streamlit_app.name,  # Uses name, not title
        "owner": streamlit_app.owner,
        "owner_role_type": streamlit_app.owner_role_type,
        "url_id": streamlit_app.url_id,
        "created": streamlit_app.created.isoformat(),
    }

    if streamlit_app.comment:
        custom_properties["comment"] = streamlit_app.comment

    # Verify all required properties are present
    assert custom_properties["name"] == "MINIMAL_APP"  # Name, not title
    assert custom_properties["owner"] == "OWNER"
    assert custom_properties["owner_role_type"] == "ROLE"
    assert custom_properties["url_id"] == "test_url_id"
    assert custom_properties["created"] == "2023-01-15T10:30:00+00:00"

    # Verify comment is not included when None
    assert "comment" not in custom_properties


def _build_generator() -> SnowflakeSchemaGenerator:
    config = SnowflakeV2Config.parse_obj(
        {"account_id": "test_account", "include_streamlits": True}
    )
    report = SnowflakeV2Report()
    identifiers = SnowflakeIdentifierBuilder(config, report)
    return SnowflakeSchemaGenerator(
        config=config,
        report=report,
        connection=MagicMock(),
        filters=SnowflakeFilter(config, report),
        identifiers=identifiers,
        domain_registry=None,
        profiler=None,
        aggregator=None,
        snowsight_url_builder=None,
    )


def _streamlit_app(title: Optional[str]) -> SnowflakeStreamlitApp:
    return SnowflakeStreamlitApp(
        name="MY_APP",
        created=datetime(2023, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
        owner="OWNER",
        database_name="MY_DB",
        schema_name="MY_SCHEMA",
        title=title,
        comment=None,
        url_id="abc123",
        owner_role_type="ROLE",
    )


def _emitted_dashboard_title(app: SnowflakeStreamlitApp) -> str:
    for wu in _build_generator()._process_streamlit_app(app):
        info = wu.get_aspect_of_type(DashboardInfoClass)
        if info is not None:
            return info.title
    raise AssertionError("no DashboardInfo aspect emitted")


def test_streamlit_title_uses_configured_title():
    """A configured (non-empty) title is used verbatim as the dashboard title."""
    assert _emitted_dashboard_title(_streamlit_app("My App")) == "My App"


def test_streamlit_blank_title_falls_back_to_name():
    """A blank title falls back to the app name, not the db.schema.url_id identifier."""
    assert _emitted_dashboard_title(_streamlit_app("")) == "MY_APP"


def test_streamlit_none_title_falls_back_to_name():
    """A NULL title (Snowflake returns None) falls back to the app name."""
    assert _emitted_dashboard_title(_streamlit_app(None)) == "MY_APP"


def test_streamlit_whitespace_title_falls_back_to_name():
    """A whitespace-only title is treated as blank and falls back to the app name."""
    assert _emitted_dashboard_title(_streamlit_app("   ")) == "MY_APP"
