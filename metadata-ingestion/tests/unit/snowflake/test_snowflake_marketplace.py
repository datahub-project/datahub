from datetime import datetime

from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_connection import SnowflakeConnection
from datahub.ingestion.source.snowflake.snowflake_marketplace import (
    SnowflakeMarketplaceHandler,
)
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows
        self.rowcount = len(rows)

    def execute(self, _query):
        return self

    def __iter__(self):
        return iter(self._rows)


class _FakeNativeConn:
    def __init__(self):
        self._last_query = None

    def cursor(self, _cursor_type):
        # Return a cursor object that yields rows depending on the last query set.
        return self

    def execute(self, query):
        self._last_query = query
        # Marketplace listings
        if "MARKETPLACE_LISTINGS" in query:
            rows = [
                {
                    "LISTING_GLOBAL_NAME": "ACME.DATA.LISTING",
                    "LISTING_DISPLAY_NAME": "Acme Data",
                    "PROVIDER_NAME": "Acme",
                    "CATEGORY": "Sample",
                    "DESCRIPTION": "Sample listing",
                    "LISTING_CREATED_ON": datetime(2024, 1, 1),
                    "LISTING_UPDATED_ON": datetime(2024, 6, 1),
                    "IS_PERSONAL_DATA": False,
                    "IS_FREE": True,
                }
            ]
            return _FakeCursor(rows)

        # Marketplace purchases
        if "MARKETPLACE_PURCHASES" in query:
            rows = [
                {
                    "LISTING_GLOBAL_NAME": "ACME.DATA.LISTING",
                    "LISTING_DISPLAY_NAME": "Acme Data",
                    "PROVIDER_NAME": "Acme",
                    "PURCHASE_DATE": datetime(2024, 7, 1),
                    "DATABASE_NAME": "DEMO_DATABASE",
                    "IS_AUTO_FULFILL": True,
                    "PURCHASE_STATUS": "ACTIVE",
                }
            ]
            return _FakeCursor(rows)

        # Default empty
        return _FakeCursor([])

    # For SnowflakeConnection.close path safety (not used here)
    def is_closed(self):
        return False

    def close(self):
        return None


def test_marketplace_creates_workunits_for_demo_database():
    # Build minimal config enabling listings and purchases
    config = SnowflakeV2Config.parse_obj(
        {
            "account_id": "test_account",
            "warehouse": "COMPUTE_WH",
            "role": "test_role",
            "username": "test_user",
            "include_marketplace_listings": True,
            "include_marketplace_purchases": True,
            "include_marketplace_usage": False,
        }
    )

    report = SnowflakeV2Report()
    identifiers = SnowflakeIdentifierBuilder(config, report)

    # Fake connection that yields marketplace rows
    fake_native = _FakeNativeConn()
    connection = SnowflakeConnection(fake_native)  # type: ignore[arg-type]

    handler = SnowflakeMarketplaceHandler(
        config=config,
        report=report,
        connection=connection,
        identifiers=identifiers,
    )

    wus = list(handler.get_marketplace_workunits())

    # Expect at least two workunits: one for data product, one for dataset enhancement
    assert len(wus) >= 2

    # Find the dataset enhancement for DEMO_DATABASE
    matched = [
        wu
        for wu in wus
        if hasattr(wu, "metadata")
        and hasattr(wu.metadata, "aspect")
        and getattr(wu.metadata, "entityUrn", "").find("demo_database") != -1
    ]
    assert matched, "Expected a DatasetProperties workunit for DEMO_DATABASE"
