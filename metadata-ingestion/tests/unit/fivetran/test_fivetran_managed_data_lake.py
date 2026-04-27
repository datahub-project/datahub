"""Unit tests for the Fivetran Managed Data Lake destination support.

Covers AWS Glue catalog backing the Managed Data Lake with log access
through a Snowflake catalog-linked database.
"""

from typing import Any

import pytest

from datahub.ingestion.source.fivetran.config import (
    ManagedDataLakeDestinationConfig,
    PlatformDetail,
)
from datahub.ingestion.source.fivetran.fivetran import FivetranSource


def _mdl_config(**overrides: Any) -> ManagedDataLakeDestinationConfig:
    """Build a minimal valid ManagedDataLakeDestinationConfig for testing."""
    base: dict = dict(
        account_id="abc12345.us-east-1",
        username="datahub",
        password="hunter2",
        warehouse="DATAHUB_WH",
        database="LH_SOURCE_FIVETRAN_USW2",
        log_schema="fivetran_metadata_test",
    )
    base.update(overrides)
    return ManagedDataLakeDestinationConfig(**base)


class TestManagedDataLakeConfigValidation:
    def test_glue_catalog_with_defaults_is_accepted(self):
        cfg = _mdl_config()
        assert cfg.catalog_type == "glue"
        assert cfg.glue_database_prefix == "fivetran_"
        # preserve_case defaults to True for MDL because CLDs are case-preserving.
        assert cfg.preserve_case is True

    @pytest.mark.parametrize(
        "unimplemented_catalog_type",
        ["iceberg_rest", "unity", "biglake", "onelake"],
    )
    def test_non_glue_catalog_rejected_at_config_load(self, unimplemented_catalog_type):
        # The Literal accepts the value for forward compatibility, but the
        # field validator rejects it at recipe-load time so the failure
        # surfaces before any ingestion work runs (rather than mid-loop in
        # URN construction). Match a stable substring rather than the full
        # error wording.
        with pytest.raises(ValueError, match="catalog_type"):
            _mdl_config(catalog_type=unimplemented_catalog_type)


class TestManagedDataLakeUrnConstruction:
    """`build_destination_urn` emits a Glue URN derived from the connector
    schema rather than a Snowflake URN pointing at the CLD. These tests pin
    the URN shape end-to-end."""

    def test_glue_urn_uses_prefix_and_schema(self):
        details = PlatformDetail(platform="managed_data_lake", env="PROD")
        urn = FivetranSource.build_destination_urn(
            destination_table="orders.line_items",
            destination_details=details,
            mdl_cfg=_mdl_config(),
        )
        assert (
            str(urn)
            == "urn:li:dataset:(urn:li:dataPlatform:glue,fivetran_orders.line_items,PROD)"
        )

    def test_glue_urn_honours_custom_prefix(self):
        # `glue_database_prefix` is the configurable knob future-proofing
        # against Fivetran renaming the auto-Glue-DB convention.
        details = PlatformDetail(platform="managed_data_lake", env="PROD")
        urn = FivetranSource.build_destination_urn(
            destination_table="orders.line_items",
            destination_details=details,
            mdl_cfg=_mdl_config(glue_database_prefix="ft_"),
        )
        assert (
            str(urn)
            == "urn:li:dataset:(urn:li:dataPlatform:glue,ft_orders.line_items,PROD)"
        )

    def test_unqualified_destination_table_raises(self):
        # If a table name reaches us without a "<schema>." prefix, the Glue
        # database name can't be derived. Better to fail loudly than to emit
        # a URN with an empty table component (e.g., `fivetran_orders.`).
        details = PlatformDetail(platform="managed_data_lake", env="PROD")
        with pytest.raises(ValueError, match="schema"):
            FivetranSource.build_destination_urn(
                destination_table="orders",  # no '.' separator
                destination_details=details,
                mdl_cfg=_mdl_config(),
            )

    def test_missing_mdl_cfg_raises(self):
        # The instance-method wrapper passes the MDL config through, but the
        # static method is reachable directly (e.g., from tests). Guard it
        # with a runtime ValueError rather than a stripped-under-`-O` assert.
        details = PlatformDetail(platform="managed_data_lake", env="PROD")
        with pytest.raises(ValueError, match="managed_data_lake_destination_config"):
            FivetranSource.build_destination_urn(
                destination_table="orders.line_items",
                destination_details=details,
                mdl_cfg=None,
            )

    def test_glue_urn_with_platform_instance(self):
        details = PlatformDetail(
            platform="managed_data_lake",
            env="DEV",
            platform_instance="lake_us_west",
        )
        urn = FivetranSource.build_destination_urn(
            destination_table="events.page_views",
            destination_details=details,
            mdl_cfg=_mdl_config(),
        )
        assert (
            str(urn)
            == "urn:li:dataset:(urn:li:dataPlatform:glue,lake_us_west.fivetran_events.page_views,DEV)"
        )

    def test_relational_branch_unchanged_for_snowflake_destination(self):
        # Regression guard: existing snowflake/bigquery/databricks recipes must
        # produce the same three-part URN shape as before.
        details = PlatformDetail(
            platform="snowflake",
            env="PROD",
            database="prod_warehouse",
        )
        urn = FivetranSource.build_destination_urn(
            destination_table="orders.line_items",
            destination_details=details,
            mdl_cfg=None,
        )
        assert (
            str(urn)
            == "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod_warehouse.orders.line_items,PROD)"
        )

    def test_relational_branch_strips_schema_when_disabled(self):
        details = PlatformDetail(
            platform="snowflake",
            env="PROD",
            database="prod_warehouse",
            include_schema_in_urn=False,
        )
        urn = FivetranSource.build_destination_urn(
            destination_table="orders.line_items",
            destination_details=details,
            mdl_cfg=None,
        )
        assert (
            str(urn)
            == "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod_warehouse.line_items,PROD)"
        )
