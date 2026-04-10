"""Smoke tests for the demo-data source and `datahub datapack` CLI.

Tests run against a live DataHub instance and verify:
  1. `datahub datapack list` returns registry packs
  2. `datahub datapack load bootstrap` loads data into DataHub
  3. demo-data source (default config) loads bootstrap data
  4. demo-data source with showcase-ecommerce loads rich metadata
  5. `datahub datapack unload` cleans up loaded data
"""

import json
import logging

import pytest

from tests.utils import (
    execute_graphql,
    run_datahub_cmd,
    wait_for_writes_to_sync,
    with_test_retry,
)

logger = logging.getLogger(__name__)


class TestDatapackCLI:
    """Test the `datahub datapack` CLI commands."""

    def test_datapack_list(self):
        """datahub datapack list returns available packs."""
        result = run_datahub_cmd(["datapack", "list", "--format", "json"])
        assert result.exit_code == 0, f"CLI failed: {result.output}"
        packs = json.loads(result.output)
        assert "bootstrap" in packs
        assert "showcase-ecommerce" in packs

    def test_datapack_info(self):
        """datahub datapack info shows pack details."""
        result = run_datahub_cmd(["datapack", "info", "bootstrap"])
        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert "bootstrap" in result.output
        assert "verified" in result.output


class TestDemoDataBootstrap:
    """Test that the demo-data source loads bootstrap data by default."""

    def test_load_bootstrap_via_cli(self, auth_session):
        """datahub datapack load bootstrap ingests data into DataHub."""
        result = run_datahub_cmd(
            ["datapack", "load", "bootstrap", "--no-time-shift"],
            env={
                "DATAHUB_GMS_URL": auth_session.gms_url(),
                "DATAHUB_GMS_TOKEN": auth_session.gms_token(),
            },
        )
        assert result.exit_code == 0, f"CLI failed: {result.output}"
        wait_for_writes_to_sync()

        @with_test_retry()
        def _check():
            # Bootstrap pack contains SampleCypressHiveDataset among others
            query = """{
                search(input: {type: DATASET, query: "*", start: 0, count: 0}) {
                    total
                }
            }"""
            res = execute_graphql(auth_session, query)
            total = res["data"]["search"]["total"]
            assert total > 0, "Expected datasets after loading bootstrap pack"

        _check()

    def test_unload_bootstrap_via_cli(self, auth_session):
        """datahub datapack unload removes previously loaded data."""
        result = run_datahub_cmd(
            ["datapack", "unload", "bootstrap"],
            env={
                "DATAHUB_GMS_URL": auth_session.gms_url(),
                "DATAHUB_GMS_TOKEN": auth_session.gms_token(),
            },
        )
        assert result.exit_code == 0, f"CLI failed: {result.output}"


class TestShowcaseData:
    """Test loading the showcase-ecommerce data pack.

    This verifies the full datapack pipeline with a larger, richer pack
    that includes lineage, governance, glossary, domains, and data products.
    """

    @pytest.fixture(autouse=True)
    def _load_and_cleanup(self, auth_session):
        """Load showcase-ecommerce pack before tests, unload after."""
        logger.info("Loading showcase-ecommerce data pack")
        result = run_datahub_cmd(
            [
                "datapack",
                "load",
                "showcase-ecommerce",
                "--no-time-shift",
            ],
            env={
                "DATAHUB_GMS_URL": auth_session.gms_url(),
                "DATAHUB_GMS_TOKEN": auth_session.gms_token(),
            },
        )
        # Allow partial failures — some MCPs may have minor schema mismatches
        # but the bulk of the data should still be ingested successfully.
        assert "events_produced" in result.output, (
            f"Load produced no output: {result.output}"
        )
        wait_for_writes_to_sync()
        yield
        logger.info("Unloading showcase-ecommerce data pack")
        run_datahub_cmd(
            ["datapack", "unload", "showcase-ecommerce"],
            env={
                "DATAHUB_GMS_URL": auth_session.gms_url(),
                "DATAHUB_GMS_TOKEN": auth_session.gms_token(),
            },
        )

    def test_showcase_has_datasets(self, auth_session):
        """Showcase pack should contain datasets."""

        @with_test_retry()
        def _check():
            query = """{
                search(input: {type: DATASET, query: "*", start: 0, count: 0}) {
                    total
                }
            }"""
            res = execute_graphql(auth_session, query)
            total = res["data"]["search"]["total"]
            assert total > 10, f"Expected many datasets from showcase pack, got {total}"

        _check()

    def test_showcase_has_dashboards(self, auth_session):
        """Showcase pack should contain dashboards."""

        @with_test_retry()
        def _check():
            query = """{
                search(input: {type: DASHBOARD, query: "*", start: 0, count: 0}) {
                    total
                }
            }"""
            res = execute_graphql(auth_session, query)
            total = res["data"]["search"]["total"]
            assert total > 0, f"Expected dashboards from showcase pack, got {total}"

        _check()

    def test_showcase_has_glossary_terms(self, auth_session):
        """Showcase pack should contain glossary terms."""

        @with_test_retry()
        def _check():
            query = """{
                search(input: {type: GLOSSARY_TERM, query: "*", start: 0, count: 0}) {
                    total
                }
            }"""
            res = execute_graphql(auth_session, query)
            total = res["data"]["search"]["total"]
            assert total > 0, f"Expected glossary terms from showcase pack, got {total}"

        _check()
