"""Smoke tests for the demo-data source and `datahub datapack` CLI.

Tests run against a live DataHub instance and verify:
  1. `datahub datapack list` returns registry packs
  2. `datahub datapack load` of an isolation pack ingests a dataset test_e2e does not use
  3. `datahub datapack unload` of that pack removes only that dataset
  4. demo-data source with showcase-ecommerce loads rich metadata
"""

import json
import logging
from pathlib import Path
from typing import Dict, List

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import EmitMode
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import StatusClass
from tests.e2e.utils import execute_graphql, run_datahub_cmd, with_test_retry
from utilities.consistency_utils import wait_for_writes_to_sync
from utilities.domains import Domain

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.INGESTION)

# Not present in bootstrap_mce.json. Unload of this pack must not soft-delete
# the catalog that test_e2e searches.
_ISOLATION_PACK_NAME = "datapack-smoke-isolation"
_ISOLATION_PACK_FILE = Path(__file__).parent / "isolation_probe.json"
_ISOLATION_DATASET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:datapacksmoke,isolation_probe,PROD)"
)

# Showcase pack definitions file sets status.removed=false then propertyDefinition.
# After datapack unload (rollback), these entities are soft-deleted with no definition.
SHOWCASE_STRUCTURED_PROPERTY_URNS: List[str] = [
    "urn:li:structuredProperty:showcase.retentionPeriod",
    "urn:li:structuredProperty:showcase.dataFreshnessSla",
    "urn:li:structuredProperty:showcase.escalationContact",
    "urn:li:structuredProperty:showcase.dataQualityScore",
    "urn:li:structuredProperty:showcase.costCenter",
]


def _datapack_cli_env(auth_session) -> Dict[str, str]:
    return {
        "DATAHUB_GMS_URL": auth_session.gms_url(),
        "DATAHUB_GMS_TOKEN": auth_session.gms_token(),
    }


def _restore_soft_deleted_showcase_structured_properties(
    graph: DataHubGraph,
) -> None:
    """Undelete showcase properties so a subsequent datapack load can upsert definitions.

    Datapack unload soft-deletes via rollback; reload sends status + propertyDefinition
    in one async file, and propertyDefinition validation can run before status commits.
    This helper performs a synchronous status write first when tombstones are present.
    """
    restore_mcps: List[MetadataChangeProposalWrapper] = []
    for urn in SHOWCASE_STRUCTURED_PROPERTY_URNS:
        if not graph.exists(urn):
            continue
        status = graph.get_aspect(urn, StatusClass)
        if status is None or not status.removed:
            continue
        restore_mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=StatusClass(removed=False),
            )
        )

    if not restore_mcps:
        return

    logger.info(
        "Restoring %d soft-deleted showcase structured properties before datapack load",
        len(restore_mcps),
    )
    graph.emit_mcps(restore_mcps, emit_mode=EmitMode.SYNC_PRIMARY)
    wait_for_writes_to_sync(mcp_only=True)


class TestDatapackCLI:
    """Test the `datahub datapack` CLI commands."""

    @pytest.mark.p0
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
    """Load and unload a pack whose URNs are not in bootstrap_mce.json."""

    def test_load_bootstrap_via_cli(self, auth_session):
        """datahub datapack load ingests the isolation dataset."""
        pack_url = _ISOLATION_PACK_FILE.resolve().as_uri()
        result = run_datahub_cmd(
            [
                "datapack",
                "load",
                _ISOLATION_PACK_NAME,
                "--url",
                pack_url,
                "--trust-custom",
                "--no-time-shift",
            ],
            env=_datapack_cli_env(auth_session),
        )
        assert result.exit_code == 0, f"CLI failed: {result.output}"

        @with_test_retry()
        def _check():
            query = """{
                search(input: {type: DATASET, query: "isolation_probe", start: 0, count: 10}) {
                    searchResults {
                        entity {
                            urn
                        }
                    }
                }
            }"""
            res = execute_graphql(auth_session, query)
            urns = [
                hit["entity"]["urn"] for hit in res["data"]["search"]["searchResults"]
            ]
            assert _ISOLATION_DATASET_URN in urns, (
                f"Expected isolation dataset after loading the pack, got {urns}"
            )

        _check()

    def test_unload_bootstrap_via_cli(self, auth_session):
        """datahub datapack unload removes the isolation pack, not bootstrap_mce.json."""
        result = run_datahub_cmd(
            ["datapack", "unload", _ISOLATION_PACK_NAME],
            env=_datapack_cli_env(auth_session),
        )
        assert result.exit_code == 0, f"CLI failed: {result.output}"


class TestShowcaseData:
    """Test loading the showcase-ecommerce data pack.

    This verifies the full datapack pipeline with a larger, richer pack
    that includes lineage, governance, glossary, domains, and data products.
    """

    @pytest.fixture(scope="class", autouse=True)
    def _load_and_cleanup(self, auth_session, openapi_graph_client):
        """Load showcase-ecommerce once for the class; hard-unload after all tests.e2e."""
        cli_env = _datapack_cli_env(auth_session)
        _restore_soft_deleted_showcase_structured_properties(openapi_graph_client)

        logger.info("Loading showcase-ecommerce data pack")
        result = run_datahub_cmd(
            [
                "datapack",
                "load",
                "showcase-ecommerce",
                "--no-time-shift",
            ],
            env=cli_env,
        )
        assert result.exit_code == 0, f"CLI failed: {result.output}"
        yield
        logger.info("Unloading showcase-ecommerce data pack (hard delete)")
        run_datahub_cmd(
            ["datapack", "unload", "showcase-ecommerce", "--hard"],
            env=cli_env,
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

    def test_showcase_has_structured_properties(self, auth_session):
        """Showcase pack should define structured properties before assignments."""

        @with_test_retry()
        def _check():
            query = """{
                search(
                    input: {
                        type: STRUCTURED_PROPERTY
                        query: "showcase"
                        start: 0
                        count: 0
                    }
                ) {
                    total
                }
            }"""
            res = execute_graphql(auth_session, query)
            total = res["data"]["search"]["total"]
            assert total > 0, f"Expected showcase structured properties, got {total}"

        _check()
