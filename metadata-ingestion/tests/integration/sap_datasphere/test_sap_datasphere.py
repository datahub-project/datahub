import json
from pathlib import Path
from typing import cast
from unittest import mock

import pytest
import requests_mock as rm_module
import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.testing import mce_helpers
from tests.test_helpers.state_helpers import (
    get_current_checkpoint_from_pipeline,
    validate_all_providers_have_committed_successfully,
)

FROZEN_TIME = "2024-01-15 12:00:00+00:00"
FIXTURES_DIR = Path(__file__).parent / "fixtures"
TENANT_URL = "https://test.eu10.hcs.cloud.sap"

pytestmark = pytest.mark.integration_batch_0


def _fixture(name: str) -> Path:
    return FIXTURES_DIR / name


def _install_mocks(m: rm_module.Mocker) -> None:
    m.get(
        f"{TENANT_URL}/api/v1/datasphere/consumption/catalog/spaces",
        text=_fixture("spaces.json").read_text(),
    )
    m.get(
        f"{TENANT_URL}/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        text=_fixture("assets_S1.json").read_text(),
    )
    m.get(
        f"{TENANT_URL}/api/v1/datasphere/consumption/catalog/spaces('S2')/assets",
        text=_fixture("assets_S2.json").read_text(),
    )
    m.get(
        f"{TENANT_URL}/edmx/S1/DIM_DAY/$metadata",
        text=_fixture("dim_day.xml").read_text(),
    )
    m.get(
        f"{TENANT_URL}/edmx/S1/FACT_SALES/$metadata",
        text=_fixture("fact_sales.xml").read_text(),
    )
    m.get(
        f"{TENANT_URL}/edmx/S2/AM_REVENUE/$metadata",
        text=_fixture("analytical_revenue.xml").read_text(),
    )
    m.get(
        f"{TENANT_URL}/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )
    m.get(
        f"{TENANT_URL}/api/v1/datasphere/spaces/S2/connections",
        json=[],
    )
    # S1 has two base tables, S2 none — exercises both the populated and empty
    # branches plus the per-table CSN fetch that backfills schemas for CLL.
    m.get(
        f"{TENANT_URL}/dwaas-core/api/v1/spaces/S1/localtables",
        json=[
            {"technicalName": "SAP.TIME.M_TIME_DIMENSION"},
            {"technicalName": "BASE_FACTS"},
        ],
    )
    m.get(
        f"{TENANT_URL}/dwaas-core/api/v1/spaces/S2/localtables",
        json=[],
    )
    # include_view_definitions defaults True, so each asset's CSN is fetched; the
    # `query` dict drives viewProperties emission (elements-only tables get none).
    m.get(
        f"{TENANT_URL}/dwaas-core/api/v1/spaces/S1/views/DIM_DAY",
        json={
            "definitions": {
                "DIM_DAY": {
                    "kind": "entity",
                    "query": {
                        "SELECT": {
                            "from": {"ref": ["BASE_FACTS"], "as": "B"},
                            "columns": [{"ref": ["DATE_SQL"]}, {"ref": ["MONTH"]}],
                            "distinct": False,
                        }
                    },
                }
            }
        },
    )
    m.get(
        f"{TENANT_URL}/dwaas-core/api/v1/spaces/S1/views/FACT_SALES",
        json={
            "definitions": {
                "FACT_SALES": {
                    "kind": "entity",
                    "query": {
                        "SELECT": {
                            "from": {"ref": ["BASE_FACTS"], "as": "F"},
                            "columns": [{"ref": ["ID"]}, {"ref": ["AMOUNT"]}],
                            "distinct": False,
                        }
                    },
                }
            }
        },
    )
    m.get(
        f"{TENANT_URL}/dwaas-core/api/v1/spaces/S1/views/STAGING_RAW",
        json={
            "definitions": {
                "STAGING_RAW": {
                    "kind": "entity",
                    "query": {
                        "SELECT": {
                            "from": {"ref": ["BASE_FACTS"]},
                            "columns": [{"ref": ["ID"]}],
                        }
                    },
                }
            }
        },
    )
    m.get(
        f"{TENANT_URL}/dwaas-core/api/v1/spaces/S2/analyticmodels/AM_REVENUE",
        json={
            "definitions": {
                "AM_REVENUE": {
                    "kind": "entity",
                    "query": {
                        "SELECT": {
                            "from": {"ref": ["FACT_SALES"], "as": "S"},
                            "columns": [{"ref": ["AMOUNT"]}],
                            "distinct": False,
                        }
                    },
                }
            }
        },
    )
    # Per-table CSN: the parser maps cds.* literals to DataHub field types.
    m.get(
        f"{TENANT_URL}/dwaas-core/api/v1/spaces/S1/localtables/SAP.TIME.M_TIME_DIMENSION",
        json={
            "definitions": {
                "SAP.TIME.M_TIME_DIMENSION": {
                    "kind": "entity",
                    "@EndUserText.label": "Time Dimension",
                    "elements": {
                        "MONTH": {
                            "type": "cds.String",
                            "length": 2,
                            "@EndUserText.label": "Month",
                        },
                        "MONTH_INT": {
                            "type": "cds.hana.TINYINT",
                            "@EndUserText.label": "Month (Number)",
                        },
                        "DATE_SQL": {"type": "cds.Date"},
                    },
                }
            }
        },
    )
    m.get(
        f"{TENANT_URL}/dwaas-core/api/v1/spaces/S1/localtables/BASE_FACTS",
        json={
            "definitions": {
                "BASE_FACTS": {
                    "kind": "entity",
                    "elements": {
                        "ID": {"type": "cds.Integer"},
                        "AMOUNT": {
                            "type": "cds.Decimal",
                            "precision": 10,
                            "scale": 2,
                        },
                    },
                }
            }
        },
    )


@time_machine.travel(FROZEN_TIME, tick=False)
def test_sap_datasphere_end_to_end_golden_file(
    pytestconfig: pytest.Config,
    tmp_path: Path,
) -> None:
    output_file = tmp_path / "sap_datasphere_mces.json"
    golden_file = Path(__file__).parent / "sap_datasphere_mces_golden.json"

    pipeline_config = {
        "run_id": "sap-datasphere-test",
        "source": {
            "type": "sap-datasphere",
            "config": {
                "base_url": TENANT_URL,
                "token": "test-token",
                "env": "PROD",
                "platform_instance": "test_tenant",
                # Exercises the local-table emission path including the
                # per-table CSN fetch that backfills schemaMetadata aspects.
                "include_local_tables": True,
            },
        },
        "sink": {
            "type": "file",
            "config": {"filename": str(output_file)},
        },
    }

    with rm_module.Mocker() as m:
        _install_mocks(m)
        pipeline = Pipeline.create(pipeline_config)
        pipeline.run()
        pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=output_file,
        golden_path=golden_file,
    )


@time_machine.travel(FROZEN_TIME, tick=False)
def test_sap_datasphere_golden_file_has_required_aspects() -> None:
    golden_file = Path(__file__).parent / "sap_datasphere_mces_golden.json"
    assert golden_file.exists(), (
        "Golden file must exist (run --update-golden-files first)"
    )
    size_bytes = golden_file.stat().st_size
    assert size_bytes >= 5_000, (
        f"Golden file is {size_bytes} bytes; standards require ≥5KB"
    )

    events = json.loads(golden_file.read_text())
    assert len(events) >= 20, f"Got {len(events)} events; need ≥20"

    aspect_names = {e.get("aspectName") for e in events if "aspectName" in e}
    assert "schemaMetadata" in aspect_names, "Missing schemaMetadata aspect"
    assert "dataPlatformInstance" in aspect_names, "Missing dataPlatformInstance aspect"
    assert "containerProperties" in aspect_names, "Missing containerProperties aspect"
    assert "subTypes" in aspect_names, "Missing subTypes aspect"
    assert "datasetProperties" in aspect_names, "Missing datasetProperties aspect"

    schema_events = [e for e in events if e.get("aspectName") == "schemaMetadata"]
    assert len(schema_events) >= 3, (
        f"Expected ≥3 schemaMetadata events (DIM_DAY, FACT_SALES, AM_REVENUE), "
        f"got {len(schema_events)}"
    )

    schema_event_text = json.dumps(events)
    assert "urn:li:tag:sap:calendar:date" in schema_event_text, (
        "Expected calendar annotation surfaced as a field tag"
    )
    assert "Analytic Model" in schema_event_text, (
        "Expected Analytic Model subtype from AM_REVENUE"
    )

    # Managed assets emit under `sap-datasphere` (the tenant's identity), not the
    # underlying HANA storage; containers (Spaces) too.
    dataset_events = [
        e for e in events if "urn:li:dataset:" in (e.get("entityUrn") or "")
    ]
    for e in dataset_events:
        urn = e["entityUrn"]
        assert "urn:li:dataPlatform:sap-datasphere" in urn, (
            f"Expected dataset URN under sap-datasphere platform, got: {urn}"
        )
    container_events = [
        e for e in events if "urn:li:container:" in (e.get("entityUrn") or "")
    ]
    assert len(container_events) > 0, "Expected at least one container event"
    container_dpi = [
        e for e in container_events if e.get("aspectName") == "dataPlatformInstance"
    ]
    for e in container_dpi:
        platform_urn = e.get("aspect", {}).get("json", {}).get("platform")
        assert "urn:li:dataPlatform:sap-datasphere" in (platform_urn or ""), (
            f"Container should reference sap-datasphere platform, got: {platform_urn}"
        )


def _lineage_csn_by_name() -> dict:
    """CSN bodies keyed by asset name, shaped like the per-object-type endpoint response."""
    return {
        "BASE_TABLE": {"definitions": {"BASE_TABLE": {"kind": "entity"}}},
        "MID_VIEW": {
            "definitions": {
                "MID_VIEW": {
                    "kind": "entity",
                    "query": {
                        "SELECT": {
                            "from": {"ref": ["BASE_TABLE"]},
                            "columns": [
                                {"ref": ["ID"]},
                                {"ref": ["NAME"]},
                                {
                                    "func": "SUM",
                                    "args": [{"ref": ["AMOUNT"]}],
                                    "as": "total_amount",
                                },
                            ],
                        }
                    },
                }
            }
        },
        "FED_SNOWFLAKE_CUST": {
            "definitions": {
                "FED_SNOWFLAKE_CUST": {
                    "kind": "entity",
                    "@remote.source": "SNOWFLAKE_PROD",
                    "@DataWarehouse.external.schema": "PUBLIC",
                }
            }
        },
        "FED_UNSUPPORTED": {
            "definitions": {
                "FED_UNSUPPORTED": {
                    "kind": "entity",
                    "@remote.source": "UNSUPPORTED_CONN",
                }
            }
        },
    }


def _lineage_analytic_model_csn() -> dict:
    """Analytic-model CSN whose businessLayerDefinitions star schema is authoritative for table-level lineage."""
    return {
        "definitions": {
            "AM_REVENUE": {
                "kind": "entity",
                "query": {
                    "SELECT": {
                        "from": {"ref": ["BASE_TABLE"], "as": "F"},
                        "columns": [{"ref": ["REVENUE"]}],
                        "distinct": False,
                    }
                },
            }
        },
        "businessLayerDefinitions": {
            "AM_REVENUE": {
                "sourceModel": {
                    "factSources": {
                        "F": {
                            "text": "Revenue facts",
                            "dataEntity": {"key": "LINEAGE_TEST.BASE_TABLE"},
                        }
                    },
                    "dimensionSources": {
                        "_REGION_DIM": {
                            "text": "Region dimension",
                            "dataEntity": {"key": "LINEAGE_TEST.MID_VIEW"},
                        }
                    },
                },
                "measures": {"REVENUE": {"isAuxiliary": False}},
                "attributes": {"REGION": {}},
                "variables": {"P_DATE": {}},
            }
        },
    }


def _install_lineage_mocks(m: rm_module.Mocker) -> None:
    """Fixtures for the federated-lineage scenario, shared by the golden and determinism tests."""
    fixtures_dir = Path(__file__).parent / "fixtures"
    m.get(
        f"{TENANT_URL}/api/v1/datasphere/consumption/catalog/spaces",
        text=(fixtures_dir / "spaces_lineage.json").read_text(),
    )
    m.get(
        f"{TENANT_URL}/api/v1/datasphere/consumption/catalog/spaces('LINEAGE_TEST')/assets",
        text=(fixtures_dir / "assets_lineage_test.json").read_text(),
    )
    m.get(
        f"{TENANT_URL}/api/v1/datasphere/spaces/LINEAGE_TEST/connections",
        text=(fixtures_dir / "connections_lineage.json").read_text(),
    )
    m.get(
        f"{TENANT_URL}/edmx/LINEAGE_TEST/BASE_TABLE/$metadata",
        text=(fixtures_dir / "base_table.xml").read_text(),
    )
    m.get(
        f"{TENANT_URL}/edmx/LINEAGE_TEST/MID_VIEW/$metadata",
        text=(fixtures_dir / "mid_view.xml").read_text(),
    )
    m.get(
        f"{TENANT_URL}/edmx/LINEAGE_TEST/AM_REVENUE/$metadata",
        text=(fixtures_dir / "am_revenue.xml").read_text(),
    )
    csn_by_name = _lineage_csn_by_name()
    for name, body in csn_by_name.items():
        # Non-analytical assets route to the /views/ sub-path.
        m.get(
            f"{TENANT_URL}/dwaas-core/api/v1/spaces/LINEAGE_TEST/views/{name}",
            json=body,
        )
    # AM_REVENUE (supportsAnalyticalQueries=true) routes to /analyticmodels/ and
    # carries a businessLayerDefinitions star schema alongside its query.
    m.get(
        f"{TENANT_URL}/dwaas-core/api/v1/spaces/LINEAGE_TEST/analyticmodels/AM_REVENUE",
        json=_lineage_analytic_model_csn(),
    )


def _lineage_pipeline_config(output_file: Path, max_workers_assets: int = 4) -> dict:
    """max_workers_assets defaults to 4; the determinism test passes 1 because LossyList.append is not thread-safe."""
    return {
        "run_id": "sap-datasphere-lineage-test",
        "source": {
            "type": "sap-datasphere",
            "config": {
                "base_url": TENANT_URL,
                "token": "test-token",
                "env": "PROD",
                "platform_instance": "test_tenant",
                "include_lineage": True,
                "max_workers_assets": max_workers_assets,
                # Managed assets always resolve to `sap-datasphere` with the
                # top-level platform_instance; this map only routes federated
                # (non-managed) connections.
                "connection_to_platform_map": {},
                "platform_type_defaults": {
                    "SNOWFLAKE": {
                        "platform": "snowflake",
                        "platform_instance": "prod_snowflake",
                        "convert_urns_to_lowercase": True,
                    },
                    # SALESFORCE intentionally unmapped, so FED_UNSUPPORTED lands
                    # in assets_skipped_unknown_typeid.
                },
            },
        },
        "sink": {"type": "file", "config": {"filename": str(output_file)}},
    }


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_sap_datasphere_lineage_federated_end_to_end_golden_file(
    pytestconfig: pytest.Config,
    tmp_path: Path,
) -> None:
    output_file = tmp_path / "sap_datasphere_lineage_federated_mces.json"
    golden_file = (
        Path(__file__).parent / "sap_datasphere_lineage_federated_mces_golden.json"
    )

    pipeline_config = _lineage_pipeline_config(output_file)

    with rm_module.Mocker() as m:
        _install_lineage_mocks(m)
        pipeline = Pipeline.create(pipeline_config)
        pipeline.run()
        pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=output_file,
        golden_path=golden_file,
    )


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_sap_datasphere_lineage_federated_is_deterministic_across_runs(
    tmp_path: Path,
) -> None:
    """Two runs must be byte-identical; pinned to max_workers_assets=1 because LossyList.append is not thread-safe."""
    output_1 = tmp_path / "run1.json"
    output_2 = tmp_path / "run2.json"

    with rm_module.Mocker() as m:
        _install_lineage_mocks(m)
        pipeline = Pipeline.create(
            _lineage_pipeline_config(output_1, max_workers_assets=1)
        )
        pipeline.run()
        pipeline.raise_from_status()

    with rm_module.Mocker() as m:
        _install_lineage_mocks(m)
        pipeline = Pipeline.create(
            _lineage_pipeline_config(output_2, max_workers_assets=1)
        )
        pipeline.run()
        pipeline.raise_from_status()

    assert output_1.read_text() == output_2.read_text(), (
        "Federated lineage scenario is non-deterministic across consecutive runs"
    )


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_sap_datasphere_lineage_federated_set_determinism_at_workers_4(
    pytestconfig: pytest.Config,
    tmp_path: Path,
) -> None:
    """At max_workers_assets=4, byte-equality isn't asserted (LossyList.append isn't thread-safe); the SET of emitted (entityUrn, aspectName) tuples must match."""
    output_1 = tmp_path / "run1.json"
    output_2 = tmp_path / "run2.json"

    for output in (output_1, output_2):
        pipeline_config = _lineage_pipeline_config(output, max_workers_assets=4)
        with rm_module.Mocker() as m:
            _install_lineage_mocks(m)
            pipeline = Pipeline.create(pipeline_config)
            pipeline.run()
            pipeline.raise_from_status()

    events_1 = json.loads(output_1.read_text())
    events_2 = json.loads(output_2.read_text())

    set_1 = {(e.get("entityUrn"), e.get("aspectName")) for e in events_1}
    set_2 = {(e.get("entityUrn"), e.get("aspectName")) for e in events_2}
    assert set_1 == set_2, (
        f"Workers=4 ingestion is not set-deterministic.\n"
        f"In run 1 but not run 2: {set_1 - set_2}\n"
        f"In run 2 but not run 1: {set_2 - set_1}"
    )
    assert len(events_1) == len(events_2), (
        f"Workers=4 event count differs: {len(events_1)} vs {len(events_2)}"
    )


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_sap_datasphere_lineage_federated_golden_has_expected_features() -> None:
    golden_file = (
        Path(__file__).parent / "sap_datasphere_lineage_federated_mces_golden.json"
    )
    assert golden_file.exists(), (
        "Golden file must exist (run --update-golden-files first)"
    )
    events = json.loads(golden_file.read_text())

    dataset_platforms = set()
    dataset_urns_all = set()
    for e in events:
        urn = e.get("entityUrn", "") or ""
        if "urn:li:dataset:" in urn:
            dataset_urns_all.add(urn)
            if "urn:li:dataPlatform:" in urn:
                plat = urn.split("urn:li:dataPlatform:")[1].split(",")[0]
                dataset_platforms.add(plat)

    assert "sap-datasphere" in dataset_platforms, (
        f"Expected at least one sap-datasphere dataset URN; "
        f"got platforms: {dataset_platforms}"
    )
    assert "snowflake" in dataset_platforms, (
        f"Expected at least one snowflake dataset URN (FED_SNOWFLAKE_CUST); "
        f"got platforms: {dataset_platforms}"
    )

    assert not any("FED_UNSUPPORTED" in u for u in dataset_urns_all), (
        f"FED_UNSUPPORTED should be skipped, but found dataset URN: "
        f"{[u for u in dataset_urns_all if 'FED_UNSUPPORTED' in u]}"
    )

    upstream_events = [e for e in events if e.get("aspectName") == "upstreamLineage"]
    assert len(upstream_events) >= 1, (
        f"Expected at least one upstreamLineage event; aspects: "
        f"{sorted({e.get('aspectName') for e in events if e.get('aspectName')})}"
    )
    mid_view_upstream = None
    for e in upstream_events:
        if "mid_view" in (e.get("entityUrn") or ""):
            mid_view_upstream = e
            break
    assert mid_view_upstream is not None, (
        "Expected an upstreamLineage event for MID_VIEW"
    )
    upstream_text = json.dumps(mid_view_upstream)
    assert "base_table" in upstream_text, (
        f"Expected MID_VIEW upstreamLineage to reference BASE_TABLE; got: {upstream_text}"
    )

    # Managed assets always inherit the source-level platform_instance ("test_tenant").
    assert any("test_tenant" in u for u in dataset_urns_all), (
        f"Expected managed datasets to use platform_instance test_tenant; got: {dataset_urns_all}"
    )

    # Snowflake URNs are lowercase because convert_urns_to_lowercase=True.
    snowflake_urns = [
        u for u in dataset_urns_all if "urn:li:dataPlatform:snowflake" in u
    ]
    for s in snowflake_urns:
        name_portion = s.split("urn:li:dataPlatform:snowflake,")[1].split(",")[0]
        assert name_portion == name_portion.lower(), (
            f"Expected lowercase Snowflake URN name; got: {name_portion}"
        )


GMS_PORT = 8080
GMS_SERVER = f"http://localhost:{GMS_PORT}"


def _stateful_pipeline_config(
    tmp_path: Path,
    run_id: str,
    sink_filename: str,
) -> dict:
    return {
        "pipeline_name": "sap-datasphere-stale-test",
        "run_id": run_id,
        "source": {
            "type": "sap-datasphere",
            "config": {
                "base_url": TENANT_URL,
                "token": "test-token",
                "env": "PROD",
                "platform_instance": "stale_test",
                "stateful_ingestion": {
                    "enabled": True,
                    "state_provider": {
                        "type": "datahub",
                        "config": {"datahub_api": {"server": GMS_SERVER}},
                    },
                },
                # Only ingest S1 to keep the test focused.
                "space_pattern": {"allow": ["^S1$"]},
            },
        },
        "sink": {
            "type": "file",
            "config": {"filename": str(tmp_path / sink_filename)},
        },
    }


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_sap_datasphere_stale_entity_removal(
    tmp_path: Path,
    mock_datahub_graph: mock.MagicMock,
) -> None:
    assets_s1_run1 = {
        "value": [
            {
                "name": "DIM_DAY",
                "label": "Day Dimension",
                "spaceName": "S1",
                "assetRelationalMetadataUrl": f"{TENANT_URL}/edmx/S1/DIM_DAY/$metadata",
                "supportsAnalyticalQueries": False,
                "hasParameters": False,
            },
            {
                "name": "FACT_SALES",
                "label": "Sales Fact Table",
                "spaceName": "S1",
                "assetRelationalMetadataUrl": f"{TENANT_URL}/edmx/S1/FACT_SALES/$metadata",
                "supportsAnalyticalQueries": False,
                "hasParameters": False,
            },
        ]
    }

    pipeline_run1 = None
    with (
        rm_module.Mocker() as m,
        mock.patch(
            "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
            mock_datahub_graph,
        ) as mock_checkpoint,
    ):
        mock_checkpoint.return_value = mock_datahub_graph
        m.get(
            f"{TENANT_URL}/api/v1/datasphere/consumption/catalog/spaces",
            text=_fixture("spaces.json").read_text(),
        )
        m.get(
            f"{TENANT_URL}/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
            json=assets_s1_run1,
        )
        m.get(
            f"{TENANT_URL}/edmx/S1/DIM_DAY/$metadata",
            text=_fixture("dim_day.xml").read_text(),
        )
        m.get(
            f"{TENANT_URL}/edmx/S1/FACT_SALES/$metadata",
            text=_fixture("fact_sales.xml").read_text(),
        )
        m.get(
            f"{TENANT_URL}/api/v1/datasphere/spaces/S1/connections",
            json=[],
        )
        # include_view_definitions defaults True, so CSN is fetched per view.
        for _view in ("DIM_DAY", "FACT_SALES"):
            m.get(
                f"{TENANT_URL}/dwaas-core/api/v1/spaces/S1/views/{_view}",
                json={
                    "definitions": {
                        _view: {
                            "kind": "entity",
                            "query": {"SELECT": {"from": {"ref": ["BASE_FACTS"]}}},
                        }
                    }
                },
            )

        pipeline_run1 = Pipeline.create(
            _stateful_pipeline_config(tmp_path, "run-1", "run1.json")
        )
        pipeline_run1.run()
        pipeline_run1.raise_from_status()

    checkpoint1 = get_current_checkpoint_from_pipeline(pipeline_run1)
    assert checkpoint1 is not None, "Run 1 must produce a checkpoint"
    assert checkpoint1.state is not None

    assets_s1_run2 = {
        "value": [
            {
                "name": "DIM_DAY",
                "label": "Day Dimension",
                "spaceName": "S1",
                "assetRelationalMetadataUrl": f"{TENANT_URL}/edmx/S1/DIM_DAY/$metadata",
                "supportsAnalyticalQueries": False,
                "hasParameters": False,
            },
        ]
    }

    pipeline_run2 = None
    with (
        rm_module.Mocker() as m,
        mock.patch(
            "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
            mock_datahub_graph,
        ) as mock_checkpoint,
    ):
        mock_checkpoint.return_value = mock_datahub_graph
        m.get(
            f"{TENANT_URL}/api/v1/datasphere/consumption/catalog/spaces",
            text=_fixture("spaces.json").read_text(),
        )
        m.get(
            f"{TENANT_URL}/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
            json=assets_s1_run2,
        )
        m.get(
            f"{TENANT_URL}/edmx/S1/DIM_DAY/$metadata",
            text=_fixture("dim_day.xml").read_text(),
        )
        m.get(
            f"{TENANT_URL}/api/v1/datasphere/spaces/S1/connections",
            json=[],
        )
        m.get(
            f"{TENANT_URL}/dwaas-core/api/v1/spaces/S1/views/DIM_DAY",
            json={
                "definitions": {
                    "DIM_DAY": {
                        "kind": "entity",
                        "query": {"SELECT": {"from": {"ref": ["BASE_FACTS"]}}},
                    }
                }
            },
        )

        pipeline_run2 = Pipeline.create(
            _stateful_pipeline_config(tmp_path, "run-2", "run2.json")
        )
        pipeline_run2.run()
        pipeline_run2.raise_from_status()

    checkpoint2 = get_current_checkpoint_from_pipeline(pipeline_run2)
    assert checkpoint2 is not None, "Run 2 must produce a checkpoint"
    assert checkpoint2.state is not None

    validate_all_providers_have_committed_successfully(
        pipeline=pipeline_run1, expected_providers=1
    )
    validate_all_providers_have_committed_successfully(
        pipeline=pipeline_run2, expected_providers=1
    )

    state1 = cast(GenericCheckpointState, checkpoint1.state)
    state2 = cast(GenericCheckpointState, checkpoint2.state)

    dropped_dataset_urns = list(
        state1.get_urns_not_in(type="dataset", other_checkpoint_state=state2)
    )
    assert len(dropped_dataset_urns) == 1, (
        f"Expected exactly 1 dropped dataset URN; got: {dropped_dataset_urns}"
    )
    assert "fact_sales" in dropped_dataset_urns[0], (
        f"Expected dropped URN to contain 'fact_sales'; got: {dropped_dataset_urns[0]}"
    )


def _install_federation_mocks(m: rm_module.Mocker) -> None:
    """Fixtures for the federation scenario: a data flow, a replication flow (ABAP -> BigQuery), and a HANA remote table with a SQL-quoted identifier to exercise quote-stripping."""
    m.get(
        f"{TENANT_URL}/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "FED_TEST", "label": "Federation Test Space"}]},
    )
    m.get(
        f"{TENANT_URL}/api/v1/datasphere/consumption/catalog/spaces('FED_TEST')/assets",
        json={"value": []},
    )
    m.get(
        f"{TENANT_URL}/api/v1/datasphere/spaces/FED_TEST/connections",
        json=[],
    )
    m.get(
        f"{TENANT_URL}/dwaas-core/api/v1/spaces/FED_TEST/dataflows",
        json=[{"technicalName": "DF_ENRICH_CUSTOMER"}],
    )
    m.get(
        f"{TENANT_URL}/dwaas-core/api/v1/spaces/FED_TEST/dataflows/DF_ENRICH_CUSTOMER",
        text=_fixture("data_flow_federated.json").read_text(),
    )
    m.get(
        f"{TENANT_URL}/dwaas-core/api/v1/spaces/FED_TEST/replicationflows",
        json=[{"technicalName": "RF_CUSTOMER_TO_BQ"}],
    )
    m.get(
        f"{TENANT_URL}/dwaas-core/api/v1/spaces/FED_TEST/replicationflows/RF_CUSTOMER_TO_BQ",
        text=_fixture("replication_flow_federated.json").read_text(),
    )
    m.get(
        f"{TENANT_URL}/dwaas-core/api/v1/spaces/FED_TEST/remotetables",
        json=[{"technicalName": "RT_BIC_MATERIAL"}],
    )
    m.get(
        f"{TENANT_URL}/dwaas-core/api/v1/spaces/FED_TEST/remotetables/RT_BIC_MATERIAL",
        text=_fixture("remote_table_hana_quoted.json").read_text(),
    )


def _federation_pipeline_config(output_file: Path) -> dict:
    return {
        "run_id": "sap-datasphere-federation-test",
        "source": {
            "type": "sap-datasphere",
            "config": {
                "base_url": TENANT_URL,
                "token": "test-token",
                "env": "PROD",
                "platform_instance": "test_tenant",
                "include_lineage": True,
                "include_data_flows": True,
                "include_replication_flows": True,
                "include_remote_tables": True,
                # Single-threaded keeps replication/remote-table emission ordered
                # and the golden byte-stable.
                "max_workers_assets": 1,
                "connection_to_platform_map": {
                    "ABAP_SRC": {"platform": "abap"},
                    # BigQuery API never returns the GCP project, so `database` is
                    # required for the target URN to stitch; casing must be preserved.
                    "BQ_TGT": {
                        "platform": "bigquery",
                        "database": "my-gcp-project",
                        "convert_urns_to_lowercase": False,
                    },
                    # HANA stores unquoted identifiers uppercase and the native HANA
                    # connector preserves that, so we must too or the URN won't match.
                    "HANA_DP_AGENT": {
                        "platform": "hana",
                        "convert_urns_to_lowercase": False,
                    },
                },
            },
        },
        "sink": {"type": "file", "config": {"filename": str(output_file)}},
    }


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_sap_datasphere_replication_flow_and_remote_table_golden_file(
    pytestconfig: pytest.Config,
    tmp_path: Path,
) -> None:
    output_file = tmp_path / "sap_datasphere_flow_remote_table_mces.json"
    golden_file = (
        Path(__file__).parent / "sap_datasphere_flow_remote_table_mces_golden.json"
    )

    with rm_module.Mocker() as m:
        _install_federation_mocks(m)
        pipeline = Pipeline.create(_federation_pipeline_config(output_file))
        pipeline.run()
        pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=output_file,
        golden_path=golden_file,
    )


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_sap_datasphere_federation_golden_has_expected_stitching() -> None:
    golden = Path(__file__).parent / "sap_datasphere_flow_remote_table_mces_golden.json"
    events = json.loads(golden.read_text())
    text = json.dumps(events)

    # Remote-table upstream must be unquoted SCHEMA.TABLE, never the raw quoted form.
    assert (
        "urn:li:dataset:(urn:li:dataPlatform:hana,MY_SCHEMA./BIC/MY_MATERIAL,PROD)"
        in text
    ), "Expected unquoted HANA upstream URN (quote-stripping fix)"
    assert '"MY_SCHEMA"."/BIC/MY_MATERIAL"' not in text, (
        "HANA upstream URN still carries SQL identifier quotes"
    )

    assert (
        "urn:li:dataPlatform:bigquery,my-gcp-project.staging.CUSTOMER,PROD" in text
    ), "Expected project-qualified, case-preserved BigQuery target URN"

    entity_types = {e.get("entityType") for e in events if e.get("entityType")}
    assert "dataJob" in entity_types, "Expected a DataJob for the replication flow"
    assert "dataFlow" in entity_types, "Expected a DataFlow for the replication flow"


@time_machine.travel(FROZEN_TIME, tick=False)
def test_sap_datasphere_lineage_federated_golden_has_column_lineage() -> None:
    golden = Path(__file__).parent / "sap_datasphere_lineage_federated_mces_golden.json"
    events = json.loads(golden.read_text())
    upstream_events = [e for e in events if e.get("aspectName") == "upstreamLineage"]
    assert upstream_events, "No upstreamLineage aspects in federated golden"
    fine_grained_events = [
        e
        for e in upstream_events
        if e.get("aspect", {}).get("json", {}).get("fineGrainedLineages")
    ]
    assert fine_grained_events, "No fineGrainedLineages in any upstreamLineage aspect"
    all_fg = [
        fg
        for e in fine_grained_events
        for fg in e["aspect"]["json"]["fineGrainedLineages"]
    ]
    assert any(fg.get("transformOperation") == "AGGREGATE" for fg in all_fg), (
        "Expected at least one AGGREGATE transformOperation in fineGrainedLineages"
    )
