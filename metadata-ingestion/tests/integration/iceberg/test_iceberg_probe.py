import subprocess
from pathlib import Path
from typing import Any, Dict, Iterator, List

import pytest
import yaml
from pyiceberg.catalog import Catalog

from datahub.ingestion.agent.probe_methods import ProbeMethodResult, run_probe_method
from datahub.ingestion.source.iceberg.iceberg_common import IcebergSourceConfig
from tests.test_helpers.docker_helpers import wait_for_port
from tests.test_helpers.iceberg_probe_helpers import (
    ingested_dataset_names,
    probe_included_dataset_names,
)

pytestmark = pytest.mark.integration_batch_5

_SEEDED: Dict[str, List[str]] = {
    "nyc": ["fares", "taxis", "taxis_tmp"],
    "ops": ["jobs"],
}
# Columns create.py writes, in schema order.
_SEEDED_COLUMNS = [
    "vendor_id",
    "trip_date",
    "trip_id",
    "trip_distance",
    "fare_amount",
    "store_and_fwd_flag",
]
# Keys pyiceberg uses for storage and catalog credentials. None may appear in
# any probe output: the REST catalog hands its FileIO these properties, and the
# recipe carries them inline. The recipe's own values for them are checked too
# (_recipe_secret_values), so a leak that drops the key name is still caught.
_CREDENTIAL_KEYS = [
    "access-key-id",
    "secret-access-key",
    "session-token",
    "s3.endpoint",
]


def _spark(args: str) -> None:
    ret = subprocess.run(
        f"docker exec spark-iceberg {args}", shell=True, capture_output=True
    )
    assert ret.returncode == 0, ret.stderr.decode(errors="replace")[-2000:]


@pytest.fixture(scope="module")
def iceberg_catalog(
    docker_compose_runner: Any, pytestconfig: pytest.Config
) -> Iterator[Dict[str, object]]:
    """The live REST catalog, seeded, and the integration recipe's source config."""
    test_resources_dir = pytestconfig.rootpath / "tests/integration/iceberg/"
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "iceberg"
    ) as docker_services:
        wait_for_port(docker_services, "spark-iceberg", 8888, timeout=120)

        for namespace, tables in _SEEDED.items():
            _spark(f'spark-sql -e "CREATE NAMESPACE IF NOT EXISTS {namespace}"')
            for table in tables:
                _spark(
                    f"spark-submit /home/iceberg/setup/create.py {namespace}.{table}"
                )

        recipe = yaml.safe_load(
            Path(test_resources_dir / "iceberg_to_file.yml").read_text()
        )
        yield recipe["source"]["config"]


def _recipe_secret_values(config_dict: Dict[str, object]) -> List[str]:
    catalogs = config_dict["catalog"]
    assert isinstance(catalogs, dict)
    values = [
        str(value)
        for properties in catalogs.values()
        for key, value in properties.items()
        # The endpoint is a key to keep out, not a secret value.
        if key != "s3.endpoint"
        and any(credential in key for credential in _CREDENTIAL_KEYS)
    ]
    # Pinned so the check below cannot pass on an empty list after a recipe edit.
    assert len(values) >= 2
    return values


def _assert_no_credentials(
    result: ProbeMethodResult, config_dict: Dict[str, object]
) -> None:
    assert result.failures == []
    rendered = repr(result.to_dict())
    for marker in [*_CREDENTIAL_KEYS, *_recipe_secret_values(config_dict)]:
        assert marker not in rendered, f"'{marker}' leaked from {result.command}"


def test_every_probe_command_reads_the_live_catalog(
    iceberg_catalog: Dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    opened: List[Catalog] = []
    closed: List[Catalog] = []
    original_get_catalog = IcebergSourceConfig.get_catalog

    def _tracking_get_catalog(self: IcebergSourceConfig) -> Catalog:
        catalog = original_get_catalog(self)
        original_close = catalog.close

        def _close() -> None:
            closed.append(catalog)
            original_close()

        monkeypatch.setattr(catalog, "close", _close)
        opened.append(catalog)
        return catalog

    monkeypatch.setattr(IcebergSourceConfig, "get_catalog", _tracking_get_catalog)

    def _run(command: str, **kwargs: object) -> ProbeMethodResult:
        result = run_probe_method(
            source_type="iceberg",
            config_dict=iceberg_catalog,
            command=command,
            kwargs=kwargs,
        )
        _assert_no_credentials(result, iceberg_catalog)
        return result

    namespaces = _run("namespaces")
    assert namespaces.kind == "Namespace"
    assert isinstance(namespaces.result, list)
    assert set(_SEEDED) <= set(namespaces.result)

    tables = _run("tables", namespace="nyc")
    assert tables.kind == "Table"
    assert tables.parent_path == ["nyc"]
    assert tables.result == _SEEDED["nyc"]

    columns = _run("columns", namespace="nyc", table="taxis")
    assert isinstance(columns.result, list)
    assert [column["name"] for column in columns.result] == _SEEDED_COLUMNS

    metadata = _run("table_metadata", namespace="nyc", table="taxis")
    assert isinstance(metadata.result, dict)
    assert metadata.result["current_snapshot_id"] is not None
    assert "trip_date" in str(metadata.result["partition_spec"])

    properties = _run("namespace_properties", namespace="nyc")
    assert isinstance(properties.result, dict)

    # Every command opened its own catalog through get_catalog, as ingestion
    # does, and closed it on the way out.
    assert len(opened) == 5
    assert closed == opened


_PATTERNS: Dict[str, object] = {
    "namespace_pattern": {"deny": ["^ops$"]},
    # Written against the qualified name, which is what ingestion matches.
    "table_pattern": {"allow": [r"^nyc\.taxis.*"], "deny": [r".*_tmp$"]},
}


def test_probe_filter_agrees_with_ingestion_on_the_live_catalog(
    iceberg_catalog: Dict[str, object],
) -> None:
    config_dict = {**iceberg_catalog, **_PATTERNS}

    ingested = ingested_dataset_names(config_dict)

    # nyc.fares fails table_pattern's allow, nyc.taxis_tmp its deny, and
    # ops.jobs sits in a denied namespace. Pinned so the parity below cannot
    # hold vacuously on an empty set.
    assert ingested == {"nyc.taxis"}
    assert probe_included_dataset_names(config_dict) == ingested
