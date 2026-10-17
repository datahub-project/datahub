"""Shared helpers for the Iceberg probe filter-parity tests.

The unit test (fake and SQL catalogs) and the integration test (docker REST
catalog) assert the same thing: the tables `probe filter` reports included are
exactly the tables ingestion emits. Both sides of that comparison live here so
a parity fix cannot reach one test and miss the other.
"""

from typing import Dict, Set

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.probe_methods import run_probe_method
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.iceberg.iceberg import IcebergSource
from datahub.ingestion.source.iceberg.iceberg_common import IcebergSourceConfig
from datahub.utilities.urns.dataset_urn import DatasetUrn


def ingested_dataset_names(config_dict: Dict[str, object]) -> Set[str]:
    config = IcebergSourceConfig.model_validate(config_dict)
    source = IcebergSource(config, PipelineContext(run_id="iceberg-probe-parity"))
    names: Set[str] = set()
    for wu in source.get_workunits_internal():
        assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
        urn = wu.metadata.entityUrn
        if urn and urn.startswith("urn:li:dataset:"):
            names.add(DatasetUrn.from_string(urn).name)
    # A table ingestion failed on is missing from the set for a reason the
    # probe's filter verdict does not model.
    assert not source.report.failures
    return names


def probe_included_dataset_names(config_dict: Dict[str, object]) -> Set[str]:
    namespaces = run_probe_method("iceberg", config_dict, "namespaces", {}).result
    assert isinstance(namespaces, list)
    included: Set[str] = set()
    for namespace in namespaces:
        listing = run_probe_method(
            "iceberg", config_dict, "tables", {"namespace": namespace}
        )
        assert isinstance(listing.result, list)
        verdicts = check_filters(
            source_type="iceberg",
            config_dict=config_dict,
            kind=str(listing.kind),
            parent_path=listing.parent_path,
            names=listing.result,
        )
        # A degraded verdict (bare-name match, ignored parent) would agree
        # with ingestion here only by accident.
        assert not [w for w in verdicts.warnings if "bare name" in w]
        assert not [w for w in verdicts.warnings if "does not declare" in w]
        included |= {f"{namespace}.{r.name}" for r in verdicts.results if r.included}
    return included
