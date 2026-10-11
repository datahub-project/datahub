from typing import Dict, List, Optional, cast

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.sap_datasphere.graph_resolver import (
    ExternalUrnGraphResolver,
)
from datahub.ingestion.source.sap_datasphere.report import SapDatasphereReport


class _FakeGraph:
    def __init__(self, urns: List[str]) -> None:
        self._urns = urns
        self.calls: List[Dict[str, object]] = []

    def get_urns_by_filter(self, **kwargs: object) -> List[str]:
        self.calls.append(kwargs)
        return list(self._urns)


class _BoomGraph:
    def get_urns_by_filter(self, **kwargs: object) -> List[str]:
        raise RuntimeError("boom")


def _bq(name: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:bigquery,{name},PROD)"


def _resolver(urns: List[str]) -> ExternalUrnGraphResolver:
    return ExternalUrnGraphResolver(
        cast(DataHubGraph, _FakeGraph(urns)), SapDatasphereReport()
    )


def _resolve(resolver: ExternalUrnGraphResolver, name: str) -> Optional[str]:
    return resolver.resolve_name("bigquery", None, "PROD", name)


def test_resolves_case_insensitive_leaf() -> None:
    # SAP reports the leaf lower-cased; the physical BigQuery table is uppercase.
    resolver = _resolver([_bq("proj.staging.ZC_FND_MDM_ZMBEW")])
    assert (
        _resolve(resolver, "proj.staging.zc_fnd_mdm_zmbew")
        == "proj.staging.ZC_FND_MDM_ZMBEW"
    )


def test_resolves_source_added_prefix() -> None:
    # Replication target physically carries a w01_cds_ prefix the flow name lacks.
    resolver = _resolver([_bq("proj.staging.w01_cds_zc_fnd_mdm_mara")])
    assert (
        _resolve(resolver, "proj.staging.zc_fnd_mdm_mara")
        == "proj.staging.w01_cds_zc_fnd_mdm_mara"
    )


def test_exact_leaf_wins_over_prefixed_candidate() -> None:
    resolver = _resolver(
        [
            _bq("proj.staging.ZC_FND_MDM_MARA"),
            _bq("proj.staging.w01_cds_zc_fnd_mdm_mara"),
        ]
    )
    assert (
        _resolve(resolver, "proj.staging.zc_fnd_mdm_mara")
        == "proj.staging.ZC_FND_MDM_MARA"
    )


def test_ambiguous_prefix_match_is_left_unresolved() -> None:
    resolver = _resolver([_bq("proj.staging.a_mara"), _bq("proj.staging.b_mara")])
    assert _resolve(resolver, "proj.staging.mara") is None


def test_no_match_returns_none() -> None:
    resolver = _resolver([_bq("proj.staging.other")])
    assert _resolve(resolver, "proj.staging.mara") is None


def test_match_scoped_to_same_dataset() -> None:
    # Same leaf in a different dataset must not stitch across datasets.
    resolver = _resolver([_bq("proj.other.zc_fnd_mdm_mara")])
    assert _resolve(resolver, "proj.staging.zc_fnd_mdm_mara") is None


def test_bare_name_without_parent_is_skipped() -> None:
    resolver = _resolver([_bq("proj.staging.x")])
    assert _resolve(resolver, "noparent") is None


def test_graph_lookup_failure_is_soft_and_reported() -> None:
    report = SapDatasphereReport()
    resolver = ExternalUrnGraphResolver(cast(DataHubGraph, _BoomGraph()), report)
    assert resolver.resolve_name("bigquery", None, "PROD", "proj.staging.x") is None
    # The report entry carries the exception type/message so the cause isn't buried at debug level.
    failures = list(report.external_lineage_graph_lookup_failed)
    assert len(failures) == 1
    assert failures[0].startswith("proj.staging")
    assert "RuntimeError" in failures[0]


def test_index_is_cached_per_parent_path() -> None:
    graph = _FakeGraph([_bq("proj.staging.X")])
    resolver = ExternalUrnGraphResolver(
        cast(DataHubGraph, graph), SapDatasphereReport()
    )
    resolver.resolve_name("bigquery", None, "PROD", "proj.staging.x")
    resolver.resolve_name("bigquery", None, "PROD", "proj.staging.y")
    # Same platform/instance/env/parent -> a single scoped fetch is reused.
    assert len(graph.calls) == 1
