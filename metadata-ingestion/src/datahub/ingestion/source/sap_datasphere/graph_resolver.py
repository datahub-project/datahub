import logging
from typing import Dict, List, Optional, Tuple

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.sap_datasphere.report import SapDatasphereReport
from datahub.metadata.urns import DatasetUrn

logger: logging.Logger = logging.getLogger(__name__)

_DATASET_ENTITY_TYPE = "dataset"


class ExternalUrnGraphResolver:
    """SAP Datasphere's flow API reports source/target names in a *logical* form
    that need not match the physical warehouse table: the leaf is lower-cased,
    while BigQuery preserves source case, and a replication target can carry a
    source-added prefix (e.g. ``w01_cds_<table>``). Left unresolved those edges
    dangle. Matching is deliberately conservative — a candidate resolves only to
    a single unambiguous real table; anything ambiguous or absent is left to the
    caller's original candidate.
    """

    def __init__(self, graph: DataHubGraph, report: SapDatasphereReport) -> None:
        self._graph = graph
        self._report = report
        # (platform, platform_instance, env, parent_path_lower) ->
        #   {leaf_lower: [real full name, ...]}
        self._index_cache: Dict[
            Tuple[str, Optional[str], str, str], Dict[str, List[str]]
        ] = {}

    def resolve_name(
        self,
        platform: str,
        platform_instance: Optional[str],
        env: str,
        candidate_name: str,
    ) -> Optional[str]:
        # Need at least ``parent.leaf`` so the lookup can be scoped to the
        # candidate's dataset/schema; a bare single-segment name has nothing to
        # anchor on and is left untouched.
        parent_path, _, leaf = candidate_name.rpartition(".")
        if not parent_path or not leaf:
            return None
        index = self._index_for(platform, platform_instance, env, parent_path)
        if not index:
            return None

        leaf_lower = leaf.lower()
        exact = index.get(leaf_lower)
        if exact is not None:
            return exact[0] if len(exact) == 1 else None

        suffix = f"_{leaf_lower}"
        prefixed = [
            full
            for real_leaf, names in index.items()
            if real_leaf.endswith(suffix)
            for full in names
        ]
        return prefixed[0] if len(prefixed) == 1 else None

    def _index_for(
        self,
        platform: str,
        platform_instance: Optional[str],
        env: str,
        parent_path: str,
    ) -> Dict[str, List[str]]:
        key = (platform, platform_instance, env, parent_path.lower())
        cached = self._index_cache.get(key)
        if cached is not None:
            return cached

        index: Dict[str, List[str]] = {}
        prefix = f"{parent_path.lower()}."
        try:
            urns = self._graph.get_urns_by_filter(
                entity_types=[_DATASET_ENTITY_TYPE],
                platform=platform,
                platform_instance=platform_instance,
                env=env,
                # Scope the scroll to the candidate's dataset/schema; the local
                # prefix check below discards any loosely-matched noise.
                query=parent_path,
            )
        except Exception as e:
            # Best-effort enrichment: a graph query failure must not abort
            # ingestion, it just falls back to the unresolved candidate name.
            # Log at warning (not debug) and carry the exception type in the
            # report entry so the cause isn't invisible at normal log levels.
            logger.warning(
                f"Graph lookup for external lineage under {parent_path!r} "
                f"(platform={platform}) failed; leaving names unresolved: "
                f"{type(e).__name__}: {e}"
            )
            self._report.external_lineage_graph_lookup_failed.append(
                f"{parent_path}: {type(e).__name__}: {e}"
            )
            self._index_cache[key] = index
            return index

        for urn in urns:
            try:
                name = DatasetUrn.from_string(urn).name
            except Exception as e:
                # A single malformed URN from the graph must not sink the whole
                # index, nor be misreported as a graph-lookup failure; skip it but
                # keep it visible in the report so a systemic parse issue isn't
                # hidden at normal log levels.
                logger.debug(f"Skipping unparseable dataset URN {urn!r}: {e}")
                self._report.external_lineage_graph_urn_unparseable.append(
                    f"{urn}: {type(e).__name__}: {e}"
                )
                continue
            if not name.lower().startswith(prefix):
                continue
            real_leaf = name.rpartition(".")[2]
            index.setdefault(real_leaf.lower(), []).append(name)

        self._index_cache[key] = index
        return index
