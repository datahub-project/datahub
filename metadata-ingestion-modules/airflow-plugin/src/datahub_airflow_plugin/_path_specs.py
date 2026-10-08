import logging
from typing import Dict, List, Optional, TypeVar

from wcmatch import pathlib

from datahub.api.entities.datajob import DataJob
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec
from datahub.metadata.schema_classes import FineGrainedLineageClass
from datahub.metadata.urns import DatasetUrn, SchemaFieldUrn
from datahub.utilities.urns.error import InvalidUrnError
from datahub_airflow_plugin._constants import FILE_PLATFORM

logger = logging.getLogger(__name__)

# DataHub platform -> the URI prefix its path specs are written with. Names on
# these platforms are what the OpenLineage adapter produces: `<bucket>/<key>` for
# object stores and an absolute path for `file`.
PLATFORM_URI_PREFIX: Dict[str, str] = {
    "s3": "s3://",
    "gcs": "gs://",
    FILE_PLATFORM: "/",
}

TABLE_MARKER = "{table}"

_T = TypeVar("_T")


def _platform_of(path_spec: PathSpec) -> str:
    for platform, prefix in PLATFORM_URI_PREFIX.items():
        if path_spec.include.startswith(prefix):
            return platform
    raise ValueError(
        f"path_specs include {path_spec.include!r} must start with one of "
        f"{sorted(PLATFORM_URI_PREFIX.values())}"
    )


def _globmatch(path: str, pattern: str) -> bool:
    return pathlib.PurePath(path).globmatch(pattern, flags=pathlib.GLOBSTAR)


def _table_dir(path_spec: PathSpec, uri: str) -> Optional[str]:
    """Return the `{table}` folder `uri` sits in, or None if the spec does not apply.

    OpenLineage references are often folders (a run or shard directory) rather
    than files, so `PathSpec.allowed` (file depth) and `dir_allowed` (no `**`)
    cannot be used as is. Instead the reference is cut at the `{table}` level
    and checked against the same glob, excludes and table filter the storage
    sources use.
    """
    depth = path_spec.include[: path_spec.include.find(TABLE_MARKER)].count("/") + 1
    parts = uri.rstrip("/").split("/")
    if len(parts) < depth:
        return None

    table_dir = "/".join(parts[:depth])
    if not _globmatch(table_dir, "/".join(path_spec.glob_include.split("/")[:depth])):
        return None
    if path_spec.is_path_hidden(table_dir) and not path_spec.include_hidden_folders:
        return None
    for exclude in path_spec.exclude or []:
        if _globmatch(uri.rstrip("/"), exclude) or _globmatch(
            table_dir, exclude.rstrip("/")
        ):
            return None

    table_name, _ = path_spec.extract_table_name_and_path(
        f"{table_dir}/{path_spec.get_remaining_glob_include(table_dir)}"
    )
    if not path_spec.tables_filter_pattern.allowed(table_name):
        return None
    return table_dir


class DatasetPathMapper:
    """Collapses file and run-folder datasets into their `{table}` folder."""

    def __init__(self, path_specs: List[PathSpec]) -> None:
        self._by_platform: Dict[str, List[PathSpec]] = {}
        for path_spec in path_specs:
            if TABLE_MARKER not in path_spec.include:
                raise ValueError(
                    f"path_specs include {path_spec.include!r} must contain "
                    f"{TABLE_MARKER} to mark the dataset folder"
                )
            self._by_platform.setdefault(_platform_of(path_spec), []).append(path_spec)

    def __bool__(self) -> bool:
        return bool(self._by_platform)

    def map_urn(self, urn: DatasetUrn) -> DatasetUrn:
        platform = urn.get_data_platform_urn().platform_name
        path_specs = self._by_platform.get(platform)
        if not path_specs:
            return urn

        prefix = PLATFORM_URI_PREFIX[platform]
        # `file` names are already absolute paths; object stores lack the scheme.
        uri = urn.name if platform == FILE_PLATFORM else f"{prefix}{urn.name}"
        for path_spec in path_specs:
            table_dir = _table_dir(path_spec, uri)
            if table_dir is not None:
                name = (
                    table_dir if platform == FILE_PLATFORM else table_dir[len(prefix) :]
                )
                return DatasetUrn(platform, name, urn.env)
        return urn

    def _map_field(self, urn: str) -> str:
        if not urn.startswith("urn:li:schemaField:"):
            return urn
        try:
            field = SchemaFieldUrn.from_string(urn)
            dataset = self.map_urn(DatasetUrn.from_string(field.parent))
        except InvalidUrnError:
            return urn
        return str(SchemaFieldUrn(str(dataset), field.field_path))

    def apply(self, datajob: DataJob) -> None:
        """Rewrite a DataJob's inlets, outlets and column lineage in place."""
        if not self:
            return

        datajob.inlets = _dedupe([self.map_urn(u) for u in datajob.inlets])
        datajob.outlets = _dedupe([self.map_urn(u) for u in datajob.outlets])

        fine_grained: List[FineGrainedLineageClass] = []
        for fgl in datajob.fine_grained_lineages:
            if fgl.upstreams:
                fgl.upstreams = _dedupe([self._map_field(u) for u in fgl.upstreams])
            if fgl.downstreams:
                fgl.downstreams = _dedupe([self._map_field(u) for u in fgl.downstreams])
            fine_grained.append(fgl)
        datajob.fine_grained_lineages = fine_grained


def _dedupe(items: List[_T]) -> List[_T]:
    # Many files of one table collapse to the same URN; keep the first, in order.
    return list(dict.fromkeys(items))
