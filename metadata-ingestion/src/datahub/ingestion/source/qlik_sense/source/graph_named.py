import logging
import re
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Iterable, List, Set

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.qlik_sense.constants import QVD_EXTENSION
from datahub.ingestion.source.qlik_sense.data_classes import ReloadStatement
from datahub.ingestion.source.qlik_sense.lib_path import (
    _is_ambiguous_lib_path,
    _path_has_label,
    _path_key,
    _path_space,
)
from datahub.ingestion.source.qlik_sense.reload_sql import _reload_files
from datahub.ingestion.source.qlik_sense.source.file_entities import FileEntitiesMixin
from datahub.ingestion.source.qlik_sense.source_models import (
    GraphNamedPath,
    _GraphNamedApps,
    _SpaceNeed,
)
from datahub.ingestion.source.qlik_sense.source_utils import _bounded_list, _qlik_app_id
from datahub.metadata.com.linkedin.pegasus2avro.dataset import DatasetLineageType

logger = logging.getLogger(__name__)


class GraphNamedMixin(FileEntitiesMixin):
    def _record_reload_files(
        self, app_id: str, statements: List[ReloadStatement]
    ) -> None:
        reload_files = _reload_files(statements)
        for discriminator in reload_files.unrecognised:
            self.reporter.unrecognised_reload_files.append(f"{app_id}: {discriminator}")
        self._reload_files[app_id] = reload_files.files

    def _read_reload_files(self, app_ids: Set[str]) -> None:
        """Reads the files of the apps' last reload that were not read with the
        apps, in parallel, as the run would otherwise wait on each in turn."""
        unread = sorted(app_ids - self._reload_files.keys())
        if not unread:
            return
        with ThreadPoolExecutor(
            max_workers=self.config.resolved_max_workers()
        ) as executor:
            futures = {
                app_id: executor.submit(self.qlik_api.get_reload_statements, app_id)
                for app_id in unread
            }
        for app_id, future in futures.items():
            try:
                statements = future.result()
            except Exception as e:
                logger.debug(f"Unable to read reload statements of app {app_id}: {e}")
                self.reporter.apps_without_reload_paths.append(f"{app_id}: {e}")
                self._reload_path_failures.append(f"{app_id}: {e}")
                self._reload_files_unread.add(app_id)
                statements = []
            self._record_reload_files(app_id, statements)

    def _graph_named_apps(self, urn: str, kept_readers: Set[str]) -> _GraphNamedApps:
        """The apps whose last reload may give the path of a dataset only the
        upstream graph names: those that load it, the writer apps this run
        ingests, apps kept from a last read that held it, and those that keep
        it from their last read as this run could not tell their sources."""
        keeping = {
            app_id
            for app_id in self._apps_graph_named_kept
            if urn in self._app_end_of_run_urns.get(app_id, ())
            or urn in self._app_read_graph_named.get(app_id, ())
        }
        return _GraphNamedApps(
            apps=set(self._graph_resource_readers.get(urn, set()))
            | keeping
            | kept_readers
            | {
                app_id
                for qri in self._graph_resource_writers.get(urn, set())
                if qri in self._app_table_urns_by_qri
                and (app_id := _qlik_app_id(qri)) is not None
            },
            graph_unread=bool(keeping),
        )

    def _graph_named_path(self, label: str, apps: _GraphNamedApps) -> GraphNamedPath:
        """The `lib://` path of a dataset only the upstream graph names, such as
        `lib://<space>:DataFiles/orders.qvd` for a QVD in a space the API key
        can't see, and whether the apps that may give it were all read. Qlik's
        label is only a name, so a path is given only when every one of those
        reloads was read and exactly one file they read or stored is at it."""
        # By key, as the same file is recorded in several spellings.
        paths: Dict[str, str] = {}
        for app_id in sorted(apps.apps):
            for file in self._reload_files.get(app_id, []):
                if _path_has_label(file.path, label):
                    paths.setdefault(_path_key(file.path), file.path)
        reloads_unread = bool(apps.apps & self._reload_files_unread)
        if len(paths) == 1 and not reloads_unread:
            return GraphNamedPath(next(iter(paths.values())), True)
        if len(paths) != 1:
            for path in paths.values():
                self._note_unseen_space(_path_space(path), _SpaceNeed.DATASETS)
        # An unread reload may hold another file with the label.
        return GraphNamedPath(None, not apps.graph_unread and not reloads_unread)

    def _leave_out_unplaced(self, urn: str, label: str, all_read: bool) -> None:
        """A dataset the graph names but whose path is not certain is not
        emitted, so the tables reading it link to its URN alone. Datasets an
        earlier run emitted are then soft-deleted, unless a reload that may give
        the path, or the graph of an app that keeps it, could not be read."""
        if not all_read:
            self._keep_in_stale_state(urn)
            return
        self.reporter.graph_named_datasets_without_path.append(f"{label}: {urn}")
        for urns in self._app_end_of_run_urns.values():
            urns.discard(urn)

    def _read_graph_named_reloads(self) -> Dict[str, _GraphNamedApps]:
        """The apps that may give the path of each dataset only the upstream
        graphs name, with their last reload read."""
        graph_named = set(self._graph_named_resources)
        if not graph_named:
            return {}
        # A kept app still loads what its last read did, so its reload counts
        # toward whether a path is certain. Its checkpoint is looked up here
        # rather than every app's entities held for the run. A dataset left out
        # last run is in no app's `urns`, so its recorded reads count too.
        kept_readers: Dict[str, Set[str]] = defaultdict(set)
        for app_id in self._kept_app_ids:
            previous = self.app_state.previous(app_id)
            if previous is None:
                continue
            read = {*previous.urns, *previous.read_graph_named_urns}
            for urn in graph_named.intersection(read):
                kept_readers[urn].add(app_id)
        apps_by_urn = {
            urn: self._graph_named_apps(urn, kept_readers.get(urn, set()))
            for urn in self._graph_named_resources
        }
        self._read_reload_files(
            {app_id for apps in apps_by_urn.values() for app_id in apps.apps}
        )
        return apps_by_urn

    def _gen_graph_named_resources(
        self, apps_by_urn: Dict[str, _GraphNamedApps]
    ) -> Iterable[MetadataWorkUnit]:
        """The datasets and files only apps' upstream graphs name whose path is
        certain, and the tables of the writer apps this run ingests as their
        upstreams."""
        emitted: Set[str] = set()
        for urn, label in self._graph_named_resources.items():
            path, all_read = self._graph_named_path(label, apps_by_urn[urn])
            if path is None:
                self._leave_out_unplaced(urn, label, all_read)
                continue
            path = self._script_spelling(path)
            if _is_ambiguous_lib_path(path):
                # Its space can't be told from its connection.
                self._leave_out_unplaced(urn, label, all_read)
                continue
            emitted.add(urn)
            self._note_unseen_space(_path_space(path), _SpaceNeed.DATASETS)
            yield from self._gen_file_entity(
                urn,
                # The label holds the folders under the connection, which the
                # browse path already gives, so the name is the file's alone.
                re.split(r"[/\\]", label)[-1],
                DatasetSubTypes.QLIK_QVD
                if label.lower().endswith(QVD_EXTENSION)
                else DatasetSubTypes.QLIK_DATASET,
                path,
            )
            yield from self._gen_lib_path_location(urn, path)
        for urn, writer_qris in self._graph_resource_writers.items():
            if urn not in emitted:
                # Its writers' edges would make the dataset exist.
                continue
            # Only writer apps this run ingests: another app's table has no URN.
            writer_urns = {
                writer
                for qri in writer_qris
                if (writer := self._app_table_urns_by_qri.get(qri)) is not None
                and writer != urn
            }
            yield from self._patch_upstreams(
                urn, sorted(writer_urns), DatasetLineageType.COPY
            )
        if self.reporter.graph_named_datasets_without_path:
            self.reporter.info(
                title="Qlik datasets without a certain path are left out",
                message="Qlik's lineage graph names these datasets, but the "
                "reloads of the apps that read or store them give no single path "
                "for them, such as a file name read through two connections, so "
                "they are not ingested, and the tables reading them link to their "
                "URN alone. Give the API key's user access to the spaces listed "
                'under "Qlik spaces the API key can\'t see", or, for files on '
                "cloud storage, map their connection in `file_connection_to_storage`.",
                context=_bounded_list(
                    sorted(self.reporter.graph_named_datasets_without_path)
                ),
            )
        if self._reload_path_failures:
            self.reporter.warning(
                title="Unable to read the file paths of Qlik apps' last reload",
                message="The files these apps read or stored in their last reload "
                "could not be read, so datasets only Qlik's lineage graph names "
                "for them get no path this run and are kept as the last run left "
                "them. They are read again on the next run.",
                context=_bounded_list(self._reload_path_failures),
            )
        if self.reporter.unrecognised_reload_files:
            self.reporter.info(
                title="Unrecognised file statements in Qlik apps' last reload",
                message="These statements of the apps' last reload name a "
                "`lib://` path in a form the connector doesn't read, so the file "
                "gives no path to datasets only Qlik's lineage graph names.",
                context=_bounded_list(list(self.reporter.unrecognised_reload_files)),
            )
