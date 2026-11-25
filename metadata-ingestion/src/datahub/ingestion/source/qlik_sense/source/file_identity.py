import functools
from functools import partial
from typing import Callable, Dict, Optional, Set, Tuple

from datahub.ingestion.source.qlik_sense.config import Constant, FileStorageDetail
from datahub.ingestion.source.qlik_sense.data_classes import App
from datahub.ingestion.source.qlik_sense.file_lineage import (
    FILE_QRI_RE,
    PLATFORM_BY_QRI_SCHEME,
    FilePlatform,
    FileReference,
    FileSourceResolver,
    FileStorage,
    file_dataset_urn,
    file_storage_of_uri,
    normalized_file_path,
    qri_part_id,
)
from datahub.ingestion.source.qlik_sense.lib_path import (
    _is_data_files,
    _lib_connection,
    _lib_name,
)
from datahub.ingestion.source.qlik_sense.script_lineage import (
    LIB_PATH_RE,
    SPACE_SEPARATOR,
    ParsedScript,
    expand_variables,
    unquoted_values,
)
from datahub.ingestion.source.qlik_sense.source.dashboards import DashboardMixin
from datahub.ingestion.source.qlik_sense.source_models import _FileSourceResolution
from datahub.ingestion.source.qlik_sense.source_utils import _is_qvd


class FileIdentityMixin(DashboardMixin):
    """How this run names a file a load script reads or stores into. There is
    no single resolver; the current precedence is:

    1. A file Qlik catalogues as a dataset this run ingests is that Qlik
       dataset, and is not linked on storage.
    2. A file on a connection whose storage is known, from the connection's
       properties or `file_connection_to_storage`, is named on that storage.
    3. A file on a connection of unknown storage is named by its space,
       connection and path, and gets a dataset only when lineage links it.

    For a file in an app's lineage graph, a connection whose properties
    match the file's QRI wins over one whose storage is only mapped, and the
    file is named only when one URN remains."""

    @functools.cached_property
    def _dataset_urns_by_secure_qri(self) -> Optional[Dict[str, str]]:
        """Dataset URNs by secure QRI, or None when the datasets could not be
        listed. First read after the allowed spaces are known."""
        datasets = self.qlik_api.list_datasets()
        if datasets is None:
            self.reporter.warning(
                title="Unable to list Qlik datasets",
                message="Lineage to and from Qlik datasets, such as QVDs, is only "
                "added to this run.",
                context=str(self.qlik_api.dataset_listing_error),
            )
            return None
        self._listed_dataset_qris = {d.secure_qri for d in datasets if d.secure_qri}
        # Only datasets of allowed spaces are ingested.
        allowed = [d for d in datasets if d.space_id in self._allowed_space_ids]
        # Qlik traces QVDs, so one listed without a QRI may be a source the
        # graph names; files it doesn't trace, such as text files, never are.
        self._datasets_without_qri = any(
            d.secure_qri is None and _is_qvd(d) for d in allowed
        )
        self._excluded_datasets_without_qri = any(
            d.secure_qri is None and _is_qvd(d)
            for d in datasets
            if d.space_id not in self._allowed_space_ids
        )
        return {
            dataset.secure_qri: self._gen_qlik_dataset_urn(
                self._qlik_dataset_identifier(dataset.space_id, dataset.name)
            )
            for dataset in allowed
            if dataset.secure_qri is not None
        }

    def _file_source_resolver(
        self, app: App, parsed: ParsedScript
    ) -> FileSourceResolver:
        references = []
        unnamed = []
        settable = []
        for load in parsed.file_loads:
            if load.uses_unknown_variable:
                unnamed.append(load.as_written)
                continue
            # Paths are often built from variables the script sets with LET or
            # in an included script, which the app's variables hold. The graph
            # follows the last reload, so either spelling may be the file's.
            candidates = [
                load.expanded,
                expand_variables(
                    load.as_written, unquoted_values(self._app_variables(app))
                ).strip(),
            ]
            named = [
                FileReference(
                    connection=lib_path.group("connection"),
                    path=lib_path.group("path"),
                )
                for path in dict.fromkeys(candidates)
                if (lib_path := LIB_PATH_RE.match(path))
                and not any(part in path for part in ("$(", "*", "?"))
            ]
            if named:
                references.extend(named)
            if not named or any("*" in path or "?" in path for path in candidates):
                # A wildcard reads files the graph names one by one, which no
                # path can be matched against, and either spelling may be the
                # one the last reload read.
                unnamed.append(load.expanded)
                # Loop and branch variables stay unknown whatever the recipe
                # says, but a variable nothing sets can come from it.
                if "$(" in load.expanded:
                    settable.append(load.expanded)
        if unnamed and self._on_cloud:
            # On-prem apps have no lineage graph files to match.
            self._apps_file_paths_unknown.add(app.id)
            self.reporter.warning(
                title="Qlik app loads files the connector can't name",
                message="Files loaded through a wildcard, an unexpanded variable "
                "or a path outside a data connection can't be matched to the "
                "files in the app's lineage graph, so lineage to them is only "
                "added to."
                + (
                    " Set the variables of the paths listed as settable in "
                    "`script_variables` to name their files; no script sets "
                    "them."
                    if settable
                    else ""
                ),
                context=f"{app.id}: {', '.join(unnamed[:5])}"
                + (f"; settable: {', '.join(settable[:5])}" if settable else ""),
            )
        return FileSourceResolver(references)

    def _file_storage(
        self, connection: str, space_id: str
    ) -> Tuple[Optional[FileStorage], Optional[FileStorageDetail]]:
        detail = self._mapped_connection(
            connection, space_id, self.config.file_connection_to_storage
        )
        info = self._find_data_connection(connection, space_id)
        visible = info.file_storage if info else None
        if detail is not None and detail.storage_uri:
            storage = file_storage_of_uri(detail.storage_uri)
            if storage is not None and visible is not None:
                # The connection's own properties still check the QRI.
                storage.qri_connection_id = visible.qri_connection_id
            return storage, detail
        return visible, detail

    def _file_urn(
        self, storage: FileStorage, detail: Optional[FileStorageDetail], path: str
    ) -> str:
        return file_dataset_urn(
            storage,
            # As Qlik Cloud names it, so lineage joins whichever way it was read.
            normalized_file_path(path),
            platform_instance=detail.platform_instance if detail else None,
            # An env the mapping leaves unset is the recipe's.
            env=detail.env
            if detail and "env" in detail.model_fields_set
            else self.config.env,
            lowercase=detail.convert_urns_to_lowercase if detail else False,
        )

    def _unmapped_file_urn(
        self, connection: str, path: str, space_id: str
    ) -> Optional[str]:
        """A Qlik dataset for a file on a connection whose storage is unknown,
        named by the connection and path, so the apps that write and read it
        still join. DataFiles hold Qlik's own datasets."""
        lib_connection = _lib_connection(connection)
        space, name = lib_connection.space, lib_connection.name
        if not space:
            # Qlik Cloud resolves a bare name in the app's own space only. A
            # personal space has no name another app could write the path with.
            if self._on_cloud and space_id != Constant.PERSONAL_SPACE_ID:
                space = self.qlik_api.spaces.get(space_id, "").strip().strip("[]")
        path = normalized_file_path(path)
        qualified = f"{_lib_name(space)}{SPACE_SEPARATOR}{_lib_name(name)}/{path}"
        if (
            not (space and name and path)
            or _is_data_files(connection)
            # Characters a dataset URN can't hold.
            or any(char in qualified for char in ",()")
        ):
            return None
        urn = self._gen_qlik_dataset_urn(qualified.lower())
        self._add_unmapped_file(urn, f"lib://{qualified}")
        return urn

    def _add_unmapped_file(self, urn: str, spelling: str) -> None:
        # Apps may spell the file in different case and are read in no fixed
        # order, so one spelling is picked the same way every run.
        known = self._unmapped_files.get(urn)
        self._unmapped_files[urn] = spelling if known is None else min(known, spelling)

    def _unmapped_storage_file_urn(
        self, connection: str, path: str, space_id: str, report: bool = True
    ) -> Optional[str]:
        """Reports a connection whose storage is unknown and names the file on
        it by the connection and path, when it can be."""
        if self._data_connections_unread():
            # Its storage may be known once listed; the unread listing is reported.
            return None
        if report and connection not in self.reporter.unresolved_file_connections:
            self.reporter.unresolved_file_connections.append(connection)
        return self._unmapped_file_urn(connection, path, space_id)

    def _catalogued_as_qlik_dataset(
        self, storage: FileStorage, path: str
    ) -> Optional[bool]:
        """Whether Qlik catalogues the file as a dataset this run ingests, or
        None when the datasets could not be listed. Without the connection's
        own properties, a dataset of any connection holding the path counts."""
        datasets = self._dataset_urns_by_secure_qri
        if datasets is None:
            return None
        connections = self._catalogued_files.get(
            (storage.platform, qri_part_id(normalized_file_path(path))), set()
        )
        return (
            storage.qri_connection_id in connections
            if storage.qri_connection_id is not None
            else bool(connections)
        )

    @functools.cached_property
    def _catalogued_files(self) -> Dict[Tuple[FilePlatform, str], Set[str]]:
        """The connection parts of catalogued files' QRIs, by platform and the
        path part."""
        files: Dict[Tuple[FilePlatform, str], Set[str]] = {}
        for qri in self._dataset_urns_by_secure_qri or {}:
            match = FILE_QRI_RE.match(qri)
            platform = (
                PLATFORM_BY_QRI_SCHEME.get(match.group("scheme")) if match else None
            )
            if match and platform:
                files.setdefault((platform, match.group("path")), set()).add(
                    match.group("connection")
                )
        return files

    @functools.cached_property
    def _qvd_storage_urns(self) -> Dict[str, Callable[[str], str]]:
        return {
            name: partial(self._file_urn, storage, detail)
            for name, detail in self.config.file_connection_to_storage.items()
            if detail.storage_uri
            and (storage := file_storage_of_uri(detail.storage_uri))
        }

    def _file_source_resolution(
        self, app_id: str, file_qri: str
    ) -> _FileSourceResolution:
        """The dataset of a file in an app's lineage graph, such as a CSV on
        Azure Storage, when the app's load script reads it through a data
        connection whose storage is known."""
        resolver = self._file_sources_by_app.get(app_id)
        space_id = self._app_space_ids.get(app_id)
        if resolver is None or space_id is None:
            return _FileSourceResolution()
        candidates = resolver.candidates(file_qri)
        # A connection whose properties match the QRI's names the file for
        # certain; one whose storage is only mapped is trusted only when no
        # other connection the script reads the same path through could hold it.
        confirmed: Set[str] = set()
        unconfirmed: Set[str] = set()
        # Unconfirmed URNs whose storage location hashes to the QRI's part.
        likely: Set[str] = set()
        # Files on connections of unknown storage, named by connection and path.
        unmapped: Set[str] = set()
        unknown = False
        for reference in candidates.references:
            storage, detail = self._file_storage(reference.connection, space_id)
            if storage is None:
                info = self._find_data_connection(reference.connection, space_id)
                if _is_data_files(reference.connection) or (
                    info is not None and info.file_platform != candidates.platform
                ):
                    # DataFiles, listed or not, and a visible connection of
                    # another kind can't hold a file on this storage.
                    continue
                unmapped_urn = self._unmapped_storage_file_urn(
                    reference.connection,
                    reference.path,
                    space_id,
                    # A file the QRI places on another connector, such as
                    # SFTP, has no storage `file_connection_to_storage` maps.
                    report=candidates.platform is not None,
                )
                if unmapped_urn is None:
                    unknown = True
                else:
                    unmapped.add(unmapped_urn)
                continue
            if storage.platform != candidates.platform or (
                storage.qri_connection_id is not None
                and storage.qri_connection_id != candidates.connection_id
            ):
                continue
            urn = self._file_urn(storage, detail, reference.path)
            (confirmed if storage.qri_connection_id else unconfirmed).add(urn)
            if storage.likely_qri_connection_id == candidates.connection_id:
                likely.add(urn)
        unresolved = _FileSourceResolution(
            on_known_storage=bool(confirmed or unconfirmed)
        )
        if confirmed:
            urns = confirmed
        elif unknown:
            return unresolved
        elif unmapped:
            urns = unconfirmed | unmapped
        elif len(unconfirmed) > 1 and len(likely & unconfirmed) == 1:
            # Such as the prod and dev branches of an IF, of which only one ran.
            urns = likely & unconfirmed
        else:
            urns = unconfirmed
        if len(urns) != 1:
            return unresolved
        if file_qri not in self._resolved_file_qris:
            self._resolved_file_qris.add(file_qri)
            self.reporter.file_sources_resolved += 1
        urn = next(iter(urns))
        if urn in unmapped:
            self._link_unmapped_file(app_id, urn)
        return _FileSourceResolution(
            urn=urn, on_known_storage=unresolved.on_known_storage
        )

    def _link_unmapped_file(self, app_id: str, urn: str) -> None:
        self._linked_unmapped_files.add(urn)
        self._app_end_of_run_urns[app_id].add(urn)
        self._app_read_unmapped_files[app_id].add(urn)
