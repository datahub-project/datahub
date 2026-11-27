import posixpath
import re
from collections import Counter, defaultdict
from typing import Dict, Iterable, List, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import add_entity_to_container
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.data_lake_common.data_lake_utils import ContainerWUCreator
from datahub.ingestion.source.qlik_sense.config import Constant
from datahub.ingestion.source.qlik_sense.constants import QVD_EXTENSION
from datahub.ingestion.source.qlik_sense.dataset_naming import (
    file_display_name,
)
from datahub.ingestion.source.qlik_sense.lib_path import (
    LibPathSegments,
    build_lib_path,
    is_ambiguous_connection,
    lib_path_segments,
    parse_lib_connection,
    path_key,
    voted_segments,
)
from datahub.ingestion.source.qlik_sense.qvd_lineage import CANONICAL_ABSOLUTE_PATH_RE
from datahub.ingestion.source.qlik_sense.script_lineage import (
    LIB_PATH_RE,
)
from datahub.ingestion.source.qlik_sense.source.store_lineage import StoreLineageMixin
from datahub.metadata.com.linkedin.pegasus2avro.common import SubTypes
from datahub.metadata.com.linkedin.pegasus2avro.dataset import DatasetProperties
from datahub.metadata.schema_classes import BrowsePathEntryClass, BrowsePathsV2Class


class FileEntitiesMixin(StoreLineageMixin):
    def _gen_file_entities(
        self, paths: Dict[str, str], subtype: DatasetSubTypes
    ) -> Iterable[MetadataWorkUnit]:
        # Emitted by every app that touches the file, so it stays while any of
        # them does; a QVD's schema and lineage come from all of them at the end.
        # The folders too: each app's checkpoint only keeps what it emitted,
        # so they are deduplicated per app, not per run.
        containers = ContainerWUCreator(
            self.platform, self.config.platform_instance, self.config.env
        )
        for urn, path in paths.items():
            yield from self._gen_file_entity(
                urn, posixpath.basename(path), subtype, path
            )
            # In Folder containers from the server and share (or drive) down,
            # as the data lake sources place files; without a container the
            # browse path would be cut at the dots of the server name. A
            # relative path has no known root to start the folders from.
            if CANONICAL_ABSOLUTE_PATH_RE.match(path):
                yield from containers.create_container_hierarchy(path.lstrip("/"), urn)
            else:
                yield from self._gen_lib_path_location(urn, path)

    def _ingested_space_id(self, name: str) -> Optional[str]:
        # Qlik resolves `lib://` space names regardless of case, so a space is
        # matched that way, and only when no other space shares the name.
        ids = [
            space_id
            for space_id, space_name in self.qlik_api.spaces.items()
            if space_name.lower() == name.lower()
        ]
        if len(ids) != 1 or ids[0] not in self._allowed_space_ids:
            return None
        return ids[0]

    def _script_spelling(self, path: str) -> str:
        """A `lib://` path spelled as the load scripts and stores the run read
        spell it. Qlik ignores case in these paths and records loads in
        lowercase, so the same space, connection or folder would otherwise be
        named in several spellings."""
        spellings = self._lib_path_spellings()
        segments = lib_path_segments(path) or voted_segments(path, spellings)
        if segments is None:
            return path
        parts = segments.parts()
        spelled = []
        for i, segment in enumerate(parts):
            votes = spellings.get(path_key("/".join(parts[: i + 1])))
            # The most used spelling, and the same one every run on a tie.
            spelled.append(
                min(votes, key=lambda name: (-votes[name], name)) if votes else segment
            )
        return build_lib_path(
            LibPathSegments(space=spelled[0], connection=spelled[1], path=spelled[2:])
        )

    def _lib_path_spellings(self) -> Dict[str, Counter[str]]:
        paths = [
            *self._unmapped_files.values(),
            *(
                f"lib://{reference.connection}/{reference.path}"
                for resolver in self._file_sources_by_app.values()
                for reference in resolver.references()
            ),
            *(
                file.path
                for files in self._reload_files.values()
                for file in files
                if file.stored
            ),
        ]
        # Reload records are read through the end of the run, and a recorded
        # spelling can be replaced, so the spellings are built again when the
        # paths change.
        if paths != self._script_spellings_built_from:
            spellings: Dict[str, Counter[str]] = defaultdict(Counter)
            for path in paths:
                segments = lib_path_segments(path)
                parts = segments.parts() if segments is not None else []
                for i, segment in enumerate(parts):
                    spellings[path_key("/".join(parts[: i + 1]))][segment] += 1
            self._script_spellings = spellings
            self._script_spellings_built_from = paths
        return self._script_spellings

    def _gen_lib_path_location(self, urn: str, path: str) -> Iterable[MetadataWorkUnit]:
        """Places a file named by its `lib://<space>:<connection>/<path>` in its
        space, under its connection and folders, as Qlik shows it. A space the
        run doesn't ingest is named, not linked, so no container is made up for
        it. Without a browse path, DataHub splits the URN at its dots."""
        match = LIB_PATH_RE.match(self._script_spelling(path))
        if not match:
            return
        if is_ambiguous_connection(match.group("connection")):
            # Placed under no guessed space; the URN and path stay as recorded.
            return
        lib_connection = parse_lib_connection(match.group("connection"))
        space, connection = lib_connection.space, lib_connection.name
        if not space:
            return
        folders = [
            part for part in re.split(r"[/\\]", match.group("path"))[:-1] if part
        ]
        space_id = self._ingested_space_id(space)
        if space_id is not None:
            space_key = self._gen_space_key(space_id)
            yield from add_entity_to_container(
                container_key=space_key, entity_type="dataset", entity_urn=urn
            )
            space_urn = space_key.as_urn()
            entries = [BrowsePathEntryClass(id=space_urn, urn=space_urn)]
        else:
            entries = [BrowsePathEntryClass(id=space)]
        entries.extend(BrowsePathEntryClass(id=part) for part in [connection, *folders])
        yield MetadataChangeProposalWrapper(
            entityUrn=urn, aspect=BrowsePathsV2Class(path=entries)
        ).as_workunit()

    def _gen_file_entity(
        self,
        urn: str,
        name: str,
        subtype: DatasetSubTypes,
        path: Optional[str] = None,
    ) -> Iterable[MetadataWorkUnit]:
        yield self._gen_entity_status_aspect(urn)
        yield MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=DatasetProperties(
                name=file_display_name(name),
                qualifiedName=path,
                customProperties={
                    Constant.TYPE: subtype,
                    **({"path": path} if path else {}),
                },
            ),
        ).as_workunit()
        dpi_aspect = self._gen_dataplatform_instance_aspect(urn)
        if dpi_aspect:
            yield dpi_aspect
        yield MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=SubTypes(typeNames=[subtype]),
        ).as_workunit()

    def _gen_unmapped_file_entities(self) -> Iterable[MetadataWorkUnit]:
        for file_urn, kept_urns in sorted(self._kept_storage_file_stores.items()):
            yield from self._gen_stored_file_lineage(file_urn, kept_urns)
        # A reader whose last read is kept linked the file then. Its checkpoint
        # is looked up again rather than every app's reads held in memory. A
        # writer's entities hold the file too, so only recorded reads count.
        unread = set(self._unmapped_file_stores) - self._linked_unmapped_files
        for app_id in sorted(self._kept_app_ids) if unread else []:
            previous = self._load_checkpoint(app_id, self._checkpoint_load_failures)
            if previous is not None:
                self._linked_unmapped_files.update(
                    unread.intersection(previous.read_unmapped_file_urns)
                )
        for file_urn, stores in sorted(self._unmapped_file_stores.items()):
            if file_urn not in self._linked_unmapped_files:
                self.reporter.unread_stored_files.append(self._unmapped_files[file_urn])
                continue
            table_urns: List[str] = []
            for app_id, urns in stores.items():
                self._app_end_of_run_urns[app_id].add(file_urn)
                table_urns.extend(urn for urn in urns if urn not in table_urns)
            yield from self._gen_stored_file_lineage(file_urn, table_urns)
        paths = {urn: self._unmapped_files[urn] for urn in self._linked_unmapped_files}
        qvds = {
            urn: path
            for urn, path in paths.items()
            if urn in self._unmapped_qvd_files or path.lower().endswith(QVD_EXTENSION)
        }
        yield from self._gen_file_entities(qvds, DatasetSubTypes.QLIK_QVD)
        yield from self._gen_file_entities(
            {urn: path for urn, path in paths.items() if urn not in qvds},
            DatasetSubTypes.QLIK_DATASET,
        )
