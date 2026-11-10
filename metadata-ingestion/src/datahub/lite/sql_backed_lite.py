import json
import logging
import pathlib
import time
from abc import abstractmethod
from contextlib import contextmanager
from typing import (
    Any,
    ClassVar,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)

from datahub.emitter.aspect import ASPECT_MAP
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import mcps_from_mce
from datahub.emitter.serialization_helper import post_json_transform
from datahub.lite.lite_local import (
    AspectVersion,
    AutoComplete,
    Browseable,
    DataHubLiteLocal,
    LiteConfig,
    LiteTransactionError,
    LiteVersionConflict,
    PathNotFoundException,
    Searchable,
    SearchFlavor,
)
from datahub.metadata.schema_classes import (
    ChartInfoClass,
    ContainerClass,
    ContainerPropertiesClass,
    DashboardInfoClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    MetadataChangeEventClass,
    SubTypesClass,
    SystemMetadataClass,
    TagPropertiesClass,
    _Aspect,
)
from datahub.utilities.urns.data_platform_urn import DataPlatformUrn
from datahub.utilities.urns.dataset_urn import DatasetUrn
from datahub.utilities.urns.urn import Urn

logger = logging.getLogger(__name__)


def _placeholders(values: Sequence[Any]) -> str:
    return ",".join("?" * len(values))


class SqlBackedLite(DataHubLiteLocal[LiteConfig]):
    """Shared implementation for the embedded SQL engines behind DataHub Lite.

    Subclasses own the connection and the few dialect-specific bits below.
    Everything else is engine independent: the aspect versioning rules, the
    browse-path edge index, and the search queries. Both supported engines
    speak `?` placeholders and the `->>` JSON operator.
    """

    # Column types used when creating the tables. SQLite accepts any type name
    # and derives affinity from it, so only DuckDB really cares about these.
    STRING_TYPE: ClassVar[str] = "VARCHAR"
    JSON_TYPE: ClassVar[str] = "TEXT"
    BIGINT_TYPE: ClassVar[str] = "BIGINT"

    def __init__(self, config: LiteConfig) -> None:
        self.config = config
        self._transaction_active = False
        self._transaction_error: Optional[BaseException] = None
        fpath = pathlib.Path(self.location())
        fpath.parent.mkdir(exist_ok=True)
        self._connect(fpath)
        if not self.read_only:
            self._init_db()

    @property
    @abstractmethod
    def read_only(self) -> bool:
        pass

    @abstractmethod
    def _connect(self, fpath: pathlib.Path) -> None:
        pass

    @abstractmethod
    def _execute(self, query: str, params: Sequence[Any] = ()) -> List[Tuple]:
        pass

    @abstractmethod
    def _execute_one(self, query: str, params: Sequence[Any] = ()) -> Optional[Tuple]:
        pass

    def _begin(self) -> None:
        """Open a transaction, for engines that do not start one implicitly."""

    @abstractmethod
    def _commit(self) -> None:
        pass

    @abstractmethod
    def _rollback(self) -> None:
        pass

    @abstractmethod
    def _close_connection(self) -> None:
        pass

    @abstractmethod
    def _create_unique_index(
        self, index_name: str, table_name: str, columns: List[str]
    ) -> None:
        pass

    @classmethod
    def _json_text(cls, column: str, path: str) -> str:
        """SQL expression extracting `path` out of `column` as unquoted text."""
        return f"{column} ->> '{path}'"

    def _init_db(self) -> None:
        self._execute(
            "CREATE TABLE IF NOT EXISTS metadata_aspect_v2 "
            f"(urn {self.STRING_TYPE}, aspect_name {self.STRING_TYPE}, version {self.BIGINT_TYPE}, "
            f"metadata {self.JSON_TYPE}, system_metadata {self.JSON_TYPE}, createdon {self.BIGINT_TYPE})"
        )

        self._create_unique_index(
            "aspect_idx", "metadata_aspect_v2", ["urn", "aspect_name", "version"]
        )

        self._init_edge_table()
        self._commit()

    def _init_edge_table(self) -> None:
        self._execute(
            "CREATE TABLE IF NOT EXISTS metadata_edge_v2 "
            f"(src_id {self.STRING_TYPE}, relnship {self.STRING_TYPE}, "
            f"dst_id {self.STRING_TYPE}, dst_label {self.STRING_TYPE})"
        )

        self._create_unique_index(
            "edge_idx", "metadata_edge_v2", ["src_id", "relnship", "dst_id"]
        )

    def destroy(self) -> None:
        self._require_no_transaction("destroy")
        fpath = pathlib.Path(self.location())
        # Drop the connection first: Windows refuses to unlink a file SQLite
        # still holds open, and an open connection can rewrite the journal
        # sidecars after we remove them.
        self._close_connection()
        fpath.unlink()
        # SQLite leaves "-wal"/"-shm"/"-journal" behind for some journal modes,
        # DuckDB a ".wal" after an unclean shutdown.
        for suffix in ("-wal", "-shm", "-journal", ".wal"):
            fpath.with_name(fpath.name + suffix).unlink(missing_ok=True)

    def _require_no_transaction(self, operation: str) -> None:
        if self._transaction_active:
            raise RuntimeError(f"Cannot {operation} inside a DataHub Lite transaction")

    @contextmanager
    def transaction(self) -> Iterator[None]:
        self._require_no_transaction("nest a transaction")
        self._begin()
        self._transaction_active = True
        try:
            yield
            if self._transaction_error is not None:
                raise LiteTransactionError(
                    "Cannot commit: an operation in this transaction failed"
                ) from self._transaction_error
            self._commit()
        except BaseException:
            self._rollback()
            raise
        finally:
            self._transaction_active = False
            self._transaction_error = None

    @contextmanager
    def _record_transaction_errors(self) -> Iterator[None]:
        try:
            yield
        except BaseException as exc:
            if self._transaction_active and self._transaction_error is None:
                self._transaction_error = exc
            raise

    @contextmanager
    def _write_transaction(self) -> Iterator[None]:
        if not self._transaction_active:
            with self.transaction():
                yield
        else:
            if self._transaction_error is not None:
                raise LiteTransactionError(
                    "Cannot write: this transaction is rollback-only"
                ) from self._transaction_error
            with self._record_transaction_errors():
                yield

    def write(
        self,
        record: Union[MetadataChangeEventClass, MetadataChangeProposalWrapper],
        *,
        if_version_match: Optional[int] = None,
    ) -> Dict[str, int]:
        # Invalid standalone calls need no write lock, but any failed write in an
        # existing transaction must still prevent a partial batch from committing.
        with self._record_transaction_errors():
            writeables: Iterable[MetadataChangeProposalWrapper]
            if isinstance(record, MetadataChangeProposalWrapper):
                writeables = [record]
            elif isinstance(record, MetadataChangeEventClass):
                if if_version_match is not None:
                    raise ValueError("Version conditions require a single-aspect MCP")
                writeables = mcps_from_mce(record)
            else:
                raise ValueError(
                    f"DataHub Lite only supports MCEs and MCPs, not {type(record)}"
                )
            if if_version_match is not None and (
                if_version_match != -1 and if_version_match < 1
            ):
                raise ValueError("Expected version must be positive, or -1 for absent")
        with self._write_transaction():
            versions: Dict[str, int] = {}
            for writeable in writeables:
                assert writeable.aspectName
                versions[writeable.aspectName] = self._write_aspect(
                    writeable, if_version_match=if_version_match
                )
            return versions

    def _write_aspect(
        self,
        writeable: MetadataChangeProposalWrapper,
        *,
        if_version_match: Optional[int],
    ) -> int:
        assert writeable.entityUrn and writeable.aspectName and writeable.aspect
        urn, aspect_name = writeable.entityUrn, writeable.aspectName
        writeable_dict = writeable.to_obj(simplified_structure=True)
        latest = self._execute_one(
            "SELECT metadata, system_metadata FROM metadata_aspect_v2 "
            "WHERE urn = ? AND aspect_name = ? AND version = 0",
            [urn, aspect_name],
        )
        current_version = -1
        if latest is not None:
            metadata_dict = json.loads(latest[0])
            system_metadata = json.loads(latest[1])
            real_version = system_metadata.get("properties", {}).get("sysVersion")
            if real_version is None:
                max_version_row = self._execute_one(
                    "SELECT max(version) FROM metadata_aspect_v2 WHERE urn = ? AND aspect_name = ?",
                    [urn, aspect_name],
                )
                assert max_version_row
                real_version = max_version_row[0]
            current_version = int(real_version)

        if if_version_match is not None and if_version_match != current_version:
            raise LiteVersionConflict(if_version_match, current_version)

        observed = (
            writeable.systemMetadata.lastObserved
            if writeable.systemMetadata is not None
            and writeable.systemMetadata.lastObserved is not None
            else int(time.time() * 1000)
        )
        if writeable.systemMetadata is None:
            writeable.systemMetadata = SystemMetadataClass(
                lastObserved=observed, properties={}
            )
        elif writeable.systemMetadata.lastObserved is None:
            writeable.systemMetadata.lastObserved = observed
        if latest is not None and writeable_dict["aspect"]["json"] == metadata_dict:
            # Observation updates belong to the live row, not the immutable history.
            system_metadata["lastObserved"] = observed
            self._execute(
                "UPDATE metadata_aspect_v2 SET system_metadata = ? "
                "WHERE urn = ? AND aspect_name = ? AND version = 0",
                [json.dumps(system_metadata), urn, aspect_name],
            )
            return current_version

        new_version = current_version + 1 if latest is not None else 1
        system_metadata = writeable.systemMetadata.to_obj()
        system_metadata["lastObserved"] = observed
        system_metadata.setdefault("properties", {})["sysVersion"] = str(new_version)
        metadata_json = json.dumps(writeable_dict["aspect"]["json"])
        system_metadata_json = json.dumps(system_metadata)
        self._execute(
            "INSERT INTO metadata_aspect_v2 VALUES (?, ?, ?, ?, ?, ?)",
            [
                urn,
                aspect_name,
                new_version,
                metadata_json,
                system_metadata_json,
                observed,
            ],
        )
        if latest is None:
            self._execute(
                "INSERT INTO metadata_aspect_v2 VALUES (?, ?, ?, ?, ?, ?)",
                [urn, aspect_name, 0, metadata_json, system_metadata_json, observed],
            )
        else:
            self._execute(
                "UPDATE metadata_aspect_v2 SET metadata = ?, system_metadata = ?, createdon = ? "
                "WHERE urn = ? AND aspect_name = ? AND version = 0",
                [metadata_json, system_metadata_json, observed, urn, aspect_name],
            )
        self.post_update_hook(urn, aspect_name, writeable.aspect)
        return new_version

    @staticmethod
    def _aspect_version(
        aspect_name: str, row: Tuple[int, str, str, int]
    ) -> AspectVersion:
        return AspectVersion(
            value=ASPECT_MAP[aspect_name].from_obj(
                post_json_transform(json.loads(row[1]))
            ),
            version=row[0],
            system_metadata=SystemMetadataClass.from_obj(json.loads(row[2])),
            createdon=row[3],
        )

    def get_aspect(
        self, urn: str, aspect_name: str, version: Optional[int] = None
    ) -> Optional[AspectVersion]:
        if version is not None and version < 1:
            raise ValueError("Aspect versions must be positive; use None for latest")
        query = (
            "SELECT version, metadata, system_metadata, createdon FROM metadata_aspect_v2 "
            "WHERE urn = ? AND aspect_name = ? AND version > 0"
        )
        params: List[Union[str, int]] = [urn, aspect_name]
        if version is not None:
            query += " AND version = ?"
            params.append(version)
        row = self._execute_one(query + " ORDER BY version DESC LIMIT 1", params)
        return self._aspect_version(aspect_name, row) if row is not None else None

    def list_versions(self, urn: str, aspect_name: str) -> List[AspectVersion]:
        return [
            self._aspect_version(aspect_name, row)
            for row in self._execute(
                "SELECT version, metadata, system_metadata, createdon FROM metadata_aspect_v2 "
                "WHERE urn = ? AND aspect_name = ? AND version > 0 ORDER BY version",
                [urn, aspect_name],
            )
        ]

    def list_ids(self) -> Iterable[str]:
        for row in self._execute("SELECT DISTINCT urn FROM metadata_aspect_v2"):
            yield row[0]

    def get(
        self,
        id: str,
        aspects: Optional[List[str]],
        typed: bool = False,
        as_of: Optional[int] = None,
        details: Optional[bool] = False,
    ) -> Optional[Dict[str, Union[str, dict, _Aspect]]]:
        columns = "urn, aspect_name, metadata, system_metadata"
        where = "urn = ?"
        params: List[Any] = [id]
        if aspects:
            where += f" AND aspect_name IN ({_placeholders(aspects)})"
            params.extend(aspects)

        if as_of:
            # Version 0 mirrors the latest version, so the history lives in
            # versions >= 1. Rank per aspect_name rather than taking a single
            # global row, otherwise asking for several aspects returns just one.
            base_query = (
                f"SELECT {columns} FROM ("
                f"SELECT {columns}, ROW_NUMBER() OVER "
                "(PARTITION BY aspect_name ORDER BY version DESC) AS rank_in_aspect "
                f"FROM metadata_aspect_v2 WHERE {where} AND version > 0 AND createdon < ?"
                ") ranked WHERE rank_in_aspect = 1"
            )
            params.append(as_of)
        else:
            base_query = f"SELECT {columns} FROM metadata_aspect_v2 WHERE {where} AND version = 0"

        results = self._execute(base_query, params)
        result_map: Dict[str, Union[str, dict, _Aspect]] = {}
        for r in results:
            aspect_name: str = r[1]
            aspect: Union[dict, _Aspect] = json.loads(r[2])
            if typed:
                assert isinstance(aspect, dict)
                aspect = ASPECT_MAP[aspect_name].from_obj(post_json_transform(aspect))

            result_map[aspect_name] = aspect
            if details:
                system_metadata: Union[dict, SystemMetadataClass] = json.loads(r[3])
                if typed:
                    assert isinstance(system_metadata, dict)
                    system_metadata = SystemMetadataClass.from_obj(system_metadata)
                result_map[aspect_name].update({"__systemMetadata": system_metadata})  # type: ignore
        if result_map:
            result_map = {**{"urn": id}, **result_map}
            return result_map
        else:
            return None

    def search(
        self,
        query: str,
        flavor: SearchFlavor,
        aspects: Optional[List[str]] = None,
        snippet: bool = True,
    ) -> Iterable[Searchable]:
        aspects = aspects or []
        if flavor == SearchFlavor.FREE_TEXT:
            # LOWER(..) LIKE stands in for ILIKE, which SQLite does not have.
            name = self._json_text("metadata", "$.name")
            base_query = (
                "SELECT DISTINCT urn, 'urn', NULL FROM metadata_aspect_v2 WHERE version = 0 AND LOWER(urn) LIKE ? "
                "UNION "
                f"SELECT urn, aspect_name, metadata FROM metadata_aspect_v2 WHERE version = 0 AND LOWER({name}) LIKE ?"
            )
            like_pattern = f"%{query.lower()}%"
            for r in self._execute(base_query, [like_pattern, like_pattern]):
                yield Searchable(
                    id=r[0], aspect=r[1], snippet=r[2] if snippet else None
                )
        elif flavor == SearchFlavor.EXACT:
            base_query = f"SELECT urn, aspect_name, metadata FROM metadata_aspect_v2 WHERE version = 0 AND ({query})"
            for r in self._execute(base_query):
                yield Searchable(
                    id=r[0], aspect=r[1], snippet=r[2] if snippet else None
                )
        else:
            raise Exception(f"Unhandled search flavor {flavor}")

    def remove_edge(self, src: str, relnship: str) -> None:
        with self._write_transaction():
            self._execute(
                "DELETE FROM metadata_edge_v2 WHERE src_id = ? AND relnship = ?",
                [src, relnship],
            )

    def add_edge(
        self,
        src: Union[Urn, str],
        relnship: str,
        dst: Union[Urn, str],
        dst_label: Optional[str] = None,
        remove_existing: bool = False,
    ) -> None:
        src_id = str(src)
        dst_id = str(dst)
        logger.debug(f"Add edge {src_id},{dst_id},{relnship},{dst_label}")
        with self._write_transaction():
            query = "SELECT * FROM metadata_edge_v2 WHERE src_id = ? AND relnship = ?"
            params = [src_id, relnship]
            if not remove_existing:
                query += " AND dst_id = ?"
                params.append(dst_id)
            maybe_row = self._execute_one(query, params)
            if not maybe_row:
                self._execute(
                    "INSERT INTO metadata_edge_v2 VALUES (?, ?, ?, ?)",
                    [src_id, relnship, dst_id, dst_label],
                )
            else:
                read_dst_id = maybe_row[2]
                read_dst_label = maybe_row[3]
                update_fragment = ""
                update_params: List[Any] = []
                if read_dst_id != dst_id:
                    update_fragment = "dst_id = ?"
                    update_params = [dst_id]
                    where_clause = "src_id = ? AND relnship = ?"
                    where_params = [src_id, relnship]

                if read_dst_label != dst_label:
                    # Append rather than replace: when the destination and the
                    # label both change, dropping the dst_id assignment here
                    # leaves a leading comma and invalid SQL.
                    update_fragment = (
                        f"{update_fragment}, dst_label = ?"
                        if update_fragment
                        else "dst_label = ?"
                    )
                    update_params += [dst_label]
                    where_clause = "src_id = ? AND relnship = ? AND dst_id = ?"
                    where_params = [src_id, relnship, dst_id]
                    if read_dst_id != dst_id:
                        where_clause = "src_id = ? AND relnship = ?"
                        where_params = [src_id, relnship]

                if update_fragment:
                    self._execute(
                        f"UPDATE metadata_edge_v2 SET {update_fragment} WHERE {where_clause}",
                        update_params + where_params,
                    )

    def ls(self, path: str) -> List[Browseable]:
        def get_id_for_name(
            name: str,
            allowed_src_ids: Optional[List[str]] = None,
            expand_search: bool = False,
        ) -> List[str]:
            if not expand_search:
                results = self._execute(
                    "SELECT src_id FROM metadata_edge_v2 WHERE dst_id = ? AND relnship = 'name'",
                    [name],
                )
            else:
                results = self._execute(
                    "SELECT src_id FROM metadata_edge_v2 WHERE LOWER(dst_id) LIKE ? AND relnship = 'name'",
                    [f"{name.lower()}%"],
                )
            if not results:
                return []
            else:
                ids_returned = [r[0] for r in results]
                if allowed_src_ids:
                    allowed_ids = [
                        r[0]
                        for r in self._execute(
                            f"SELECT dst_id FROM metadata_edge_v2 WHERE src_id IN ({_placeholders(allowed_src_ids)})",
                            allowed_src_ids,
                        )
                    ]
                    ids_returned = [id for id in ids_returned if id in allowed_ids]
                return ids_returned

        def resolve_name_from_id(maybe_urn: str) -> str:
            results = self._execute(
                "SELECT dst_id FROM metadata_edge_v2 WHERE src_id = ? AND relnship = 'name'",
                [maybe_urn],
            )
            if not results:
                return maybe_urn
            else:
                return results[0][0]

        def children_query(parents: List[str], dst_ids: List[str]) -> str:
            return (
                "SELECT dst_id FROM metadata_edge_v2 WHERE relnship = 'child' "
                f"AND src_id IN ({_placeholders(parents)}) AND dst_id IN ({_placeholders(dst_ids)})"
            )

        pieces = [p for p in path.split("/") if p]

        pieces = ["__root__"] + pieces

        # very lazy walk
        in_list = [pieces[0]]

        for i, p in enumerate(pieces[1:]):
            ids = get_id_for_name(p, allowed_src_ids=in_list)
            if ids:
                query = children_query(in_list, ids)
                query_params = in_list + ids
            else:
                query = (
                    "SELECT dst_id FROM metadata_edge_v2 WHERE relnship = 'child' "
                    f"AND src_id IN ({_placeholders(in_list)}) AND (dst_id = ? OR dst_label = ?)"
                )
                query_params = in_list + [p, p]
            results = self._execute(query, query_params)
            if not results:
                ids = get_id_for_name(p, allowed_src_ids=in_list, expand_search=True)
                alternatives = None
                if ids:
                    query = children_query(in_list, ids)
                    results = self._execute(query, in_list + ids)
                    success_path = "/" + "/".join(pieces[1 : i + 1])
                    results_list = []
                    for r in results:
                        r_name = resolve_name_from_id(r[0])
                        results_list.append(
                            Browseable(
                                id=r[0],
                                name=r_name,
                                leaf=False,
                                parents=in_list,
                                auto_complete=AutoComplete(
                                    success_path=success_path,
                                    failed_token=p,
                                    suggested_path=f"{success_path}/{r_name}".replace(
                                        "//", "/"
                                    ),
                                ),
                            )
                        )
                    return results_list
                raise PathNotFoundException(
                    f"Path {path} not found at {p} for query: {query}, did you mean {alternatives}"
                )
            in_list = [r[0] for r in results]

        results = self._execute(
            f"SELECT dst_id, dst_label FROM metadata_edge_v2 WHERE src_id IN ({_placeholders(in_list)}) AND relnship = 'child'",
            in_list,
        )
        if results:
            results_list = [
                Browseable(
                    parents=in_list,
                    id=r[0],
                    name=r[1] if r[1] else resolve_name_from_id(r[0]),
                )
                for r in results
            ]
            return results_list
        else:
            # this is a leaf, return the urn for the entity
            return [
                Browseable(id=r, name=pieces[-1], leaf=True)
                for r in in_list
                if r != "__root__"
            ]

    def reindex(self) -> None:
        self._require_no_transaction("reindex")
        with self.transaction():
            # DuckDB can reject deleting and reinserting persisted unique keys in
            # one transaction. Transactional DDL also preserves the old index if
            # the rebuild fails, without reusing those index entries.
            self._execute("DROP TABLE metadata_edge_v2")
            self._init_edge_table()
            for urn_aspect_dict in self.get_all_entities(typed=True):
                for urn, aspect_map in urn_aspect_dict.items():
                    for aspect_name, aspect_value in aspect_map.items():
                        assert isinstance(aspect_value, _Aspect)
                        self.post_update_hook(urn, aspect_name, aspect_value)
                    self.global_post_update_hook(urn, aspect_map)  # type: ignore

    def get_all_entities(
        self, typed: bool = False
    ) -> Iterable[Dict[str, Union[dict, _Aspect]]]:
        results = self._execute(
            "SELECT urn, aspect_name, metadata, system_metadata FROM metadata_aspect_v2 "
            "WHERE version = 0 ORDER BY urn, aspect_name"
        )
        aspect_map: Dict[str, _Aspect] = {}
        current_urn = None
        for r in results:
            urn = r[0]
            aspect_name = r[1]
            aspect_payload = json.loads(r[2])
            if typed:
                assert aspect_name in ASPECT_MAP, (
                    f"Missing aspect name {aspect_name} in the registry"
                )
                try:
                    aspect_payload = ASPECT_MAP[aspect_name].from_obj(
                        post_json_transform(aspect_payload)
                    )
                except Exception as e:
                    logger.exception(
                        f"Failed to process urn: {urn}, aspect_name: {aspect_name}, metadata: {aspect_payload}",
                        exc_info=e,
                    )
                    raise

            if current_urn is None:
                current_urn = urn
            if urn != current_urn:
                if aspect_map:
                    yield {current_urn: aspect_map}
                    aspect_map = {}
                    current_urn = urn

            aspect_map[aspect_name] = aspect_payload

        if aspect_map:
            assert current_urn
            yield {current_urn: aspect_map}

    def get_all_aspects(self) -> Iterable[MetadataChangeProposalWrapper]:
        results = self._execute(
            "SELECT urn, aspect_name, metadata, system_metadata FROM metadata_aspect_v2 WHERE version = 0"
        )
        for r in results:
            urn = r[0]
            aspect_name = r[1]
            aspect_metadata = ASPECT_MAP[aspect_name].from_obj(
                post_json_transform(json.loads(r[2]))
            )  # type: ignore
            system_metadata = SystemMetadataClass.from_obj(json.loads(r[3]))
            mcp = MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspectName=aspect_name,
                aspect=aspect_metadata,
                systemMetadata=system_metadata,
            )
            yield mcp

    def close(self) -> None:
        self._require_no_transaction("close")
        if not self.read_only:
            self.reindex()
        self._close_connection()

    def get_category_from_platform(self, data_platform_urn: DataPlatformUrn) -> Urn:
        # TODO: Use the dataplatforms.json resource to auto-gen this
        category_to_platform_map = {
            "bi_tools": ["looker", "tableau", "powerbi", "superset"],
            "databases": [
                "mysql",
                "snowflake",
                "postgres",
                "bigquery",
                "redshift",
                "clickhouse",
            ],
            "data_lakes": [
                "s3",
                "hdfs",
                "delta-lake",
                "glue",
                "hive",
                "presto",
                "iceberg",
                "trino",
            ],
            "streaming": ["kafka"],
            "orchestrators": ["airflow", "spark"],
            "data_movers": ["kafka-connect", "nifi"],
            "transformation_tools": ["dbt"],
            "data_quality": ["great-expectations"],
        }
        for k, v in category_to_platform_map.items():
            if data_platform_urn.get_entity_id_as_string() in v:
                return Urn(entity_type="systemNode", entity_id=[k])

        logger.debug(
            f"Failed to find category for platform {data_platform_urn}, mapping to generic data_platform"
        )
        return Urn(entity_type="systemNode", entity_id=["data_platforms"])

    def global_post_update_hook(
        self, entity_urn: str, aspect_map: Dict[str, _Aspect]
    ) -> None:
        def pluralize(noun: str) -> str:
            return noun.lower() + "s"

        def get_typed_aspect(
            aspect_map: Dict[str, _Aspect], aspect_type: Type[_Aspect]
        ) -> Optional[_Aspect]:
            aspect_names = [k for k, v in ASPECT_MAP.items() if v == aspect_type]
            if aspect_names:
                return aspect_map.get(aspect_names[0])
            raise Exception(
                f"Unable to locate aspect type {aspect_type} in the registry"
            )

        if not entity_urn:
            logger.error(f"Bad input {entity_urn}: {aspect_map}")

        container: Optional[ContainerClass] = get_typed_aspect(  # type: ignore
            aspect_map, ContainerClass
        )  # type: ignore
        subtypes: Optional[SubTypesClass] = get_typed_aspect(aspect_map, SubTypesClass)  # type: ignore
        dpi: Optional[DataPlatformInstanceClass] = get_typed_aspect(  # type: ignore
            aspect_map, DataPlatformInstanceClass
        )  # type: ignore

        needs_platform = Urn.from_string(entity_urn).get_type() in [
            "dataset",
            "container",
            "chart",
            "dashboard",
            "dataFlow",
            "dataJob",
        ]
        entity_urn_parsed = Urn.from_string(entity_urn)
        if entity_urn_parsed.get_type() in ["dataFlow", "dataJob"]:
            self.add_edge(
                entity_urn,
                "name",
                entity_urn_parsed.get_entity_id()[1],
                remove_existing=True,
            )

        if not container and needs_platform:
            # this is a top-level entity
            if not dpi:
                logger.debug(f"No data platform instance for {entity_urn}")
                maybe_parent_urn = Urn.from_string(entity_urn).get_entity_id()[0]
                needs_dpi = False
                if maybe_parent_urn.startswith(Urn.URN_PREFIX):
                    parent_urn = maybe_parent_urn
                    if Urn.from_string(maybe_parent_urn).get_type() == "dataPlatform":
                        data_platform_urn = DataPlatformUrn.from_string(
                            maybe_parent_urn
                        )
                        needs_dpi = True
                else:
                    data_platform_urn = DataPlatformUrn.create_from_id(maybe_parent_urn)
                    needs_dpi = True

                if needs_dpi:
                    data_platform_instance = "default"
                    data_platform_instance_urn = Urn(
                        entity_type="dataPlatformInstance",
                        entity_id=[str(data_platform_urn), data_platform_instance],
                    )
                    self._create_edges_from_data_platform_instance(
                        data_platform_instance_urn
                    )
                    parent_urn = str(data_platform_instance_urn)
            else:
                data_platform_urn = DataPlatformUrn.from_string(dpi.platform)
                data_platform_instance = dpi.instance or "default"
                data_platform_instance_urn = Urn(
                    entity_type="dataPlatformInstance",
                    entity_id=[str(data_platform_urn), data_platform_instance],
                )
                parent_urn = str(data_platform_instance_urn)
        elif container:
            parent_urn = container.container
        else:
            parent_urn = "__root__"

        types = (
            subtypes.typeNames if subtypes else [Urn.from_string(entity_urn).get_type()]
        )
        for t in types:
            type_urn = Urn(entity_type="systemNode", entity_id=[parent_urn, t])
            self.add_edge(parent_urn, "child", type_urn)
            self.add_edge(type_urn, "child", entity_urn)
            self.add_edge(type_urn, "name", pluralize(t), remove_existing=True)

    def _create_edges_from_data_platform_instance(
        self, data_platform_instance_urn: Urn
    ) -> None:
        data_platform_urn = DataPlatformUrn.from_string(
            data_platform_instance_urn.get_entity_id()[0]
        )
        data_platform_instances_urn = Urn(
            entity_type="systemNode", entity_id=[str(data_platform_urn), "instances"]
        )

        data_platform_category = self.get_category_from_platform(data_platform_urn)
        # /<data_platform_category>/

        self.add_edge("__root__", "child", str(data_platform_category))
        self.add_edge(
            str(data_platform_category),
            "name",
            data_platform_category.get_entity_id_as_string(),
            remove_existing=True,
        )
        # /<data_platform_category>/<data_platform>
        self.add_edge(
            str(data_platform_category),
            "child",
            str(data_platform_urn),
            data_platform_urn.get_entity_id_as_string(),
        )
        self.add_edge(
            data_platform_urn,
            "name",
            data_platform_urn.get_entity_id_as_string(),
            remove_existing=True,
        )
        # /<data_platform_category>/<data_platform>/instances
        self.add_edge(str(data_platform_urn), "child", str(data_platform_instances_urn))
        self.add_edge(
            str(data_platform_instances_urn), "name", "instances", remove_existing=True
        )
        # /<data_platform_category>/<data_platform>/instances/<instance_name>
        self.add_edge(
            str(data_platform_instances_urn),
            "child",
            str(data_platform_instance_urn),
            data_platform_instance_urn.get_entity_id()[-1],
        )

    def post_update_hook(
        self, entity_urn: str, aspect_name: str, aspect: _Aspect
    ) -> None:
        if isinstance(aspect, DatasetPropertiesClass):
            dp: DatasetPropertiesClass = aspect
            if dp.name:
                specific_urn = DatasetUrn.from_string(entity_urn)
                if (
                    specific_urn.get_data_platform_urn().get_entity_id_as_string()
                    == "looker"
                ):
                    # Looker dataset urns (views and explores) need special handling
                    dataset_id = specific_urn.get_entity_id()[1]
                    self.add_edge(
                        entity_urn,
                        "name",
                        dataset_id.replace(".explore", "").replace(".view", ""),
                        remove_existing=True,
                    )
                else:
                    self.add_edge(entity_urn, "name", dp.name, remove_existing=True)
        elif isinstance(aspect, ContainerPropertiesClass):
            cp: ContainerPropertiesClass = aspect
            self.add_edge(entity_urn, "name", cp.name, remove_existing=True)
        elif isinstance(aspect, DataPlatformInstanceClass):
            dpi: DataPlatformInstanceClass = aspect
            data_platform_urn = DataPlatformUrn.from_string(dpi.platform)
            data_platform_instance = dpi.instance or "default"
            data_platform_instance_urn = Urn(
                entity_type="dataPlatformInstance",
                entity_id=[str(data_platform_urn), data_platform_instance],
            )
            self._create_edges_from_data_platform_instance(data_platform_instance_urn)
        elif isinstance(aspect, (ChartInfoClass, DashboardInfoClass)):
            urn = Urn.from_string(entity_urn)
            self.add_edge(
                entity_urn,
                "name",
                aspect.title + f" ({urn.get_entity_id()[-1]})",
                remove_existing=True,
            )
        elif isinstance(aspect, TagPropertiesClass):
            self.add_edge(entity_urn, "name", aspect.name, remove_existing=True)
