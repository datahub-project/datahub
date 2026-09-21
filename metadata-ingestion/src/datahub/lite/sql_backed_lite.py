import json
import logging
import pathlib
import time
from abc import abstractmethod
from typing import (
    Any,
    ClassVar,
    Dict,
    Iterable,
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
    AutoComplete,
    Browseable,
    DataHubLiteLocal,
    LiteConfig,
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

    def _init_db(self) -> None:
        self._execute(
            "CREATE TABLE IF NOT EXISTS metadata_aspect_v2 "
            f"(urn {self.STRING_TYPE}, aspect_name {self.STRING_TYPE}, version {self.BIGINT_TYPE}, "
            f"metadata {self.JSON_TYPE}, system_metadata {self.JSON_TYPE}, createdon {self.BIGINT_TYPE})"
        )

        self._create_unique_index(
            "aspect_idx", "metadata_aspect_v2", ["urn", "aspect_name", "version"]
        )

        self._execute(
            "CREATE TABLE IF NOT EXISTS metadata_edge_v2 "
            f"(src_id {self.STRING_TYPE}, relnship {self.STRING_TYPE}, "
            f"dst_id {self.STRING_TYPE}, dst_label {self.STRING_TYPE})"
        )

        self._create_unique_index(
            "edge_idx", "metadata_edge_v2", ["src_id", "relnship", "dst_id"]
        )
        self._commit()

    def destroy(self) -> None:
        fpath = pathlib.Path(self.location())
        fpath.unlink()
        # SQLite leaves "-wal"/"-shm"/"-journal" behind for some journal modes,
        # DuckDB a ".wal" after an unclean shutdown.
        for suffix in ("-wal", "-shm", "-journal", ".wal"):
            fpath.with_name(fpath.name + suffix).unlink(missing_ok=True)

    def write(
        self,
        record: Union[
            MetadataChangeEventClass,
            MetadataChangeProposalWrapper,
        ],
    ) -> None:
        writeables: Iterable[MetadataChangeProposalWrapper]
        if isinstance(record, MetadataChangeProposalWrapper):
            writeables = [record]
        elif isinstance(record, MetadataChangeEventClass):
            writeables = mcps_from_mce(record)
        else:
            raise ValueError(
                f"DataHub Lite only supports MCEs and MCPs, not {type(record)}"
            )

        if not writeables:
            return

        # The edge writes issued by post_update_hook commit as they go, so this
        # transaction only spans up to the first of those. It still keeps the
        # paired v0 + vN aspect inserts atomic, and keeps the two engines
        # behaving the same: SQLite opens a transaction implicitly on DML,
        # DuckDB needs to be told.
        self._begin()
        for writeable in writeables:
            needs_write = False
            try:
                writeable_dict = writeable.to_obj(simplified_structure=True)
                max_row = self._execute_one(
                    "SELECT metadata, system_metadata FROM metadata_aspect_v2 WHERE urn = ? AND aspect_name = ? AND version = 0",
                    [writeable.entityUrn, writeable.aspectName],
                )
                if max_row is None:
                    new_version = 1
                    needs_write = True
                else:
                    metadata_dict = json.loads(max_row[0])
                    system_metadata = json.loads(max_row[1])
                    real_version = system_metadata.get("properties", {}).get(
                        "sysVersion"
                    )
                    if real_version is None:
                        max_version_row = self._execute_one(
                            "SELECT max(version) FROM metadata_aspect_v2 WHERE urn = ? AND aspect_name = ?",
                            [writeable.entityUrn, writeable.aspectName],
                        )
                        assert max_version_row
                        real_version = max_version_row[0]
                    # systemMetadata.properties is a map<string, string>, so
                    # sysVersion comes back as a string.
                    real_version = int(real_version)

                    if writeable_dict["aspect"]["json"] == metadata_dict:
                        needs_write = False
                        new_version = real_version
                    else:
                        needs_write = True
                        new_version = real_version + 1

                current_time = int(time.time() * 1000.0)
                created_on = current_time
                if (
                    writeable.systemMetadata is not None
                    and writeable.systemMetadata.lastObserved
                ):
                    created_on = writeable.systemMetadata.lastObserved

                if writeable.systemMetadata is None:
                    writeable.systemMetadata = SystemMetadataClass(
                        lastObserved=created_on, properties={}
                    )
                elif writeable.systemMetadata.lastObserved is None:
                    writeable.systemMetadata.lastObserved = created_on

                if "systemMetadata" not in writeable_dict:
                    # to_obj() ran before we populated systemMetadata above, so the
                    # key is absent whenever the caller did not supply one.
                    writeable_dict["systemMetadata"] = writeable.systemMetadata.to_obj()
                if "properties" not in writeable_dict["systemMetadata"]:
                    writeable_dict["systemMetadata"]["properties"] = {}
                writeable_dict["systemMetadata"]["properties"]["sysVersion"] = str(
                    new_version
                )
                if needs_write:
                    self._execute(
                        "INSERT INTO metadata_aspect_v2 VALUES (?, ?, ?, ?, ?, ?)",
                        [
                            writeable.entityUrn,
                            writeable.aspectName,
                            new_version,
                            json.dumps(writeable_dict["aspect"]["json"]),
                            json.dumps(writeable_dict["systemMetadata"]),
                            created_on,
                        ],
                    )
                    if not max_row:
                        self._execute(
                            "INSERT INTO metadata_aspect_v2 VALUES (?, ?, ?, ?, ?, ?)",
                            [
                                writeable.entityUrn,
                                writeable.aspectName,
                                0,
                                json.dumps(writeable_dict["aspect"]["json"]),
                                json.dumps(writeable_dict["systemMetadata"]),
                                created_on,
                            ],
                        )
                    else:
                        # we update the existing v0 row
                        self._execute(
                            "UPDATE metadata_aspect_v2 SET metadata = ?, system_metadata = ? WHERE urn = ? AND aspect_name = ? AND version = 0",
                            [
                                json.dumps(writeable_dict["aspect"]["json"]),
                                json.dumps(writeable_dict["systemMetadata"]),
                                writeable.entityUrn,
                                writeable.aspectName,
                            ],
                        )
                else:
                    # this is a dup, we still want to update the lastObserved timestamp
                    if not system_metadata:
                        system_metadata = {
                            "lastObserved": writeable.systemMetadata.lastObserved
                        }
                    else:
                        system_metadata["lastObserved"] = (
                            writeable.systemMetadata.lastObserved
                        )
                    self._execute(
                        "UPDATE metadata_aspect_v2 SET system_metadata = ? WHERE urn = ? AND aspect_name = ? AND version = 0",
                        [
                            json.dumps(system_metadata),
                            writeable.entityUrn,
                            writeable.aspectName,
                        ],
                    )
            except Exception as e:
                self._rollback()
                logger.error(f"Failed to write {writeable}", exc_info=e)
                raise
            else:
                if needs_write:
                    assert (
                        writeable.entityUrn
                        and writeable.aspectName
                        and writeable.aspect
                    )
                    self.post_update_hook(
                        writeable.entityUrn, writeable.aspectName, writeable.aspect
                    )

        self._commit()

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
            base_query = (
                "SELECT DISTINCT urn, 'urn', NULL FROM metadata_aspect_v2 WHERE LOWER(urn) LIKE ? "
                "UNION "
                "SELECT urn, aspect_name, metadata FROM metadata_aspect_v2 WHERE LOWER(metadata ->> '$.name') LIKE ?"
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
        try:
            self._execute(
                "DELETE FROM metadata_edge_v2 WHERE src_id = ? AND relnship = ?",
                [src, relnship],
            )
        except Exception as e:
            logger.error("Failed to remove any edge", exc_info=e)
        else:
            self._commit()

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
        try:
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
                    update_fragment = (
                        ",dst_label = ?" if update_fragment else "dst_label = ?"
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
        except Exception as e:
            logger.error(
                f"Failed to write {src_id}, {relnship}, {dst_id}, {dst_label}",
                exc_info=e,
            )
            raise

        self._commit()

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
        self._execute("DELETE FROM metadata_edge_v2")
        self._commit()
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
                    try:
                        self._create_edges_from_data_platform_instance(
                            data_platform_instance_urn
                        )
                    except Exception as e:
                        logger.error(
                            f"Failed to generate edges entity {entity_urn}", exc_info=e
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
