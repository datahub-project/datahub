import json
import logging
import pathlib
import time
from typing import Any, Dict, Iterable, List, Optional, Type, Union

import duckdb

from datahub.emitter.aspect import ASPECT_MAP
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import mcps_from_mce
from datahub.emitter.serialization_helper import post_json_transform
from datahub.lite.duckdb_lite_config import DuckDBLiteConfig
from datahub.lite.lite_local import (
    AutoComplete,
    Browseable,
    DataHubLiteLocal,
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


class DuckDBLite(DataHubLiteLocal[DuckDBLiteConfig]):
    @classmethod
    def create(cls, config_dict: dict) -> "DuckDBLite":
        config: DuckDBLiteConfig = DuckDBLiteConfig.parse_obj(config_dict)
        return DuckDBLite(config)

    def __init__(self, config: DuckDBLiteConfig) -> None:
        self.config = config
        fpath = pathlib.Path(self.config.file)
        fpath.parent.mkdir(exist_ok=True)
        self.duckdb_client = duckdb.connect(
            str(fpath), read_only=config.read_only, config=config.options
        )
        if not config.read_only:
            self._init_db()

    def _create_unique_index(
        self, index_name: str, table_name: str, columns: list
    ) -> None:
        try:
            self.duckdb_client.execute(
                f"CREATE UNIQUE INDEX {index_name} ON {table_name} ({', '.join(columns)})"
            )
        except duckdb.CatalogException as e:
            if "already exists" not in str(e).lower():
                raise

    def _init_db(self) -> None:
        self.duckdb_client.execute(
            "CREATE TABLE IF NOT EXISTS metadata_aspect_v2 "
            "(urn VARCHAR, aspect_name VARCHAR, version BIGINT, metadata JSON, system_metadata JSON, createdon BIGINT)"
        )

        self._create_unique_index(
            "aspect_idx", "metadata_aspect_v2", ["urn", "aspect_name", "version"]
        )

        self.duckdb_client.execute(
            "CREATE TABLE IF NOT EXISTS metadata_edge_v2 "
            "(src_id VARCHAR, relnship VARCHAR, dst_id VARCHAR, dst_label VARCHAR)"
        )

        self._create_unique_index(
            "edge_idx", "metadata_edge_v2", ["src_id", "relnship", "dst_id"]
        )

    def location(self) -> str:
        return self.config.file

    def destroy(self) -> None:
        fpath = pathlib.Path(self.config.file)
        fpath.unlink()

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
                f"DuckDBCatalog only supports MCEs and MCPs, not {type(record)}"
            )

        if not writeables:
            return

        # TODO use `with` for transaction
        self.duckdb_client.begin()
        for writeable in writeables:
            needs_write = False
            try:
                writeable_dict = writeable.to_obj(simplified_structure=True)
                exists = self.duckdb_client.execute(
                    "SELECT metadata, system_metadata FROM metadata_aspect_v2 WHERE urn = ? AND aspect_name = ? AND version = 0",
                    [writeable.entityUrn, writeable.aspectName],
                )
                max_row = exists.fetchone()
                if max_row is None:
                    new_version = 1
                    needs_write = True
                else:
                    metadata_dict = json.loads(max_row[0])  # type: ignore
                    system_metadata = json.loads(max_row[1])  # type: ignore
                    real_version = system_metadata.get("properties", {}).get(
                        "sysVersion"
                    )
                    if real_version is None:
                        max_version_row = self.duckdb_client.execute(
                            "SELECT max(version) FROM metadata_aspect_v2 WHERE urn = ? AND aspect_name = ?",
                            [writeable.entityUrn, writeable.aspectName],
                        ).fetchone()
                        real_version = max_version_row[0]  # type: ignore

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

                if "properties" not in writeable_dict["systemMetadata"]:
                    writeable_dict["systemMetadata"]["properties"] = {}
                writeable_dict["systemMetadata"]["properties"][
                    "sysVersion"
                ] = new_version
                if needs_write:
                    self.duckdb_client.execute(
                        query="INSERT INTO metadata_aspect_v2 VALUES (?, ?, ?, ?, ?, ?)",
                        parameters=[
                            writeable.entityUrn,
                            writeable.aspectName,
                            new_version,
                            json.dumps(writeable_dict["aspect"]["json"]),
                            json.dumps(writeable_dict["systemMetadata"]),
                            created_on,
                        ],
                    )
                    if not max_row:
                        self.duckdb_client.execute(
                            query="INSERT INTO metadata_aspect_v2 VALUES (?, ?, ?, ?, ?, ?)",
                            parameters=[
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
                        self.duckdb_client.execute(
                            query="UPDATE metadata_aspect_v2 SET metadata = ?, system_metadata = ? WHERE urn = ? AND aspect_name = ? AND version = 0",
                            parameters=[
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
                        system_metadata[
                            "lastObserved"
                        ] = writeable.systemMetadata.lastObserved
                    self.duckdb_client.execute(
                        query="UPDATE metadata_aspect_v2 SET system_metadata = ? WHERE urn = ? AND aspect_name = ? AND version = 0",
                        parameters=[
                            json.dumps(system_metadata),
                            writeable.entityUrn,
                            writeable.aspectName,
                        ],
                    )
            except Exception as e:
                logger.error(f"Failed to write {writeable}", e)
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

        self.duckdb_client.commit()

    def list_ids(self) -> Iterable[str]:
        self.duckdb_client.execute("SELECT distinct(urn) from metadata_aspect_v2")
        for row in self.duckdb_client.fetchall():
            yield row[0]

    def get(
        self,
        id: str,
        aspects: Optional[List[str]],
        typed: bool = False,
        as_of: Optional[int] = None,
        details: Optional[bool] = False,
    ) -> Optional[Dict[str, Union[str, dict, _Aspect]]]:
        base_query = "SELECT urn, aspect_name, metadata, system_metadata from metadata_aspect_v2 WHERE urn = ?"
        if aspects:
            base_query += (
                " AND aspect_name IN (" + ",".join([f"'{x}'" for x in aspects]) + ")"
            )
        if as_of:
            base_query += f" AND createdon < '{as_of}' ORDER BY version DESC LIMIT 1"
        else:
            base_query += " AND version = 0"

        self.duckdb_client.execute(base_query, [id])
        results = self.duckdb_client.fetchall()
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
        aspects: List[str] = [],
        snippet: bool = True,
    ) -> Iterable[Searchable]:
        if flavor == SearchFlavor.FREE_TEXT:
            base_query = f"SELECT distinct(urn), 'urn', NULL from metadata_aspect_v2 where urn ILIKE '%{query}%' UNION SELECT urn, aspect_name, metadata from metadata_aspect_v2 where metadata->>'$.name' ILIKE '%{query}%'"
            for r in self.duckdb_client.execute(base_query).fetchall():
                yield Searchable(
                    id=r[0], aspect=r[1], snippet=r[2] if snippet else None
                )
        elif flavor == SearchFlavor.EXACT:
            base_query = f"SELECT urn, aspect_name, metadata from metadata_aspect_v2 where version = 0 AND ({query})"
            for r in self.duckdb_client.execute(base_query).fetchall():
                yield Searchable(
                    id=r[0], aspect=r[1], snippet=r[2] if snippet else None
                )
        else:
            raise Exception(f"Unhandled search flavor {flavor}")

    def remove_edge(self, src: str, relnship: str) -> None:
        try:
            self.duckdb_client.execute(
                "DELETE FROM metadata_edge_v2 WHERE src_id = ? AND relnship = ?",
                [src, relnship],
            )
        except Exception as e:
            logger.error("Failed to remove any edge", e)
        else:
            self.duckdb_client.commit()

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
            exists = self.duckdb_client.execute(query, params)
            maybe_row = exists.fetchone()
            if not maybe_row:
                self.duckdb_client.execute(
                    query="INSERT INTO metadata_edge_v2 VALUES (?, ?, ?, ?)",
                    parameters=[src_id, relnship, dst_id, dst_label],
                )
            else:
                read_dst_id = maybe_row[2]  # type: ignore
                read_dst_label = maybe_row[3]  # type: ignore
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
                    query = f"UPDATE metadata_edge_v2 SET {update_fragment} WHERE {where_clause}"
                    params = update_params + where_params
                    self.duckdb_client.execute(
                        query=query,
                        parameters=params,
                    )
        except Exception as e:
            logger.error(
                f"Failed to write {src_id}, {relnship}, {dst_id}, {dst_label}", e
            )
            raise

        self.duckdb_client.commit()

    def ls(self, path: str) -> List[Browseable]:
        def get_id_for_name(
            name: str,
            allowed_src_ids: Optional[List[str]] = None,
            expand_search: bool = False,
        ) -> List[str]:
            if not expand_search:
                query = f"SELECT src_id from metadata_edge_v2 where dst_id ='{name}' and relnship = 'name'"
            else:
                query = f"SELECT src_id from metadata_edge_v2 where dst_id ILIKE '{name}%' and relnship = 'name'"
            results = self.duckdb_client.execute(query).fetchall()
            if not results:
                return []
            else:
                ids_returned = [r[0] for r in results]
                if allowed_src_ids:
                    allowed_src_ids_quoted = [f"'{r}'" for r in allowed_src_ids]
                    query = f"SELECT dst_id from metadata_edge_v2 where src_id IN ({','.join(allowed_src_ids_quoted)})"
                    allowed_ids = [
                        r[0] for r in self.duckdb_client.execute(query).fetchall()
                    ]
                    ids_returned = [id for id in ids_returned if id in allowed_ids]
                return ids_returned

        def resolve_name_from_id(maybe_urn: str) -> str:
            query = f"SELECT dst_id from metadata_edge_v2 where src_id = '{maybe_urn}' and relnship = 'name'"
            results = self.duckdb_client.execute(query).fetchall()
            if not results:
                return maybe_urn
            else:
                return results[0][0]

        pieces = [p for p in path.split("/") if p]

        pieces = ["__root__"] + pieces

        # very lazy walk
        in_list = [pieces[0]]
        in_list_quoted = [f"'{r}'" for r in in_list]

        for i, p in enumerate(pieces[1:]):
            ids = get_id_for_name(p, allowed_src_ids=in_list)
            if ids:
                ids_quoted = [f"'{r}'" for r in ids]
                query = f"SELECT dst_id from metadata_edge_v2 where relnship = 'child' AND src_id IN ({','.join(in_list_quoted)}) and dst_id IN ({','.join(ids_quoted)})"
            else:
                query = f"SELECT dst_id from metadata_edge_v2 where relnship = 'child' AND src_id IN ({','.join(in_list_quoted)}) and (dst_id = '{p}' OR dst_label = '{p}')"
            results = self.duckdb_client.execute(query).fetchall()
            if not results:
                ids = get_id_for_name(p, allowed_src_ids=in_list, expand_search=True)
                alternatives = None
                if ids:
                    ids_quoted = [f"'{r}'" for r in ids]
                    query = f"SELECT dst_id from metadata_edge_v2 where relnship = 'child' AND src_id IN ({','.join(in_list_quoted)}) and dst_id IN ({','.join(ids_quoted)})"
                    results = self.duckdb_client.execute(query).fetchall()
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
            in_list_quoted = [f"'{r}'" for r in in_list]

        query = f"SELECT dst_id, dst_label from metadata_edge_v2 where src_id IN ({','.join(in_list_quoted)}) and relnship = 'child'"
        results = self.duckdb_client.execute(query).fetchall()
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
        self.duckdb_client.execute("DELETE FROM metadata_edge_v2")
        self.duckdb_client.commit()
        for urn_aspect_dict in self.get_all_entities(typed=True):
            for urn, aspect_map in urn_aspect_dict.items():
                for aspect_name, aspect_value in aspect_map.items():
                    assert isinstance(aspect_value, _Aspect)
                    self.post_update_hook(urn, aspect_name, aspect_value)
                self.global_post_update_hook(urn, aspect_map)  # type: ignore

    def get_all_entities(
        self, typed: bool = False
    ) -> Iterable[Dict[str, Union[dict, _Aspect]]]:
        query = "SELECT urn, aspect_name, metadata, system_metadata from metadata_aspect_v2 where version = 0 order by (urn, aspect_name)"
        results = self.duckdb_client.execute(query)
        aspect_map: Dict[str, _Aspect] = {}
        current_urn = None
        for r in results.fetchall():
            urn = r[0]
            aspect_name = r[1]
            aspect_payload = json.loads(r[2])
            if typed:
                assert (
                    aspect_name in ASPECT_MAP
                ), f"Missing aspect name {aspect_name} in the registry"
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
        query = "SELECT urn, aspect_name, metadata, system_metadata from metadata_aspect_v2 where version = 0"
        results = self.duckdb_client.execute(query)
        for r in results.fetchall():
            urn = r[0]
            aspect_name = r[1]
            aspect_metadata = ASPECT_MAP[aspect_name].from_obj(post_json_transform(json.loads(r[2])))  # type: ignore
            system_metadata = SystemMetadataClass.from_obj(json.loads(r[3]))
            mcp = MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspectName=aspect_name,
                aspect=aspect_metadata,
                systemMetadata=system_metadata,
            )
            yield mcp

    def close(self) -> None:
        self.reindex()
        self.duckdb_client.close()

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

        needs_platform = Urn.create_from_string(entity_urn).get_type() in [
            "dataset",
            "container",
            "chart",
            "dashboard",
            "dataFlow",
            "dataJob",
        ]
        entity_urn_parsed = Urn.create_from_string(entity_urn)
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
                maybe_parent_urn = Urn.create_from_string(entity_urn).get_entity_id()[0]
                needs_dpi = False
                if maybe_parent_urn.startswith(Urn.URN_PREFIX):
                    parent_urn = maybe_parent_urn
                    if (
                        Urn.create_from_string(maybe_parent_urn).get_type()
                        == "dataPlatform"
                    ):
                        data_platform_urn = DataPlatformUrn.create_from_string(
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
                        logger.error(f"Failed to generate edges entity {entity_urn}", e)
                    parent_urn = str(data_platform_instance_urn)
            else:
                data_platform_urn = DataPlatformUrn.create_from_string(dpi.platform)
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
            subtypes.typeNames
            if subtypes
            else [Urn.create_from_string(entity_urn).get_type()]
        )
        for t in types:
            type_urn = Urn(entity_type="systemNode", entity_id=[parent_urn, t])
            self.add_edge(parent_urn, "child", type_urn)
            self.add_edge(type_urn, "child", entity_urn)
            self.add_edge(type_urn, "name", pluralize(t), remove_existing=True)

    def _create_edges_from_data_platform_instance(
        self, data_platform_instance_urn: Urn
    ) -> None:
        data_platform_urn = DataPlatformUrn.create_from_string(
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
                specific_urn = DatasetUrn.create_from_string(entity_urn)
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
            data_platform_urn = DataPlatformUrn.create_from_string(dpi.platform)
            data_platform_instance = dpi.instance or "default"
            data_platform_instance_urn = Urn(
                entity_type="dataPlatformInstance",
                entity_id=[str(data_platform_urn), data_platform_instance],
            )
            self._create_edges_from_data_platform_instance(data_platform_instance_urn)
        elif isinstance(aspect, ChartInfoClass):
            urn = Urn.create_from_string(entity_urn)
            self.add_edge(
                entity_urn,
                "name",
                aspect.title + f" ({urn.get_entity_id()[-1]})",
                remove_existing=True,
            )
        elif isinstance(aspect, DashboardInfoClass):
            urn = Urn.create_from_string(entity_urn)
            self.add_edge(
                entity_urn,
                "name",
                aspect.title + f" ({urn.get_entity_id()[-1]})",
                remove_existing=True,
            )
        elif isinstance(aspect, TagPropertiesClass):
            self.add_edge(entity_urn, "name", aspect.name, remove_existing=True)
