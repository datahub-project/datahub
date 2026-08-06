import logging
from typing import Iterable, Optional, Set

from datahub.emitter.mcp_builder import (
    DatabaseKey,
    SchemaKey,
    gen_containers,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sqlmesh.base import SqlmeshSourceBase
from datahub.ingestion.source.sqlmesh.constants import (
    SQLMESH_PLATFORM,
    SUBTYPE_DATABASE,
    SUBTYPE_SCHEMA,
)
from datahub.ingestion.source.sqlmesh.models import _EffectiveProjectConfig

logger = logging.getLogger(__name__)


class ContainerMixin(SqlmeshSourceBase):
    def _emit_containers(
        self, fqns: Set[str], effective: _EffectiveProjectConfig
    ) -> Iterable[MetadataWorkUnit]:
        seen_databases: Set[str] = set()
        seen_schemas: Set[str] = set()

        for fqn in sorted(fqns):
            parts = fqn.split(".")
            catalog: Optional[str]
            if len(parts) >= 3:
                catalog, schema = parts[0], parts[1]
            elif len(parts) == 2:
                catalog, schema = None, parts[0]
            else:
                continue  # 1-part name — no containers

            if catalog and catalog not in seen_databases:
                seen_databases.add(catalog)
                db_key = DatabaseKey(
                    platform=SQLMESH_PLATFORM,
                    instance=effective.sqlmesh_platform_instance,
                    env=self.config.env,
                    database=catalog,
                )
                yield from gen_containers(
                    container_key=db_key,
                    name=catalog,
                    sub_types=[SUBTYPE_DATABASE],
                )
                self.report.num_containers_emitted += 1

            schema_key_str = f"{catalog}.{schema}" if catalog else schema
            if schema_key_str not in seen_schemas:
                self.report.num_containers_emitted += 1
                seen_schemas.add(schema_key_str)
                if catalog:
                    db_key = DatabaseKey(
                        platform=SQLMESH_PLATFORM,
                        instance=effective.sqlmesh_platform_instance,
                        env=self.config.env,
                        database=catalog,
                    )
                    schema_key = SchemaKey(
                        platform=SQLMESH_PLATFORM,
                        instance=effective.sqlmesh_platform_instance,
                        env=self.config.env,
                        database=catalog,
                        schema=schema,
                    )
                    yield from gen_containers(
                        container_key=schema_key,
                        name=schema,
                        sub_types=[SUBTYPE_SCHEMA],
                        parent_container_key=db_key,
                    )
                else:
                    schema_key = SchemaKey(
                        platform=SQLMESH_PLATFORM,
                        instance=effective.sqlmesh_platform_instance,
                        env=self.config.env,
                        database="",
                        schema=schema,
                    )
                    yield from gen_containers(
                        container_key=schema_key,
                        name=schema,
                        sub_types=[SUBTYPE_SCHEMA],
                    )
