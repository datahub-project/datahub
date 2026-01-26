import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from pydantic.fields import Field
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.reflection import Inspector

from datahub.configuration.common import AllowDenyPattern, HiddenFromDocs
from datahub.emitter.mcp_builder import CatalogKey, gen_containers
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import SourceCapabilityModifier
from datahub.ingestion.source.sql.sql_common import SQLAlchemySource
from datahub.ingestion.source.sql.sql_config import (
    BasicSQLAlchemyConfig,
    SQLAlchemyConnectionConfig,
)
from datahub.ingestion.source.sql.sql_utils import (
    gen_database_key,
)

logger = logging.getLogger(__name__)


@dataclass
class StarRocksCatalog:
    """Represents a StarRocks catalog."""

    name: str
    catalog_type: str  # 'Internal' or external type like 'hive', 'iceberg', etc.

    @property
    def is_external(self) -> bool:
        return self.catalog_type.lower() != "internal"


class StarRocksConnectionConfig(SQLAlchemyConnectionConfig):
    """Connection configuration for StarRocks."""

    host_port: str = Field(
        default="localhost:9030",
        description="StarRocks FE host URL.",
    )
    scheme: HiddenFromDocs[str] = Field(
        default="starrocks",
        description="SQLAlchemy scheme for StarRocks connection.",
    )


class StarRocksConfig(StarRocksConnectionConfig, BasicSQLAlchemyConfig):
    """
    Configuration for StarRocks metadata ingestion.

    Supports multi-catalog discovery including external catalogs (three-tier hierarchy: Catalog -> Database -> Table).
    Note: In DataHub's three-tier model, StarRocks catalogs are 'databases' and StarRocks databases are 'schemas'.
    """

    database: Optional[str] = Field(
        default=None,
        description="Database in catalog.database format (e.g., 'default_catalog.quickstart'). "
        "If specified, only this database will be ingested. "
        "Leave unset for multi-catalog discovery.",
    )

    # Override inherited schema_pattern to clarify it filters StarRocks databases
    schema_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for databases to filter in ingestion. "
        "Note: In StarRocks three-tier hierarchy (Catalog -> Database -> Table), this filters databases within catalogs. "
        "Specify regex to match the database name only (e.g., 'quickstart', not 'default_catalog.quickstart').",
    )

    # Catalog configuration
    include_external_catalogs: bool = Field(
        default=True,
        description="Whether to include external catalogs (Hive, Iceberg, etc.) in ingestion.",
    )

    catalog_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for catalogs to filter in ingestion. "
        "Specify regex to match the catalog name.",
    )


@platform_name("StarRocks")
@config_class(StarRocksConfig)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(
    SourceCapability.CONTAINERS,
    "Enabled by default",
    subtype_modifier=[
        SourceCapabilityModifier.CATALOG,
        SourceCapabilityModifier.DATABASE,
    ],
)
class StarRocksSource(SQLAlchemySource):
    """
    DataHub ingestion source for StarRocks with multi-catalog support.

    This plugin implements three-tier discovery: Catalog -> Database -> Table.
    It supports both internal catalogs and external catalogs,
    extracting metadata for databases and tables.
    """

    config: StarRocksConfig

    def __init__(self, config: StarRocksConfig, ctx: PipelineContext):
        super().__init__(config, ctx, "starrocks")
        self._current_catalog: Optional[StarRocksCatalog] = None
        self._catalog_containers_emitted: set[str] = set()

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "StarRocksSource":
        config = StarRocksConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_inspectors(self) -> Iterable[Inspector]:
        """
        Yield inspectors for each catalog/database combination.

        Implements three-layer discovery: Catalog → Database → Table.
        """
        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url:{url}")
        engine = create_engine(url, **self.config.options)

        catalogs = self._discover_catalogs(engine)

        for catalog in catalogs:
            if catalog.is_external and not self.config.include_external_catalogs:
                logger.debug(
                    f"Skipping external catalog {catalog.name} "
                    "(include_external_catalogs=False)"
                )
                continue
            if not self.config.catalog_pattern.allowed(catalog.name):
                logger.debug(
                    f"Skipping catalog {catalog.name} - not in catalog_pattern"
                )
                continue

            self._current_catalog = catalog
            databases = self._discover_databases(engine, catalog)

            logger.info(
                f"Processing {len(databases)} databases in catalog {catalog.name}"
            )

            for db_name in databases:
                if not self.config.schema_pattern.allowed(db_name):
                    continue
                logger.debug(f"Processing: {catalog.name}.{db_name}")

                # StarRocks SQLAlchemy dialect uses catalog.database format
                db_url = self.config.get_sql_alchemy_url(
                    database=f"{catalog.name}.{db_name}"
                )

                try:
                    db_engine = create_engine(db_url, **self.config.options)
                    with db_engine.connect() as conn:
                        inspector = inspect(conn)
                        yield inspector
                except Exception as e:
                    logger.warning(
                        f"Failed to connect to {catalog.name}.{db_name}: {e}"
                    )
                    continue

    """
    Catalog discovery and all resulting metadata discovery is
    not currently supported through starrocks-sqlalchemy v1.3.3.
    We implement direct SQL-based discovery using SHOW CATALOGS and SHOW DATABASES.
    """

    def _discover_catalogs(self, engine: Engine) -> List[StarRocksCatalog]:
        """
        Discover catalogs using SHOW CATALOGS.

        Returns a list of all discovered StarRocksCatalog objects.
        """
        catalogs: List[StarRocksCatalog] = []

        try:
            with engine.connect() as conn:
                result = conn.execute(text("SHOW CATALOGS"))
                for row in result:
                    # SHOW CATALOGS returns: Catalog, Type, Comment
                    catalog_name = row[0]
                    catalog_type = row[1] if len(row) > 1 else "Internal"

                    catalog = StarRocksCatalog(
                        name=catalog_name,
                        catalog_type=catalog_type,
                    )
                    catalogs.append(catalog)
                    logger.debug(
                        f"Discovered catalog: {catalog_name} (type: {catalog_type})"
                    )

        except Exception as e:
            logger.warning(f"Failed to discover catalogs via SHOW CATALOGS: {e}")
            # Fall back to default_catalog if catalog discovery fails
            default_catalog = StarRocksCatalog(
                name="default_catalog", catalog_type="Internal"
            )
            catalogs.append(default_catalog)

        return catalogs

    def _discover_databases(
        self, engine: Engine, catalog: StarRocksCatalog
    ) -> List[str]:
        """
        Discover databases within a catalog.
        """
        databases: List[str] = []

        try:
            with engine.connect() as conn:
                conn.execute(text(f"SET CATALOG `{catalog.name}`"))
                result = conn.execute(text("SHOW DATABASES"))
                for row in result:
                    db_name = row[0]
                    databases.append(db_name)
                    logger.debug(f"Discovered database: {catalog.name}.{db_name}")

        except Exception as e:
            logger.warning(
                f"Failed to discover databases in catalog {catalog.name}: {e}"
            )

        return databases

    def gen_catalog_key(self, catalog_name: str) -> CatalogKey:
        """Generate a container key for a catalog."""
        return CatalogKey(
            catalog=catalog_name,
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
            backcompat_env_as_instance=True,
        )

    def gen_catalog_containers(
        self, catalog: StarRocksCatalog
    ) -> Iterable[MetadataWorkUnit]:
        """Generate container workunits for a catalog."""
        catalog_key = self.gen_catalog_key(catalog.name)

        yield from gen_containers(
            container_key=catalog_key,
            name=catalog.name,
            sub_types=["Catalog"],
            description=f"StarRocks {catalog.catalog_type} catalog",
            extra_properties={"catalog_type": catalog.catalog_type},
        )

    def get_identifier(
        self, *, schema: str, entity: str, inspector: Inspector, **kwargs: Any
    ) -> str:
        """
        Get the dataset identifier with catalog prefix.

        For StarRocks: catalog.database.table
        """
        catalog_name = (
            self._current_catalog.name if self._current_catalog else "default_catalog"
        )
        return f"{catalog_name}.{schema}.{entity}"

    def get_db_name(self, inspector: Inspector) -> str:
        """
        Get the database identifier from the inspector.

        Returns the full catalog.database identifier for proper container hierarchy.
        """
        engine = inspector.engine
        if engine and hasattr(engine, "url") and hasattr(engine.url, "database"):
            db = str(engine.url.database).strip('"')
            return db
        raise Exception("Unable to get database name from SQLAlchemy inspector")

    def get_db_schema(self, dataset_identifier: str) -> Tuple[Optional[str], str]:
        """
        Parse dataset identifier into (database, schema).

        Returns: (catalog, database)
        """
        # dataset_identifier is catalog.database.table
        parts = dataset_identifier.split(".", 2)
        if len(parts) >= 2:
            return parts[0], parts[1]  # (catalog, database)
        return None, dataset_identifier

    def get_allowed_schemas(self, inspector: Inspector, db_name: str) -> Iterable[str]:
        """
        Return the database portion as the schema.
        """
        # db_name is "catalog.database", extract just the database portion
        if "." in db_name:
            _, database = db_name.split(".", 1)
            yield database
        else:
            yield db_name

    def gen_schema_containers(
        self,
        schema: str,
        database: str,
        extra_properties: Optional[Dict[str, Any]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Skip schema container generation.

        StarRocks has a two-level container hierarchy (catalog -> database),
        not three levels. The "schema" in base class terms is actually our database.
        """
        return []

    def add_table_to_schema_container(
        self,
        dataset_urn: str,
        db_name: str,
        schema: str,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Add table to database container.

        StarRocks has two-tier hierarchy, so tables go directly into database containers.
        db_name is "catalog.database" and schema is "database" portion.
        """
        from datahub.ingestion.source.sql.sql_utils import (
            add_table_to_schema_container,
        )

        # For StarRocks, db_name is already "catalog.database" which is what we want
        database_container_key = gen_database_key(
            database=db_name,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

        yield from add_table_to_schema_container(
            dataset_urn=dataset_urn,
            parent_container_key=database_container_key,
        )

    def get_database_level_workunits(
        self, inspector: Inspector, database: str
    ) -> Iterable[MetadataWorkUnit]:
        """
        Generate catalog and database containers.

        Overrides base class to inject catalog container generation.
        database parameter is "catalog.database" format.
        """
        if self._current_catalog:
            catalog_name = self._current_catalog.name
            if catalog_name not in self._catalog_containers_emitted:  # once per catalog
                yield from self.gen_catalog_containers(self._current_catalog)
                self._catalog_containers_emitted.add(catalog_name)

        yield from super().get_database_level_workunits(
            inspector=inspector, database=database
        )

    def gen_database_containers(
        self,
        database: str,
        extra_properties: Optional[Dict[str, Any]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Generate database container with catalog as parent.

        database parameter is "catalog.database" format.
        """
        from datahub.emitter.mcp_builder import gen_containers
        from datahub.ingestion.source.sql.sql_utils import gen_domain_urn

        catalog_name, db_name = (
            database.split(".", 1) if "." in database else ("default_catalog", database)
        )

        database_container_key = gen_database_key(
            database,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

        catalog_parent_key = self.gen_catalog_key(catalog_name)

        domain_urn: Optional[str] = None
        if self.domain_registry:
            domain_urn = gen_domain_urn(
                database,
                domain_config=self.config.domain,
                domain_registry=self.domain_registry,
            )

        # Use db_name (without catalog prefix) for display name
        yield from gen_containers(
            container_key=database_container_key,
            name=db_name,
            sub_types=["Database"],
            parent_container_key=catalog_parent_key,
            domain_urn=domain_urn,
            extra_properties=extra_properties,
        )

    def get_table_properties(
        self, inspector: Inspector, schema: str, table: str
    ) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
        """
        Get table properties with catalog information.
        """
        description, properties, location = super().get_table_properties(
            inspector, schema, table
        )

        if self._current_catalog:
            properties["catalog"] = self._current_catalog.name
            properties["catalog_type"] = self._current_catalog.catalog_type

        return description, properties, location
