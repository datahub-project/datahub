import json
import logging
import re
from typing import Any, Dict, List, Optional

from pydantic.class_validators import validator
from pydantic.fields import Field

# This import verifies that the dependencies are available.
from pyhive import hive  # noqa: F401
from pyhive.sqlalchemy_hive import HiveDate, HiveDecimal, HiveTimestamp

from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.extractor import schema_util
from datahub.ingestion.source.sql.sql_common import (
    BasicSQLAlchemyConfig,
    SQLAlchemySource,
    register_custom_type,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    SchemaField,
    TimeTypeClass,
)
from datahub.utilities import config_clean
from datahub.utilities.hive_schema_to_avro import get_avro_schema_for_hive_column

logger = logging.getLogger(__name__)

register_custom_type(HiveDate, DateTypeClass)
register_custom_type(HiveTimestamp, TimeTypeClass)
register_custom_type(HiveDecimal, NumberTypeClass)

try:

    from databricks_dbapi.sqlalchemy_dialects.hive import DatabricksPyhiveDialect
    from pyhive.sqlalchemy_hive import _type_map
    from sqlalchemy import types, util
    from sqlalchemy.engine import reflection

    @reflection.cache  # type: ignore
    def dbapi_get_columns_patched(self, connection, table_name, schema=None, **kw):
        """Patches the get_columns method from dbapi (databricks_dbapi.sqlalchemy_dialects.base) to pass the native type through"""
        rows = self._get_table_columns(connection, table_name, schema)
        # Strip whitespace
        rows = [[col.strip() if col else None for col in row] for row in rows]
        # Filter out empty rows and comment
        rows = [row for row in rows if row[0] and row[0] != "# col_name"]
        result = []
        for (col_name, col_type, _comment) in rows:
            # Handle both oss hive and Databricks' hive partition header, respectively
            if col_name in ("# Partition Information", "# Partitioning"):
                break
            # Take out the more detailed type information
            # e.g. 'map<int,int>' -> 'map'
            #      'decimal(10,1)' -> decimal
            orig_col_type = col_type  # keep a copy
            col_type = re.search(r"^\w+", col_type).group(0)  # type: ignore
            try:
                coltype = _type_map[col_type]
            except KeyError:
                util.warn(
                    "Did not recognize type '%s' of column '%s'" % (col_type, col_name)
                )
                coltype = types.NullType  # type: ignore
            result.append(
                {
                    "name": col_name,
                    "type": coltype,
                    "nullable": True,
                    "default": None,
                    "full_type": orig_col_type,  # pass it through
                    "comment": _comment,
                }
            )
        return result

    DatabricksPyhiveDialect.get_columns = dbapi_get_columns_patched
except ModuleNotFoundError:
    pass
except Exception as e:
    logger.warning(f"Failed to patch method due to {e}")


class HiveConfig(BasicSQLAlchemyConfig):
    # defaults
    scheme = Field(default="hive", exclude=True)

    # Hive SQLAlchemy connector returns views as tables.
    # See https://github.com/dropbox/PyHive/blob/b21c507a24ed2f2b0cf15b0b6abb1c43f31d3ee0/pyhive/sqlalchemy_hive.py#L270-L273.
    # Disabling views helps us prevent this duplication.
    include_views = Field(
        default=False,
        exclude=True,
        description="Hive SQLAlchemy connector returns views as tables. See https://github.com/dropbox/PyHive/blob/b21c507a24ed2f2b0cf15b0b6abb1c43f31d3ee0/pyhive/sqlalchemy_hive.py#L270-L273. Disabling views helps us prevent this duplication.",
    )

    @validator("host_port")
    def clean_host_port(cls, v):
        return config_clean.remove_protocol(v)


@platform_name("Hive")
@config_class(HiveConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
class HiveSource(SQLAlchemySource):
    """
    This plugin extracts the following:

    - Metadata for databases, schemas, and tables
    - Column types associated with each table
    - Detailed table and storage information
    - Table, row, and column statistics via optional SQL profiling.

    """

    _COMPLEX_TYPE = re.compile("^(struct|map|array|uniontype)")

    def __init__(self, config, ctx):
        super().__init__(config, ctx, "hive")

    @classmethod
    def create(cls, config_dict, ctx):
        config = HiveConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_schema_names(self, inspector):
        assert isinstance(self.config, HiveConfig)
        # This condition restricts the ingestion to the specified database.
        if self.config.database:
            return [self.config.database]
        else:
            return super().get_schema_names(inspector)

    def get_schema_fields_for_column(
        self,
        dataset_name: str,
        column: Dict[Any, Any],
        pk_constraints: Optional[Dict[Any, Any]] = None,
        tags: Optional[List[str]] = None,
    ) -> List[SchemaField]:

        fields = super().get_schema_fields_for_column(
            dataset_name, column, pk_constraints
        )

        if self._COMPLEX_TYPE.match(fields[0].nativeDataType) and isinstance(
            fields[0].type.type, NullTypeClass
        ):
            assert len(fields) == 1
            field = fields[0]
            # Get avro schema for subfields along with parent complex field
            avro_schema = get_avro_schema_for_hive_column(
                column["name"], field.nativeDataType
            )

            new_fields = schema_util.avro_schema_to_mce_fields(
                json.dumps(avro_schema), default_nullable=True
            )

            # First field is the parent complex field
            new_fields[0].nullable = field.nullable
            new_fields[0].description = field.description
            new_fields[0].isPartOfKey = field.isPartOfKey
            return new_fields

        return fields
