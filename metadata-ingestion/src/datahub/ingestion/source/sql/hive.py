import json
import re
from typing import Any, Dict, List, Optional

from pydantic.class_validators import validator

# This import verifies that the dependencies are available.
from pyhive import hive  # noqa: F401
from pyhive.sqlalchemy_hive import HiveDate, HiveDecimal, HiveTimestamp

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

register_custom_type(HiveDate, DateTypeClass)
register_custom_type(HiveTimestamp, TimeTypeClass)
register_custom_type(HiveDecimal, NumberTypeClass)


class HiveConfig(BasicSQLAlchemyConfig):
    # defaults
    scheme = "hive"

    # Hive SQLAlchemy connector returns views as tables.
    # See https://github.com/dropbox/PyHive/blob/b21c507a24ed2f2b0cf15b0b6abb1c43f31d3ee0/pyhive/sqlalchemy_hive.py#L270-L273.
    # Disabling views helps us prevent this duplication.
    include_views = False

    @validator("host_port")
    def clean_host_port(cls, v):
        return config_clean.remove_protocol(v)


class HiveSource(SQLAlchemySource):

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
