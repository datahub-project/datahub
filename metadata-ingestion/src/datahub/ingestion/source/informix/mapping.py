from typing import List

from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.informix.config import InformixSourceConfig
from datahub.ingestion.source.informix.constants import map_coltype
from datahub.ingestion.source.informix.models import InformixColumn
from datahub.metadata.schema_classes import SchemaFieldClass

# Numeric/DECIMAL types pack precision+scale into collength rather than a
# plain character length, so length is only meaningful for these string types.
_LENGTH_TYPES = {"CHAR", "VARCHAR", "NCHAR", "NVARCHAR", "LVARCHAR"}


def build_jdbc_url(config: InformixSourceConfig) -> str:
    user = config.username or ""
    password = config.password.get_secret_value() if config.password is not None else ""
    url = (
        f"jdbc:informix-sqli://{config.host_port}/{config.database}:"
        f"INFORMIXSERVER={config.server};user={user};password={password}"
    )
    if config.extra_props:
        url = f"{url};{config.extra_props.strip(';')}"
    return url


def make_table_identifier(database: str, owner: str, table: str) -> str:
    return f"{database}.{owner}.{table}"


def columns_to_schema_fields(
    columns: List[InformixColumn], report: SourceReport
) -> List[SchemaFieldClass]:
    fields: List[SchemaFieldClass] = []
    for col in columns:
        dh_type, nullable, native = map_coltype(col.coltype)
        if native in _LENGTH_TYPES and col.length > 0:
            native = f"{native}({col.length})"
        if native.startswith("UNKNOWN"):
            report.warning(
                title="Unmapped Informix column type",
                message="Column type has no DataHub mapping; using NullType.",
                context=f"{col.name} coltype={col.coltype}",
            )
        fields.append(
            SchemaFieldClass(
                fieldPath=col.name,
                type=dh_type,
                nativeDataType=native,
                nullable=nullable,
                isPartOfKey=col.is_pk,
            )
        )
    return fields
