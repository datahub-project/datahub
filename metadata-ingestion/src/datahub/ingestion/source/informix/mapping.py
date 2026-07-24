from typing import List, Optional

from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.informix.config import InformixSourceConfig
from datahub.ingestion.source.informix.constants import PLATFORM, map_coltype
from datahub.ingestion.source.informix.models import InformixColumn, InformixForeignKey
from datahub.metadata.schema_classes import ForeignKeyConstraintClass, SchemaFieldClass

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


def build_foreign_key_constraints(
    fks: List[InformixForeignKey],
    child_dataset_urn: str,
    database: str,
    env: str,
    platform_instance: Optional[str],
) -> List[ForeignKeyConstraintClass]:
    constraints: List[ForeignKeyConstraintClass] = []
    for fk in fks:
        parent_urn = make_dataset_urn_with_platform_instance(
            platform=PLATFORM,
            name=make_table_identifier(database, fk.parent_owner, fk.parent_table),
            platform_instance=platform_instance,
            env=env,
        )
        constraints.append(
            ForeignKeyConstraintClass(
                name=fk.name,
                foreignDataset=parent_urn,
                sourceFields=[
                    make_schema_field_urn(child_dataset_urn, col)
                    for col in fk.child_columns
                ],
                foreignFields=[
                    make_schema_field_urn(parent_urn, col) for col in fk.parent_columns
                ],
            )
        )
    return constraints
