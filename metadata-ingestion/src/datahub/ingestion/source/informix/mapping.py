from typing import List, Optional

from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
    make_user_urn,
)
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.informix.config import InformixSourceConfig
from datahub.ingestion.source.informix.constants import PLATFORM, map_coltype
from datahub.ingestion.source.informix.models import InformixColumn, InformixForeignKey
from datahub.metadata.schema_classes import (
    ForeignKeyConstraintClass,
    NullTypeClass,
    OwnerClass,
    OwnershipTypeClass,
    SchemaFieldClass,
)

# Numeric/DECIMAL types pack precision+scale into collength rather than a
# plain character length, so length is only meaningful for these string types.
_LENGTH_TYPES = {"CHAR", "VARCHAR", "NCHAR", "NVARCHAR", "LVARCHAR"}

# VARCHAR/NVARCHAR go further and pack the whole declaration into collength as
# (reserved_min_space * 256) + max_size. collength is a signed SMALLINT, so a
# reserved minimum of 128 or more wraps it negative. Measured on Informix
# 15.0.1: VARCHAR(100) -> 100, VARCHAR(100,10) -> 2660, VARCHAR(200,150) ->
# -26936, NVARCHAR(80,20) -> 5200. CHAR/NCHAR/LVARCHAR store the length as-is.
# See the IBM Informix SQL Reference (SYSCOLUMNS).
_PACKED_LENGTH_TYPES = {"VARCHAR", "NVARCHAR"}


def _native_with_length(native: str, collength: int) -> str:
    if native not in _LENGTH_TYPES:
        return native
    # Only the low byte (the declared maximum) of a packed collength is
    # reported; the reserved minimum is a storage hint, not part of the column's
    # size contract. Python's % already normalizes the negative wrap, so the
    # signed SMALLINT needs no separate unwrapping step.
    length = collength % 256 if native in _PACKED_LENGTH_TYPES else collength
    return f"{native}({length})" if length > 0 else native


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


def make_table_identifier(
    database: str, owner: str, table: str, convert_to_lowercase: bool = False
) -> str:
    identifier = f"{database}.{owner}.{table}"
    return identifier.lower() if convert_to_lowercase else identifier


def build_owners(owner: str) -> List[OwnerClass]:
    # systables.owner is the database user that created the object, which is the
    # closest thing Informix records to an owner -- there is no group or team
    # concept to disambiguate against, so it always maps to a corpuser.
    # DATAOWNER matches how the other SQL sources classify a catalog-derived owner.
    return [OwnerClass(owner=make_user_urn(owner), type=OwnershipTypeClass.DATAOWNER)]


def columns_to_schema_fields(
    columns: List[InformixColumn], report: SourceReport
) -> List[SchemaFieldClass]:
    fields: List[SchemaFieldClass] = []
    for col in columns:
        mapped = map_coltype(coltype=col.coltype, extended=col.extended)
        native = _native_with_length(mapped.native, col.length)
        if isinstance(mapped.data_type.type, NullTypeClass):
            # Checked on the resolved DataHub type rather than an "UNKNOWN" name
            # prefix: an opaque type recovered from sysxtdtypes has a real name
            # but still no DataHub equivalent, and is worth reporting.
            report.warning(
                title="Unmapped Informix column type",
                message="Column type has no DataHub mapping; using NullType.",
                context=f"{col.name} coltype={col.coltype} native={native}",
            )
        fields.append(
            SchemaFieldClass(
                fieldPath=col.name,
                type=mapped.data_type,
                nativeDataType=native,
                nullable=mapped.nullable,
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
    convert_to_lowercase: bool = False,
) -> List[ForeignKeyConstraintClass]:
    constraints: List[ForeignKeyConstraintClass] = []
    for fk in fks:
        parent_urn = make_dataset_urn_with_platform_instance(
            platform=PLATFORM,
            name=make_table_identifier(
                database, fk.parent_owner, fk.parent_table, convert_to_lowercase
            ),
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
