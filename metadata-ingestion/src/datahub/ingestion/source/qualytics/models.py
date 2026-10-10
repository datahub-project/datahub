"""Pydantic models for the slice of the Qualytics API this connector reads.

Modelled from `tests/unit/qualytics/fixtures/openapi.json` and trimmed to the fields the
connector actually reads. A field nobody reads is not documentation: it is one more
way for a payload to fail validation and be dropped, so it is left out.

Three rules govern everything here, all driven by Qualytics being single-tenant with
per-deployment release versions:

1. ``extra="ignore"`` everywhere. A newer deployment adding fields must not break an
   older connector.
2. Never stricter than the spec. A field the spec leaves optional or nullable is
   optional or nullable here; ``tests/unit/qualytics/test_model_spec_alignment.py`` enforces
   both. Lists the spec allows to be null are read as empty.
3. Open-ended enums are typed ``str``, not ``Enum``. A rule type or status this build
   has never seen has to reach the mapper so it can degrade to a custom assertion and
   warn -- a ValidationError at parse time would drop the whole object instead. Only
   the two discriminators are constrained, and those are dispatched by hand with a
   documented fallback.
"""

from typing import Annotated, Any, Generic, Literal, TypeVar

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field

T = TypeVar("T")


def _none_as_empty(value: Any) -> Any:
    return [] if value is None else value


# A list the spec allows to be null. Read as empty rather than rejected: the
# difference between "no fields" and "null fields" means nothing to this connector,
# and rejecting it drops the object that carries it.
NullableList = Annotated[list[T], BeforeValidator(_none_as_empty)]


class _Model(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class Page(_Model, Generic[T]):
    """The fastapi-pagination envelope every Qualytics list endpoint returns.

    Strict on purpose, unlike everything else here: a changed envelope must fail
    loudly, because the alternative is a run that pages through nothing and reports
    success.
    """

    items: list[T]
    total: int
    page: int
    pages: int
    size: int


# --- Datastores -------------------------------------------------------------------


class Datastore(_Model):
    """Fields common to every datastore, and the fallback for an unknown store_type."""

    id: int
    name: str
    # Not Literal[...]: an unrecognised store_type must reach parse_datastore() so it
    # can warn and fall back, rather than failing validation here.
    store_type: str
    # Qualytics connection type -- "snowflake", "postgresql", "s3", ... The main
    # signal for inferring the DataHub platform.
    type: str


class JdbcDatastore(Datastore):
    store_type: Literal["jdbc"] = "jdbc"
    database: str | None = None
    # `schema` shadows a BaseModel attribute, so it is aliased.
    schema_: str | None = Field(default=None, alias="schema")


class DfsDatastore(Datastore):
    store_type: Literal["dfs"] = "dfs"
    uri: str
    root_path: str


class NativeDatastore(Datastore):
    store_type: Literal["native"] = "native"
    catalog: str
    schema_: str | None = Field(default=None, alias="schema")


# --- Containers -------------------------------------------------------------------


class Container(_Model):
    """Fields common to every container, and the fallback for an unknown type."""

    id: int
    name: str
    container_type: str


class TableContainer(Container):
    container_type: Literal["table"] = "table"


class FileContainer(Container):
    container_type: Literal["file"] = "file"
    relative_path: str


class ComputedTableContainer(Container):
    container_type: Literal["computed_table"] = "computed_table"


class ComputedFileContainer(Container):
    container_type: Literal["computed_file"] = "computed_file"


class ComputedJoinContainer(Container):
    container_type: Literal["computed_join"] = "computed_join"


# --- Profiles ---------------------------------------------------------------------


class HistogramBucket(_Model):
    value: str
    count: int


class ContainerProfile(_Model):
    id: int
    created: str
    records_count: int


class FieldProfile(_Model):
    name: str
    completeness: float | None = None
    approximate_distinct_values: float | None = None
    min: float | None = None
    max: float | None = None
    mean: float | None = None
    median: float | None = None
    std_dev: float | None = None
    q1: float | None = None
    q3: float | None = None
    histogram_buckets: NullableList[HistogramBucket] = Field(default_factory=list)


# --- Quality checks and anomalies -------------------------------------------------


class QualityCheckField(_Model):
    """A column a quality check covers.

    Only ``name``: the copy embedded in ``Anomaly.failed_checks[].quality_check`` is the
    spec's ``FieldStub``, whose sole required property is ``name``.
    """

    name: str


class QualityCheck(_Model):
    id: int
    # One of 49 values in this build. Deliberately str: an unrecognised rule type must
    # reach the assertion mapper, which turns it into a CUSTOM assertion and reports
    # it, rather than failing validation and disappearing.
    rule_type: str
    description: str | None = None
    coverage: float = 0.0
    filter: str | None = None
    properties: dict[str, Any] | None = None
    inferred: bool = False
    status: str | None = None
    is_passing: bool | None = None
    last_asserted: str | None = None
    active_anomaly_count: int = 0
    weight: float = 0.0
    fields: NullableList[QualityCheckField] = Field(default_factory=list)


class FailedCheck(_Model):
    quality_check: QualityCheck
    message: str
    suggested_value: str | None = None


class Anomaly(_Model):
    id: int
    uuid: str
    # "shape" | "record"
    type: str
    status: str
    created: str
    anomalous_records_count: int | None = None
    failed_checks: NullableList[FailedCheck] = Field(default_factory=list)


# --- Hand-rolled discriminator dispatch -------------------------------------------
#
# Pydantic discriminated unions reject an unknown discriminator value outright, which
# would drop a whole datastore or container the day Qualytics adds a store or container
# type. These dispatch on the discriminator and fall back to the base model so the
# caller can warn and keep going.

_DATASTORE_TYPES: dict[str, type[Datastore]] = {
    "jdbc": JdbcDatastore,
    "dfs": DfsDatastore,
    "native": NativeDatastore,
}

_CONTAINER_TYPES: dict[str, type[Container]] = {
    "table": TableContainer,
    "file": FileContainer,
    "computed_table": ComputedTableContainer,
    "computed_file": ComputedFileContainer,
    "computed_join": ComputedJoinContainer,
}


def parse_datastore(payload: dict[str, Any]) -> tuple[Datastore, bool]:
    """Parse a datastore payload. Returns (model, recognised).

    ``recognised`` is False when ``store_type`` is one this build does not know, in
    which case the base ``Datastore`` is returned so the caller can warn and skip
    rather than crash.
    """
    model = _DATASTORE_TYPES.get(str(payload.get("store_type", "")))
    if model is None:
        return Datastore.model_validate(payload), False
    return model.model_validate(payload), True


def parse_container(payload: dict[str, Any]) -> tuple[Container, bool]:
    """Parse a container payload. Returns (model, recognised). See parse_datastore."""
    model = _CONTAINER_TYPES.get(str(payload.get("container_type", "")))
    if model is None:
        return Container.model_validate(payload), False
    return model.model_validate(payload), True
