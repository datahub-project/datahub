"""Shared fixtures and builders for the Snowflake unit tests.

Two things live here:

* the ``DATAHUB_TEST_PRESERVE_COLUMN_CASE`` sweep (an autouse session fixture);
* plain builders for the connector's collaborators and dataclasses, imported by
  the test modules (``from tests.unit.snowflake.conftest import ...``) so that no
  test module doubles as a fixture library for another.

Mocks of DataHub-internal collaborators (``SnowflakeConnection``,
``SnowflakeDataDictionary``, ``SqlParsingAggregator``, the skip handlers, ...)
are built with ``create_autospec`` so a renamed method fails loudly instead of
being auto-created. The snowflake-connector cursor/connection are external and
stay plain ``MagicMock``s.
"""

import base64
import os
from datetime import datetime, timedelta, timezone
from typing import (
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)
from unittest.mock import MagicMock, NonCallableMock, create_autospec, patch

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import StructuredLogEntry
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake.constants import SnowflakeEdition
from datahub.ingestion.source.snowflake.snowflake_config import (
    SnowflakeConfig,
    SnowflakeIdentifierConfig,
    SnowflakeV2Config,
)
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakeConnection,
)
from datahub.ingestion.source.snowflake.snowflake_lineage_v2 import (
    SnowflakeLineageExtractor,
)
from datahub.ingestion.source.snowflake.snowflake_queries import (
    SnowflakeQueriesSourceConfig,
)
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeColumn,
    SnowflakeDatabase,
    SnowflakeDataDictionary,
    SnowflakeDynamicTable,
    SnowflakeDynamicTableInput,
    SnowflakeSchema,
    SnowflakeSemanticView,
    SnowflakeStream,
    SnowflakeStreamlitApp,
    SnowflakeTable,
    SnowflakeTag,
    SnowflakeView,
)
from datahub.ingestion.source.snowflake.snowflake_schema_gen import (
    SnowflakeSchemaGenerator,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeFilter,
    SnowflakeIdentifierBuilder,
)
from datahub.ingestion.source.snowflake.snowflake_v2 import SnowflakeV2Source
from datahub.ingestion.source.sql.stored_procedures.models import BaseProcedure
from datahub.metadata.schema_classes import _Aspect
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator

# --- preserve_column_case sweep -----------------------------------------------

# An opt-in flag gets the whole existing suite as a regression net for its OFF
# path and nothing for its ON path. Setting DATAHUB_TEST_PRESERVE_COLUMN_CASE=1
# flips the default so every Snowflake test runs with the flag enabled, which is
# what surfaces aspects that disagree with each other on a column's field path.
#
#   DATAHUB_TEST_PRESERVE_COLUMN_CASE=1 pytest tests/unit/snowflake/
#
# Tests that set preserve_column_case explicitly are unaffected.
PRESERVE_COLUMN_CASE_SWEEP = "DATAHUB_TEST_PRESERVE_COLUMN_CASE"


# Every class the suite builds that carries the flag. Pydantic compiles each
# model's defaults at class definition, so each needs its own rebuild — flipping
# only the base is a silent no-op.
#
# SnowflakeQueriesExtractorConfig is deliberately absent: it derives from
# ConfigModel, not SnowflakeIdentifierConfig, and carries no identifier fields.
# Only SnowflakeQueriesSourceConfig mixes the two in.
_CONFIG_MODELS = (
    SnowflakeIdentifierConfig,
    SnowflakeConfig,
    SnowflakeV2Config,
    SnowflakeQueriesSourceConfig,
)


@pytest.fixture(autouse=True, scope="session")
def _preserve_column_case_sweep() -> Iterator[None]:
    if os.environ.get(PRESERVE_COLUMN_CASE_SWEEP) != "1":
        yield
        return

    for model in _CONFIG_MODELS:
        field = model.model_fields.get("preserve_column_case")
        # Catches a rename or removal, which would otherwise make the sweep a
        # no-op for that class without anyone noticing.
        assert field is not None, f"{model.__name__} no longer carries the flag"
        field.default = True
        model.model_rebuild(force=True)

    yield

    # These are class-level defaults, so without this the flip leaks into every
    # Snowflake config built for the rest of the session -- including tests outside
    # this directory, when someone runs a wider selection with the env var set.
    for model in _CONFIG_MODELS:
        model.model_fields["preserve_column_case"].default = False
        model.model_rebuild(force=True)


# --- Constants ----------------------------------------------------------------

DB = "TEST_DB"
SCHEMA = "PUBLIC"
CREATED = datetime(2023, 1, 15, 10, 30, tzinfo=timezone.utc)
START = datetime(2022, 6, 6, tzinfo=timezone.utc)
END = datetime(2022, 6, 8, tzinfo=timezone.utc)

# The common preamble of every SnowflakeV2Config built by the suite.
BASE_CONFIG: Dict[str, object] = {
    "account_id": "test_account",
    "username": "user",
    "password": "pass",
}

QueryResult = Union[Sequence[dict], Exception]
QueryHandler = Callable[[str], QueryResult]

A = TypeVar("A", bound=_Aspect)


# --- Generic helpers ----------------------------------------------------------


def as_mock(obj: object) -> NonCallableMock:
    """Narrow a collaborator the production type annotates as real to its mock.

    Autospecced instances are ``NonCallableMagicMock``s, so the check is on the
    common base; their methods are ordinary ``MagicMock``s.
    """
    assert isinstance(obj, NonCallableMock)
    return obj


def log_keys(entries: Iterable[StructuredLogEntry]) -> Set[str]:
    """The stable identity of each report entry.

    Structured logs are keyed by title when one was given and by message
    otherwise (``report.warning("Failed to ...", context)`` sets no title), so
    tests assert membership on that key rather than on counts or ordering.
    """
    return {entry.title or entry.message for entry in entries}


def aspects(
    workunits: Iterable[MetadataWorkUnit], aspect_type: Type[A]
) -> Dict[str, A]:
    found: Dict[str, A] = {}
    for wu in workunits:
        aspect = wu.get_aspect_of_type(aspect_type)
        if aspect is not None:
            found[wu.get_urn()] = aspect
    return found


def make_v2_config(**overrides: object) -> SnowflakeV2Config:
    return SnowflakeV2Config.model_validate({**BASE_CONFIG, **overrides})


# --- Connections --------------------------------------------------------------


class FakeCursor(list):
    """A result set with the cursor attributes the extractors read."""

    rownumber: Optional[int] = None

    @property
    def rowcount(self) -> int:
        return len(self)


def fake_connection(handler: QueryHandler) -> NonCallableMock:
    """A ``SnowflakeConnection`` whose ``query`` is answered by ``handler``.

    Returning an exception from the handler raises it from ``query``.
    """
    connection = create_autospec(SnowflakeConnection, instance=True)

    def query(sql: str) -> FakeCursor:
        result = handler(sql)
        if isinstance(result, Exception):
            raise result
        return FakeCursor(result)

    connection.query.side_effect = query
    return connection


def fake_native_connection(execute: Callable[[str], object]) -> MagicMock:
    """The snowflake-connector connection: ``cursor().execute(sql)`` runs ``execute``."""
    native = MagicMock()
    native.cursor.return_value.execute.side_effect = execute
    return native


def make_aggregator() -> NonCallableMock:
    return create_autospec(SqlParsingAggregator, instance=True)


# --- Dataclass builders -------------------------------------------------------


def column(name: str, position: int = 1) -> SnowflakeColumn:
    return SnowflakeColumn(
        name=name,
        ordinal_position=position,
        is_nullable=True,
        data_type="VARCHAR",
        comment=None,
        character_maximum_length=None,
        numeric_precision=None,
        numeric_scale=None,
    )


def table(name: str, **overrides: object) -> SnowflakeTable:
    return SnowflakeTable(
        name=name,
        comment=None,
        created=CREATED,
        last_altered=None,
        size_in_bytes=None,
        rows_count=None,
        **overrides,  # type: ignore[arg-type]
    )


def dynamic_table(
    name: str, definition: str, inputs: Sequence[str] = ()
) -> SnowflakeDynamicTable:
    return SnowflakeDynamicTable(
        name=name,
        comment=None,
        created=CREATED,
        last_altered=None,
        size_in_bytes=None,
        rows_count=None,
        definition=definition,
        upstream_tables=[
            SnowflakeDynamicTableInput(name=i, kind="TABLE") for i in inputs
        ],
    )


def view(name: str, **overrides: object) -> SnowflakeView:
    return SnowflakeView(
        name=name,
        comment=None,
        created=CREATED,
        last_altered=None,
        view_definition="select 1",
        **overrides,  # type: ignore[arg-type]
    )


def semantic_view(name: str, **overrides: object) -> SnowflakeSemanticView:
    return SnowflakeSemanticView(
        name=name,
        comment=None,
        created=CREATED,
        last_altered=None,
        view_definition="create semantic view ...",
        **overrides,  # type: ignore[arg-type]
    )


def schema(name: str = SCHEMA) -> SnowflakeSchema:
    return SnowflakeSchema(name=name, created=CREATED, last_altered=None, comment=None)


def database(name: str = DB) -> SnowflakeDatabase:
    return SnowflakeDatabase(name=name, created=CREATED, comment=None)


def stream(name: str, source: str) -> SnowflakeStream:
    return SnowflakeStream(
        name=name,
        created=CREATED,
        owner="OWNER",
        source_type="Table",
        type="DELTA",
        stale="false",
        mode="DEFAULT",
        invalid_reason="",
        owner_role_type="ROLE",
        database_name=DB,
        schema_name=SCHEMA,
        table_name=f"{DB}.{SCHEMA}.{source}",
        comment=None,
        stale_after=CREATED,
    )


def streamlit_app(name: str) -> SnowflakeStreamlitApp:
    return SnowflakeStreamlitApp(
        name=name,
        created=CREATED,
        owner="OWNER",
        database_name=DB,
        schema_name=SCHEMA,
        title=None,
        comment="an app",
        url_id="abc123",
        owner_role_type="ROLE",
    )


def procedure(name: str) -> BaseProcedure:
    return BaseProcedure(
        name=name,
        procedure_definition="begin return 1; end",
        created=None,
        last_altered=None,
        comment=None,
        argument_signature="()",
        return_type="NUMBER",
        language="SQL",
        extra_properties=None,
    )


def tag(name: str = "PII", value: str = "yes") -> SnowflakeTag:
    return SnowflakeTag(database=DB, schema=SCHEMA, name=name, value=value)


# --- Collaborator builders ----------------------------------------------------


def make_gen(
    aggregator: Optional[NonCallableMock] = None, **config_overrides: object
) -> SnowflakeSchemaGenerator:
    """A ``SnowflakeSchemaGenerator`` over an autospecced data dictionary.

    The data dictionary is the query layer; its row parsing is covered by
    test_snowflake_data_dictionary.py, so here it answers with canned objects
    (empty by default). ``preserve_column_case`` is pinned off because the
    tests built on this assert lowercased field paths; see the sweep above.
    """
    config = make_v2_config(
        **{
            "include_streams": False,
            "include_procedures": False,
            "include_streamlits": False,
            "preserve_column_case": False,
            **config_overrides,
        }
    )
    report = SnowflakeV2Report()
    gen = SnowflakeSchemaGenerator(
        config=config,
        report=report,
        connection=fake_connection(lambda _sql: []),
        filters=SnowflakeFilter(filter_config=config, structured_reporter=report),
        identifiers=SnowflakeIdentifierBuilder(
            identifier_config=config, structured_reporter=report
        ),
        domain_registry=None,
        profiler=None,
        aggregator=aggregator,
        snowsight_url_builder=None,
    )
    data_dictionary = create_autospec(SnowflakeDataDictionary, instance=True)
    data_dictionary.get_pk_constraints_for_schema.return_value = {}
    data_dictionary.get_fk_constraints_for_schema.return_value = {}
    data_dictionary.get_columns_for_schema.return_value = {}
    gen.data_dictionary = data_dictionary
    gen.tag_extractor.data_dictionary = data_dictionary
    return gen


def dataset_urn(
    owner: Union[SnowflakeSchemaGenerator, SnowflakeLineageExtractor], name: str
) -> str:
    """The URN ``owner`` would emit for ``DB.SCHEMA.<name>``."""
    return owner.identifiers.gen_dataset_urn(f"{DB}.{SCHEMA}.{name}".lower())


def make_lineage_extractor(
    handler: QueryHandler,
    *,
    edition: SnowflakeEdition = SnowflakeEdition.ENTERPRISE,
    **config_overrides: object,
) -> SnowflakeLineageExtractor:
    config = make_v2_config(
        **{
            "start_time": START,
            "end_time": END,
            # Pinned: the lineage tests assert lowercased column names.
            "preserve_column_case": False,
            **config_overrides,
        }
    )
    report = SnowflakeV2Report()
    report.edition = edition
    return SnowflakeLineageExtractor(
        config=config,
        report=report,
        connection=fake_connection(handler),
        filters=SnowflakeFilter(filter_config=config, structured_reporter=report),
        identifiers=SnowflakeIdentifierBuilder(
            identifier_config=config, structured_reporter=report
        ),
        redundant_run_skip_handler=None,
        sql_aggregator=make_aggregator(),
    )


SESSION_ROWS: Dict[str, List[dict]] = {
    SnowflakeQuery.current_version(): [{"CURRENT_VERSION()": "8.1.0"}],
    SnowflakeQuery.current_role(): [{"CURRENT_ROLE()": "TEST_ROLE"}],
    SnowflakeQuery.current_warehouse(): [{"CURRENT_WAREHOUSE()": "TEST_WH"}],
    SnowflakeQuery.show_tags(): [],
    SnowflakeQuery.current_account(): [{"CURRENT_ACCOUNT()": "ABC123"}],
    SnowflakeQuery.current_region(): [{"CURRENT_REGION()": "PUBLIC.AWS_US_WEST_2"}],
}


def make_source(
    overrides: Optional[Dict[str, QueryResult]] = None, **config_overrides: object
) -> SnowflakeV2Source:
    """A ``SnowflakeV2Source`` whose native connection answers exact query strings.

    ``SESSION_ROWS`` covers the session probes; ``overrides`` adds or replaces
    entries. Anything else raises so an unexpected query fails the test.
    """
    rows: Dict[str, QueryResult] = {**SESSION_ROWS, **(overrides or {})}

    def execute(query: str) -> FakeCursor:
        if query not in rows:
            raise AssertionError(f"Unexpected query: {query}")
        result = rows[query]
        if isinstance(result, Exception):
            raise result
        return FakeCursor(result)

    config = SnowflakeV2Config.model_validate(
        {
            "account_id": "test_account",
            "username": "user",
            # Password auth adds a deprecation warning to the report, which the
            # tests built on this would otherwise have to filter out.
            "authentication_type": "EXTERNAL_BROWSER_AUTHENTICATOR",
            "use_queries_v2": False,
            "include_table_lineage": False,
            "include_usage_stats": False,
            "include_operational_stats": False,
            "include_assertion_results": False,
            **config_overrides,
        }
    )
    with patch(
        "snowflake.connector.connect", return_value=fake_native_connection(execute)
    ):
        return SnowflakeV2Source(ctx=PipelineContext(run_id="test"), config=config)


# --- Key material -------------------------------------------------------------


def rsa_private_key_pem(password: Optional[bytes] = None) -> bytes:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    encryption: serialization.KeySerializationEncryption = (
        serialization.BestAvailableEncryption(password)
        if password
        else serialization.NoEncryption()
    )
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=encryption,
    )


def self_signed_certificate() -> Tuple[bytes, bytes, bytes]:
    """(private key PEM, certificate PEM, certificate DER) for a throwaway cert."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "datahub-test")])
    now = datetime.now(timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(1)
        .not_valid_before(now)
        .not_valid_after(now + timedelta(hours=1))
        .sign(key, hashes.SHA256())
    )
    private_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return (
        private_pem,
        cert.public_bytes(serialization.Encoding.PEM),
        cert.public_bytes(serialization.Encoding.DER),
    )


def b64(raw: bytes) -> str:
    return base64.b64encode(raw).decode()
