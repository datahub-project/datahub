import pytest
import sqlalchemy as sa
from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    Integer,
    MetaData,
    Table,
    Unicode,
)

from datahub.testing import mce_helpers
from datahub.testing.docker_utils import wait_for_port
from tests.test_helpers.click_helpers import run_datahub_cmd

# Unlike most SQL connector tests, we do NOT freeze time with time_machine: the
# YDB client relies on real wall-clock time for its connection handshake and
# fails when the clock is pinned to the past. The volatile systemMetadata fields
# (lastObserved/runId) are already excluded by the golden comparison defaults.
YDB_PORT = 2136


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/ydb"


def _seed_ydb() -> None:
    """Create a deterministic set of tables and a view under `datahub_it/`.

    YDB DDL must run as a scheme operation (outside a transaction). SQLAlchemy
    routes that path only for DDL constructs (where ``context.isddl`` is true),
    so we use ``metadata.create_all`` and ``sa.DDL`` rather than raw text.
    """
    engine = sa.create_engine("yql+ydb://localhost:2136/local")
    md = MetaData()
    Table(
        "datahub_it/users",
        md,
        Column("id", Integer, primary_key=True),
        Column("name", Unicode),
        Column("email", Unicode),
        Column("created_at", DateTime),
    )
    Table(
        "datahub_it/orders",
        md,
        Column("order_id", BigInteger, primary_key=True),
        Column("user_id", Integer),
        Column("ordered_on", Date),
    )
    md.drop_all(engine)
    md.create_all(engine)
    with engine.connect() as conn:
        conn.execute(sa.DDL("DROP VIEW IF EXISTS `datahub_it/user_emails`"))
        conn.execute(
            sa.DDL(
                "CREATE VIEW `datahub_it/user_emails` WITH (security_invoker = TRUE) "
                "AS SELECT id, email FROM `datahub_it/users`"
            )
        )
        # Deterministic rows so the profiling golden is stable. UPSERT is DML, so
        # (unlike DDL) it runs fine inside the default transaction.
        conn.execute(
            sa.text(
                "UPSERT INTO `datahub_it/users` (id, name, email) "
                "VALUES (1, 'Alice', 'a@x.io'), (2, 'Bob', 'b@x.io'), (3, 'Carol', 'c@x.io')"
            )
        )
        conn.execute(
            sa.text(
                "UPSERT INTO `datahub_it/orders` (order_id, user_id) "
                "VALUES (10, 1), (11, 1), (12, 2)"
            )
        )
    engine.dispose()


def is_ydb_up() -> bool:
    try:
        engine = sa.create_engine("yql+ydb://localhost:2136/local")
        with engine.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
        engine.dispose()
        return True
    except Exception:
        return False


@pytest.fixture(scope="module")
def ydb_runner(docker_compose_runner, test_resources_dir):
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "ydb"
    ) as docker_services:
        wait_for_port(
            docker_services,
            "testydb",
            YDB_PORT,
            timeout=120,
            checker=is_ydb_up,
        )
        _seed_ydb()
        yield docker_services


@pytest.mark.parametrize(
    "config_file,golden_file,ignore_paths",
    [
        ("ydb_to_file.yml", "ydb_mces_golden.json", []),
        (
            "ydb_profile_to_file.yml",
            "ydb_profile_mces_golden.json",
            # Profile capture time is wall-clock; not frozen for YDB (see note above).
            [r"root\[\d+\]\['aspect'\]\['json'\]\['timestampMillis'\]"],
        ),
    ],
)
@pytest.mark.integration
def test_ydb_ingest(
    ydb_runner,
    pytestconfig,
    test_resources_dir,
    tmp_path,
    config_file,
    golden_file,
    ignore_paths,
):
    config_file_path = (test_resources_dir / config_file).resolve()
    run_datahub_cmd(["ingest", "-c", f"{config_file_path}"], tmp_path=tmp_path)

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "ydb_mces.json",
        golden_path=test_resources_dir / golden_file,
        ignore_paths=ignore_paths,
    )
