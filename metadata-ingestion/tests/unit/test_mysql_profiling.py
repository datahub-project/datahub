from typing import Type
from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.doris.doris_source import DorisConfig, DorisSource
from datahub.ingestion.source.sql.mysql import MySQLConfig, MySQLSource


def _source() -> MySQLSource:
    config = MySQLConfig(
        host_port="localhost:3306",
        profiling={"enabled": True},
    )
    return MySQLSource(config, PipelineContext(run_id="mysql-profiling-test"))


def _inspector_returning(rows: list) -> MagicMock:
    conn = MagicMock()
    conn.execute.return_value = rows
    inspector = MagicMock()
    inspector.engine.connect.return_value.__enter__.return_value = conn
    return inspector


@pytest.mark.parametrize(
    "source_cls,config_cls,host_port",
    [
        (MySQLSource, MySQLConfig, "localhost:3306"),
        # Doris inherits add_profile_metadata, so an override there has to keep
        # reading positionally too.
        (DorisSource, DorisConfig, "localhost:9030"),
    ],
)
def test_add_profile_metadata_reads_storage_bytes_positionally(
    source_cls: Type[MySQLSource],
    config_cls: Type[MySQLConfig],
    host_port: str,
) -> None:
    # Tuple rows (no named attributes) prove access is positional, not by the
    # label whose case differs across MySQL/MariaDB/Doris/TiDB.
    source = source_cls(
        config_cls(host_port=host_port, profiling={"enabled": True}),
        PipelineContext(run_id="mysql-family-profiling-test"),
    )
    inspector = _inspector_returning(
        [
            ("my_db", "orders", 4096),
            ("my_db", "customers", 8192),
        ]
    )

    source.add_profile_metadata(inspector)

    assert source.profile_metadata_info.dataset_name_to_storage_bytes == {
        "my_db.orders": 4096,
        "my_db.customers": 8192,
    }


def test_mysql_profiling_config_schema_lists_mysql_supported() -> None:
    # Redeclaring with Annotated[...] preserves the SupportedSources metadata on the
    # subclass field (a plain redeclaration drops it — verified empirically). MySQL's
    # config JSON schema must therefore advertise the platforms that inherit
    # MySQLProfilingConfig's guardrails: mysql (direct) plus mariadb/doris/tidb, which
    # inherit MySQLSource.generate_profile_candidates and so genuinely use these limits.
    from datahub.ingestion.source.sql.mysql import MySQLProfilingConfig

    expected = ["mysql", "mariadb", "doris", "tidb"]
    props = MySQLProfilingConfig.model_json_schema()["properties"]
    for field in ("profile_table_row_limit", "profile_table_size_limit"):
        assert props[field]["schema_extra"]["supported_sources"] == expected, (
            f"{field} supported_sources drifted from {expected}"
        )


def test_generate_profile_candidates_returns_get_identifier_strings() -> None:
    # Whatever generate_profile_candidates returns must match the dataset_name produced by
    # get_identifier character-for-character, or the membership test in
    # is_dataset_eligible_for_profiling silently no-ops. Building candidates via the SAME
    # get_identifier call guarantees that; this test pins the invariant.
    config = MySQLConfig(
        host_port="localhost:3306",
        profiling={
            "enabled": True,
            "profile_table_row_limit": 1_000_000,
        },
    )
    source = MySQLSource(config, PipelineContext(run_id="mysql-profiling-test"))
    inspector = _inspector_returning(
        [
            ("orders", 100, 1024),
            ("customers", 200, 2048),
            ("Mixed_Case", 50, 512),
        ]
    )

    candidates = source.generate_profile_candidates(
        inspector, threshold_time=None, schema="my_db"
    )

    expected = [
        source.get_identifier(schema="my_db", entity="orders", inspector=inspector),
        source.get_identifier(schema="my_db", entity="customers", inspector=inspector),
        source.get_identifier(schema="my_db", entity="Mixed_Case", inspector=inspector),
    ]
    assert candidates == expected
    # And the concrete shape for two-tier MySQL:
    assert candidates == ["my_db.orders", "my_db.customers", "my_db.Mixed_Case"]


def test_generate_profile_candidates_wiring() -> None:
    # The row/size guardrail is enforced inside the SQL (the `:table_row_limit IS NULL OR
    # table_rows < :table_row_limit` clauses), not in Python, so a mock inspector can't
    # verify filtered *results*. Pin the wiring instead: with limits set, the configured
    # values are forwarded as bind params so a real DB actually applies them; with both
    # limits at their None default, the method short-circuits and runs no query at all
    # (restoring the pre-guardrail behaviour of returning no candidate filter).
    config = MySQLConfig(
        host_port="localhost:3306",
        profiling={
            "enabled": True,
            "profile_table_row_limit": 150,
            "profile_table_size_limit": 1,
        },
    )
    source = MySQLSource(config, PipelineContext(run_id="mysql-profiling-test"))
    conn = MagicMock()
    conn.execute.return_value = [("small", 100, 1024)]
    inspector = MagicMock()
    inspector.engine.connect.return_value.__enter__.return_value = conn

    candidates = source.generate_profile_candidates(
        inspector, threshold_time=None, schema="my_db"
    )

    args, _kwargs = conn.execute.call_args
    params = args[1]
    assert params["table_row_limit"] == 150
    assert params["table_size_limit"] == 1
    assert params["schema"] == "my_db"
    assert candidates == ["my_db.small"]

    # Default config: both limits None -> no query, returns None.
    default_source = _source()
    default_conn = MagicMock()
    default_inspector = MagicMock()
    default_inspector.engine.connect.return_value.__enter__.return_value = default_conn

    result = default_source.generate_profile_candidates(
        default_inspector, threshold_time=None, schema="my_db"
    )

    assert result is None
    default_conn.execute.assert_not_called()


def test_mysql_profiling_overrides_do_not_drift() -> None:
    """Drift guard for the MySQLProfilingConfig inheritance sweep.

    Only compares ``fi.default``: an override that changes a field's description, type, or
    validators while keeping the default is invisible, as is any field using ``default_factory``
    (both sides read ``PydanticUndefined`` and compare equal). Do not over-trust this test for
    those cases.

    The allowlist is PER-FIELD, not per-config: MySQLProfilingConfig narrows four fields from
    GEProfilingConfig; each MySQL-derived config must, for each override field, either REVERT
    it to the grandparent's default or INHERIT MySQL's default. The decision is field-specific:
      - max_workers, report_expensive_tables: reverted by Doris and TiDB (single-primary-row-
        store rationale doesn't hold for an MPP / distributed engine; the warning's remediation
        advice is MySQL-specific). MariaDB inherits (it IS a single-primary row store).
      - profile_table_row_limit, profile_table_size_limit: inherited as MySQL's `None` by Doris,
        TiDB, AND MariaDB. None is what preserves prior behavior — the enforcement mechanism
        (generate_profile_candidates) is newly implemented for the MySQL family, so a non-None
        default would ACTIVATE a guardrail that never ran before, silently dropping profiles
        for tables over 5M rows using information_schema.tables.table_rows semantics these
        engines don't share with InnoDB.
    Without this test, a fifth field added to MySQLProfilingConfig would silently leak into the
    subclasses without a deliberate decision.
    """
    from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
    from datahub.ingestion.source.sql.doris.doris_source import DorisProfilingConfig
    from datahub.ingestion.source.sql.mysql import MySQLConfig, MySQLProfilingConfig
    from datahub.ingestion.source.sql.tidb import TiDBProfilingConfig

    def defaults(cfg_cls: type[BaseModel]) -> dict:
        return {name: fi.default for name, fi in cfg_cls.model_fields.items()}

    ge = defaults(GEProfilingConfig)
    mysql = defaults(MySQLProfilingConfig)
    mysql_overrides = {name for name in mysql if name in ge and mysql[name] != ge[name]}

    # Pin the override set so a new override added to MySQLProfilingConfig forces an update here.
    assert mysql_overrides == {
        "profile_table_row_limit",
        "profile_table_size_limit",
        "max_workers",
        "report_expensive_tables",
    }, (
        "MySQLProfilingConfig override set changed — update the per-field allowlist below and "
        f"this test. New override set: {sorted(mysql_overrides)}"
    )

    # Per-field decision table: "revert" = match GEProfilingConfig default; "inherit" = match
    # MySQLProfilingConfig default. Every subclass must declare a decision for every override
    # field; a missing entry fails loudly so a new override can't slip through undecided.
    decisions: dict[str, dict[str, str]] = {
        "DorisProfilingConfig": {
            "max_workers": "revert",
            "report_expensive_tables": "revert",
            "profile_table_row_limit": "inherit",
            "profile_table_size_limit": "inherit",
        },
        "TiDBProfilingConfig": {
            "max_workers": "revert",
            "report_expensive_tables": "revert",
            "profile_table_row_limit": "inherit",
            "profile_table_size_limit": "inherit",
        },
        # MariaDB uses MySQLConfig directly, so its profiling IS MySQLProfilingConfig — every
        # override is inherited. Listed explicitly so the test documents the intent and the next
        # sweep doesn't "fix" it wrongly (MariaDB is a MySQL fork; reverting would reintroduce
        # the long-transaction risk for a certified source).
        "MySQLConfig": {
            "max_workers": "inherit",
            "report_expensive_tables": "inherit",
            "profile_table_row_limit": "inherit",
            "profile_table_size_limit": "inherit",
        },
    }

    subclass_defaults = {
        "DorisProfilingConfig": defaults(DorisProfilingConfig),
        "TiDBProfilingConfig": defaults(TiDBProfilingConfig),
        # MariaDB's profiling config is MySQLProfilingConfig by construction; verify that too.
        "MySQLConfig": defaults(MySQLConfig().profiling.__class__),
    }

    for subclass_name, field_decisions in decisions.items():
        # Every override field must have a decision — no silent omissions.
        missing = mysql_overrides - set(field_decisions)
        assert not missing, (
            f"{subclass_name} has no decision for {sorted(missing)} — add each to the "
            "allowlist as 'revert' or 'inherit'."
        )
        actual = subclass_defaults[subclass_name]
        for field, decision in field_decisions.items():
            expected_source = ge if decision == "revert" else mysql
            assert actual[field] == expected_source[field], (
                f"{subclass_name}.{field} is '{decision}' but default {actual[field]!r} != "
                f"{('GE' if decision == 'revert' else 'MySQL')} default {expected_source[field]!r}. "
                "Either fix the subclass or correct the allowlist entry."
            )

    # MariaDB-specific: pin that MySQLConfig.profiling IS MySQLProfilingConfig (the inheritance
    # is by construction, not by redeclaring identical defaults — so it tracks future MySQL changes).
    assert MySQLConfig.model_fields["profiling"].annotation is MySQLProfilingConfig, (
        "MariaDB uses MySQLConfig directly; its profiling field must be MySQLProfilingConfig "
        "so it inherits MySQL's overrides. If this changed, update the comment in mariadb.py."
    )


def test_doris_and_tidb_inherited_limit_fields_keep_optional_and_supported_sources() -> (
    None
):
    # The limit fields on Doris/TiDB are INHERITED from MySQLProfilingConfig (not redeclared),
    # so they must carry MySQLProfilingConfig's
    # Annotated[Optional[int], SupportedSources(["mysql", "mariadb", "doris", "tidb"])] metadata
    # through to the subclass JSON schema. A bare-int redeclaration (the bug this guards
    # against) would drop Optional (so `null` is rejected) AND SupportedSources. Checked via
    # the JSON schema — the same surface MySQLProfilingConfig is verified on.
    from datahub.ingestion.source.sql.doris.doris_source import DorisProfilingConfig
    from datahub.ingestion.source.sql.tidb import TiDBProfilingConfig

    for cfg_cls in (DorisProfilingConfig, TiDBProfilingConfig):
        props = cfg_cls.model_json_schema()["properties"]
        for field in ("profile_table_row_limit", "profile_table_size_limit"):
            # Optional preserved: anyOf with null must be present.
            assert "anyOf" in props[field], (
                f"{cfg_cls.__name__}.{field} lost Optional — 'null' no longer accepted"
            )
            assert any(t.get("type") == "null" for t in props[field]["anyOf"]), (
                f"{cfg_cls.__name__}.{field} anyOf has no null variant"
            )
            # SupportedSources preserved: schema_extra.supported_sources must be non-empty.
            assert props[field]["schema_extra"].get("supported_sources"), (
                f"{cfg_cls.__name__}.{field} lost SupportedSources metadata"
            )
