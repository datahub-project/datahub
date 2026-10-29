"""OracleInspectorObjectWrapper (DBA_* data dictionary) behaviour: identifier
normalisation, column type mapping and constraint parsing, against a stubbed
SQLAlchemy connection."""

from typing import Dict, List, Optional, Tuple
from unittest.mock import Mock

import pytest
import sqlalchemy.exc
from sqlalchemy.engine import Connection, Inspector
from sqlalchemy.sql import sqltypes

from datahub.ingestion.source.sql.oracle import OracleInspectorObjectWrapper
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from tests.unit.sql_test_helpers import FakeResult

Row = Tuple[object, ...]


@pytest.fixture
def inspector() -> Mock:
    mock_inspector = Mock(spec=Inspector)
    mock_inspector.bind = Mock(spec=Connection)
    mock_inspector.dialect = Mock()
    # Like the real dialect, both pass None through untouched.
    mock_inspector.dialect.normalize_name = Mock(
        side_effect=lambda x: x.lower() if x else x
    )
    mock_inspector.dialect.denormalize_name = Mock(
        side_effect=lambda x: x.upper() if x else x
    )
    mock_inspector.dialect.default_schema_name = "DEFAULT_OWNER"
    mock_inspector.default_schema_name = "DEFAULT_OWNER"
    mock_inspector.dialect.server_version_info = (19,)
    return mock_inspector


@pytest.fixture
def report() -> Mock:
    return Mock(spec=SQLSourceReport)


@pytest.fixture
def wrapper(inspector: Mock, report: Mock) -> OracleInspectorObjectWrapper:
    return OracleInspectorObjectWrapper(inspector, report)


def _answer(inspector: Mock, rows: List[Row]) -> None:
    """Make every data-dictionary query on the wrapped connection return ``rows``."""
    inspector.bind.execute.return_value = FakeResult(rows)


def _last_query(inspector: Mock) -> Tuple[str, Dict[str, object]]:
    args = inspector.bind.execute.call_args.args
    return str(args[0]), (args[1] if len(args) > 1 else {})


def test_schema_names_are_normalised(
    wrapper: OracleInspectorObjectWrapper, inspector: Mock
) -> None:
    _answer(inspector, [("HR",), ("SALES",)])
    assert wrapper.get_schema_names() == ["hr", "sales"]


def test_table_names_exclude_system_tablespaces_and_use_the_denormalised_owner(
    wrapper: OracleInspectorObjectWrapper, inspector: Mock
) -> None:
    _answer(inspector, [("ORDERS",)])
    assert wrapper.get_table_names("hr") == ["orders"]
    sql, params = _last_query(inspector)
    assert "'SYSTEM', 'SYSAUX'" in sql
    assert "IOT_NAME IS NULL" in sql
    assert params == {"owner": "HR"}


def test_view_names_fall_back_to_the_dialect_default_schema(
    wrapper: OracleInspectorObjectWrapper, inspector: Mock
) -> None:
    _answer(inspector, [("V_ORDERS",)])
    assert wrapper.get_view_names() == ["v_orders"]
    _, params = _last_query(inspector)
    assert params == {"owner": "DEFAULT_OWNER"}


def _column_row(
    name: str,
    data_type: str,
    length: Optional[int] = None,
    precision: Optional[int] = None,
    scale: Optional[int] = None,
    nullable: str = "Y",
    default: Optional[str] = None,
    comment: Optional[str] = None,
    virtual: str = "NO",
    default_on_null: Optional[str] = None,
    identity_options: Optional[str] = None,
) -> Row:
    return (
        name,
        data_type,
        length,
        precision,
        scale,
        nullable,
        default,
        comment,
        virtual,
        default_on_null,
        identity_options,
    )


def test_columns_map_oracle_data_types(
    wrapper: OracleInspectorObjectWrapper, inspector: Mock
) -> None:
    _answer(
        inspector,
        [
            _column_row("ID", "NUMBER", scale=0, nullable="N", comment="pk"),
            _column_row("AMOUNT", "NUMBER", precision=10, scale=2),
            _column_row("NAME", "VARCHAR2", length=50, default="'anon'"),
            _column_row("TS", "TIMESTAMP(6) WITH TIME ZONE"),
            _column_row("MYSTERY", "SOME_UDT_TYPE"),
        ],
    )
    columns = {c["name"]: c for c in wrapper.get_columns("orders", "hr")}

    assert isinstance(columns["id"]["type"], sqltypes.INTEGER)
    assert columns["id"]["nullable"] is False
    assert columns["id"]["comment"] == "pk"
    amount_type = columns["amount"]["type"]
    assert (amount_type.precision, amount_type.scale) == (10, 2)
    assert columns["name"]["type"].length == 50
    assert columns["name"]["default"] == "'anon'"
    assert columns["ts"]["type"].timezone is True
    assert isinstance(columns["mystery"]["type"], sqltypes.NullType)

    _, params = _last_query(inspector)
    assert params == {"table_name": "ORDERS", "owner": "HR"}


def test_virtual_and_identity_columns_move_their_expression_out_of_default(
    wrapper: OracleInspectorObjectWrapper, inspector: Mock
) -> None:
    """Oracle reports a virtual column's expression and an identity column's
    sequence as DATA_DEFAULT; neither is a default value, so they are surfaced
    as ``computed`` / ``identity`` and the default is cleared."""
    inspector.dialect._parse_identity_options.return_value = {"always": True}
    _answer(
        inspector,
        [
            _column_row(
                "FULL_NAME",
                "VARCHAR2",
                length=100,
                default='"FIRST"||"LAST"',
                virtual="YES",
            ),
            _column_row(
                "SEQ",
                "NUMBER",
                scale=0,
                default='"HR"."ISEQ$$_1".nextval',
                default_on_null="NO",
                identity_options="ALWAYS,START WITH: 1",
            ),
        ],
    )
    columns = {c["name"]: c for c in wrapper.get_columns("orders", "hr")}

    assert columns["full_name"]["computed"] == {"sqltext": '"FIRST"||"LAST"'}
    assert columns["full_name"]["default"] is None
    assert columns["seq"]["identity"] == {"always": True}
    assert columns["seq"]["default"] is None


def test_table_comment_is_read_for_the_denormalised_table_and_owner(
    wrapper: OracleInspectorObjectWrapper, inspector: Mock
) -> None:
    _answer(inspector, [("customer orders",)])
    assert wrapper.get_table_comment("orders", "hr") == {"text": "customer orders"}
    _, params = _last_query(inspector)
    assert params == {"table_name": "ORDERS", "schema_name": "HR"}


def _constraint_row(
    name: str,
    kind: str,
    local_column: str,
    remote_table: Optional[str] = None,
    remote_column: Optional[str] = None,
    remote_owner: Optional[str] = None,
    delete_rule: Optional[str] = None,
) -> Row:
    return (
        name,
        kind,
        local_column,
        remote_table,
        remote_column,
        remote_owner,
        1,
        1,
        None,
        delete_rule,
    )


def test_primary_key_collects_only_p_constraint_columns(
    wrapper: OracleInspectorObjectWrapper, inspector: Mock
) -> None:
    _answer(
        inspector,
        [
            _constraint_row("PK_ORDERS", "P", "ID"),
            _constraint_row("PK_ORDERS", "P", "REGION"),
            _constraint_row("UQ_CODE", "U", "CODE"),
        ],
    )
    assert wrapper.get_pk_constraint("orders", "hr") == {
        "constrained_columns": ["id", "region"],
        "name": "pk_orders",
    }


def test_primary_key_lookup_failure_degrades_to_no_key_with_a_warning(
    wrapper: OracleInspectorObjectWrapper, inspector: Mock, report: Mock
) -> None:
    inspector.bind.execute.side_effect = sqlalchemy.exc.DatabaseError(
        "select", None, Exception("ORA-00942")
    )
    assert wrapper.get_pk_constraint("orders", "hr") == {
        "constrained_columns": [],
        "name": None,
    }
    report.warning.assert_called_once()


def test_foreign_keys_group_columns_per_constraint_with_referred_schema(
    wrapper: OracleInspectorObjectWrapper, inspector: Mock
) -> None:
    """A composite FK arrives as one row per column; they must be folded into
    a single constraint, keeping the remote schema when it differs from the owner."""
    _answer(
        inspector,
        [
            _constraint_row("PK_ORDERS", "P", "ID"),
            _constraint_row(
                "FK_CUST", "R", "CUSTOMER_ID", "CUSTOMERS", "ID", "SALES", "CASCADE"
            ),
            _constraint_row(
                "FK_CUST",
                "R",
                "REGION_ID",
                "CUSTOMERS",
                "REGION_ID",
                "SALES",
                "CASCADE",
            ),
            _constraint_row(
                "FK_ITEM", "R", "ITEM_ID", "ITEMS", "ID", "HR", "NO ACTION"
            ),
        ],
    )
    fkeys = {fk["name"]: fk for fk in wrapper.get_foreign_keys("orders", "hr")}

    assert set(fkeys) == {"fk_cust", "fk_item"}
    assert fkeys["fk_cust"]["constrained_columns"] == ["customer_id", "region_id"]
    assert fkeys["fk_cust"]["referred_columns"] == ["id", "region_id"]
    assert fkeys["fk_cust"]["referred_table"] == "customers"
    assert fkeys["fk_cust"]["referred_schema"] == "sales"
    assert fkeys["fk_cust"]["options"] == {"ondelete": "CASCADE"}
    assert fkeys["fk_item"]["options"] == {}


def test_foreign_key_to_an_invisible_table_is_reported(
    wrapper: OracleInspectorObjectWrapper, inspector: Mock, report: Mock
) -> None:
    _answer(inspector, [_constraint_row("FK_HIDDEN", "R", "X_ID", None, None, None)])
    fkeys = wrapper.get_foreign_keys("orders", "hr")
    assert len(fkeys) == 1
    assert fkeys[0]["referred_table"] is None
    report.warning.assert_called_once()


def test_view_definition_is_scoped_to_the_owner(
    wrapper: OracleInspectorObjectWrapper, inspector: Mock
) -> None:
    _answer(inspector, [("SELECT * FROM orders",)])
    assert wrapper.get_view_definition("v_orders", "hr") == "SELECT * FROM orders"
    sql, params = _last_query(inspector)
    assert "owner = :owner" in sql
    assert params == {"view_name": "V_ORDERS", "owner": "HR"}
