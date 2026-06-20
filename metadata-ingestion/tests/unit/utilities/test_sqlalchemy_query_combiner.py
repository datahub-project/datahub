import sqlalchemy as sa

from datahub.utilities.sqlalchemy_query_combiner import SQLAlchemyQueryCombiner


def _single_row(query) -> bool:
    return True


def test_combines_two_single_row_selects_into_one(tmp_path):
    engine = sa.create_engine("sqlite://")
    combiner = SQLAlchemyQueryCombiner(
        enabled=True,
        catch_exceptions=False,
        is_single_row_query_method=_single_row,
        serial_execution_fallback_enabled=False,
    )
    results = {}

    def work(key, value):
        with engine.connect() as conn:
            results[key] = conn.execute(sa.select(sa.literal(value))).scalar()

    with combiner.activate():
        combiner.run(lambda: work("a", 1))
        combiner.run(lambda: work("b", 2))
        combiner.flush()

    assert results == {"a": 1, "b": 2}
    assert combiner.report.combined_queries_issued == 1
    assert combiner.report.queries_combined == 2


def test_combines_multi_column_single_row_queries():
    engine = sa.create_engine("sqlite://")
    combiner = SQLAlchemyQueryCombiner(
        enabled=True,
        catch_exceptions=False,
        is_single_row_query_method=lambda q: True,
        serial_execution_fallback_enabled=False,
    )
    out = {}

    def work(key):
        with engine.connect() as conn:
            row = conn.execute(
                sa.select(sa.literal(1).label("x"), sa.literal(2).label("y"))
            ).fetchall()
            out[key] = (row[0][0], row[0][1])

    with combiner.activate():
        combiner.run(lambda: work("a"))
        combiner.run(lambda: work("b"))
        combiner.flush()

    assert out == {"a": (1, 2), "b": (1, 2)}
    assert combiner.report.combined_queries_issued == 1
    assert combiner.report.queries_combined == 2


def test_parameterized_query_is_not_combined_and_returns_correct_value():
    engine = sa.create_engine("sqlite://")
    combiner = SQLAlchemyQueryCombiner(
        enabled=True,
        catch_exceptions=False,
        is_single_row_query_method=lambda q: True,
        serial_execution_fallback_enabled=False,
    )
    out = {}

    def work():
        with engine.connect() as conn:
            stmt = sa.select(sa.bindparam("v", type_=sa.Integer))
            out["v"] = conn.execute(stmt, {"v": 7}).scalar()

    with combiner.activate():
        combiner.run(work)
        combiner.flush()

    assert out["v"] == 7
    assert combiner.report.uncombined_queries_issued >= 1


def test_serial_fallback_when_combine_fails():
    engine = sa.create_engine("sqlite://")
    combiner = SQLAlchemyQueryCombiner(
        enabled=True,
        catch_exceptions=True,
        is_single_row_query_method=lambda q: True,
        serial_execution_fallback_enabled=True,
    )
    out = {}

    def work(key, value):
        with engine.connect() as conn:
            out[key] = conn.execute(sa.select(sa.literal(value))).scalar()

    with combiner.activate():
        combiner.run(lambda: work("a", 1))
        combiner.flush()

    assert out == {"a": 1}
