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
