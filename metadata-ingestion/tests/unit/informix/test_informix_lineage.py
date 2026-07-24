from datahub.emitter.mce_builder import make_dataset_urn
from datahub.ingestion.source.informix.lineage import build_view_upstream_lineage
from datahub.sql_parsing.schema_resolver import SchemaResolver


def _resolver() -> SchemaResolver:
    sr = SchemaResolver(platform="informix", env="PROD")

    def urn(n: str) -> str:
        return make_dataset_urn("informix", f"testdb.informix.{n}", "PROD")

    sr.add_raw_schema_info(
        urn("customers"), {"id": "INTEGER", "name": "VARCHAR", "email": "VARCHAR"}
    )
    sr.add_raw_schema_info(
        urn("orders"),
        {"order_id": "INTEGER", "customer_id": "INTEGER", "amount": "DECIMAL"},
    )
    return sr


def test_view_lineage_join_table_and_column_level() -> None:
    sql = (
        'create view "informix".customer_orders (customer_id,customer_name,order_id,amount) as '
        "select x0.id ,x0.name ,x1.order_id ,x1.amount from "
        '("informix".customers x0 join "informix".orders x1 on (x0.id = x1.customer_id ) )'
    )
    view_urn = make_dataset_urn("informix", "testdb.informix.customer_orders", "PROD")
    up = build_view_upstream_lineage(
        view_urn,
        sql,
        _resolver(),
        "testdb",
        "informix",
        ["customer_id", "customer_name", "order_id", "amount"],
    )
    assert up is not None
    upstream_names = sorted(u.dataset.split(",")[-2] for u in up.upstreams)
    assert upstream_names == ["testdb.informix.customers", "testdb.informix.orders"]
    assert up.fineGrainedLineages

    # Downstream field names must be the view's DECLARED columns (customer_id,
    # customer_name), not the inner SELECT projection names (id, name) that
    # sqlglot reports for Informix's alias-stripped normalized view text.
    def _down(fgl: object) -> str:
        return fgl.downstreams[0].split(",")[-1].rstrip(")")  # type: ignore[attr-defined]

    def _up(fgl: object) -> str:
        return fgl.upstreams[0].split(",")[-1].rstrip(")")  # type: ignore[attr-defined]

    lineage_map = {_down(fgl): _up(fgl) for fgl in up.fineGrainedLineages}
    assert lineage_map["customer_id"] == "id"
    assert lineage_map["customer_name"] == "name"
    assert "id" not in lineage_map  # the inner projection name must NOT leak downstream
