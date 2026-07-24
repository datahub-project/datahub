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
        urn("orders"), {"order_id": "INTEGER", "customer_id": "INTEGER", "amount": "DECIMAL"}
    )
    return sr


def test_view_lineage_join_table_and_column_level() -> None:
    sql = (
        'create view "informix".customer_orders (customer_id,customer_name,order_id,amount) as '
        "select x0.id ,x0.name ,x1.order_id ,x1.amount from "
        '("informix".customers x0 join "informix".orders x1 on (x0.id = x1.customer_id ) )'
    )
    view_urn = make_dataset_urn("informix", "testdb.informix.customer_orders", "PROD")
    up = build_view_upstream_lineage(view_urn, sql, _resolver(), "testdb", "informix")
    assert up is not None
    upstream_names = sorted(u.dataset.split(",")[-2] for u in up.upstreams)
    assert upstream_names == ["testdb.informix.customers", "testdb.informix.orders"]
    assert up.fineGrainedLineages
