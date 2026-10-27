"""Notebook source lineage, reconciled with the tables a Spark run actually touched."""

from unittest.mock import MagicMock

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.ingestion.source.fabric.common.models import FabricSparkSession
from datahub.ingestion.source.fabric.onelake.notebook_code_lineage import (
    CodeTable,
    analyze_notebook_code,
    extract_notebook_code_lineage,
    reconcile_spark_lineage,
)
from datahub.ingestion.source.fabric.onelake.notebook_jobs import (
    NotebookProcessEmitter,
    PendingNotebook,
)
from datahub.ingestion.source.fabric.onelake.notebook_spark_lineage import (
    SparkTableRef,
    extract_spark_lineage,
)
from datahub.ingestion.source.fabric.onelake.report import FabricOneLakeSourceReport
from datahub.metadata.schema_classes import DataJobInputOutputClass
from datahub.sdk.datajob import DataJob

WS = "11111111-1111-1111-1111-111111111111"
LH = "22222222-2222-2222-2222-222222222222"
NB = "33333333-3333-3333-3333-333333333333"

# Loads three tables, writes dim_user from two of them, and writes dim_accounts
# from one. The dim_products write is commented out, so it is not a real target.
# Paths are assembled at run time the way Fabric notebooks build lakehouse paths.
_NOTEBOOK = """
%pip install example-lib

silver_path = f"{lakehouse[0]['abfs_path']}"
gold_path = f"{lakehouse[0]['abfs_path']}"
source = f"{silver_path}/Tables/silver/"
destination = f"{gold_path}/Tables/gold/"

orders = spark.read.format("delta").load(f"{source}orders")
customers = spark.read.format("delta").load(f"{source}customers")
products = spark.read.format("delta").load(f"{source}products")

# products.write.format("delta").mode("overwrite").save(f"{destination}dim_products")

table_user = "dim_user"
target_user = f"{destination}{table_user}"
users = orders.join(customers, "id", "left")
users.write.format("delta").mode("overwrite").save(target_user)

def write_with_retry(df, delta_path, mode="overwrite"):
    df.write.format("delta").mode(mode).save(delta_path)

table_name = "dim_accounts"
target_accounts = f"{destination}{table_name}"
accounts = orders.filter(orders["id"].isNotNull())
write_with_retry(accounts, target_accounts)
"""


def _pair(table: str, schema: str) -> tuple[str | None, str]:
    return schema, table


def test_code_lineage_keeps_only_sources_each_write_uses() -> None:
    lineage = extract_notebook_code_lineage(_NOTEBOOK)
    assert lineage is not None
    by_target = {
        _pair(target.table, target.schema or ""): {
            _pair(source.table, source.schema or "") for source in sources
        }
        for target, sources in lineage.items()
    }
    assert by_target == {
        ("gold", "dim_user"): {("silver", "orders"), ("silver", "customers")},
        ("gold", "dim_accounts"): {("silver", "orders")},
    }


def test_commented_write_and_unused_load_are_absent() -> None:
    lineage = extract_notebook_code_lineage(_NOTEBOOK)
    assert lineage is not None
    tables = {target.table for target in lineage}
    assert "dim_products" not in tables
    used = {source.table for sources in lineage.values() for source in sources}
    assert "products" not in used


def test_intermediate_write_is_a_source_of_the_next_write() -> None:
    source = """
source = f"{silver_path}/Tables/silver/"
destination = f"{gold_path}/Tables/gold/"
orders = spark.read.format("delta").load(f"{source}orders")
customers = spark.read.format("delta").load(f"{source}customers")
mid = orders.withColumn("x", orders["id"])
mid.write.format("delta").save(f"{destination}gold_mid")
final = mid.join(customers, "id")
final.write.format("delta").save(f"{destination}gold_final")
"""
    lineage = extract_notebook_code_lineage(source)
    assert lineage is not None
    final = lineage[CodeTable(table="gold_final", schema="gold")]
    assert CodeTable(table="orders", schema="silver") in final
    assert CodeTable(table="customers", schema="silver") in final
    assert CodeTable(table="gold_mid", schema="gold") in final


def test_unparseable_source_leaves_plan_lineage_unchanged() -> None:
    assert extract_notebook_code_lineage("def broken(:") is None


def _onelake(path: str) -> str:
    return f"abfss://{WS}@onelake.dfs.fabric.microsoft.com/{LH}/{path}"


def _plan(output: str, reads: list[str]) -> str:
    scans = "\n".join(
        f"Location: InMemoryFileIndex[{_onelake(path)}]" for path in reads
    )
    return f"""== Physical Plan ==
(1) SaveIntoDataSourceCommand
Arguments: [path={_onelake(output)}], Overwrite
(2) Scan json
{scans}
"""


def test_reconcile_drops_plan_sources_the_code_does_not_use() -> None:
    # The first plan loaded every table before dim_user was written, which is
    # how a Spark plan reports unused loads as sources of that write.
    execution = extract_spark_lineage(
        [
            _plan(
                "Tables/gold/dim_user",
                [
                    "Tables/silver/orders/_delta_log/0.json",
                    "Tables/silver/customers/_delta_log/0.json",
                    "Tables/silver/products/_delta_log/0.json",
                ],
            ),
            _plan(
                "Tables/gold/dim_accounts",
                ["Tables/silver/orders/_delta_log/0.json"],
            ),
            _plan(
                "Tables/gold/dim_products",
                ["Tables/silver/products/_delta_log/0.json"],
            ),
        ]
    )
    code = extract_notebook_code_lineage(_NOTEBOOK)
    assert code is not None
    reconciled = reconcile_spark_lineage(code, execution, lambda name: name)

    def inputs(table: str) -> set[str]:
        output = SparkTableRef(workspace_id=WS, item_id=LH, table=table, schema="gold")
        return {ref.table for ref in reconciled[output]}

    assert inputs("dim_user") == {"orders", "customers"}
    assert inputs("dim_accounts") == {"orders"}
    assert all(ref.table != "dim_products" for ref in reconciled)


def _emit_dim_user(*, reconcile_code: bool) -> DataJobInputOutputClass:
    orders = SparkTableRef(workspace_id=WS, item_id=LH, table="orders", schema="silver")
    customers = SparkTableRef(
        workspace_id=WS, item_id=LH, table="customers", schema="silver"
    )
    products = SparkTableRef(
        workspace_id=WS, item_id=LH, table="products", schema="silver"
    )
    dim_user = SparkTableRef(
        workspace_id=WS, item_id=LH, table="dim_user", schema="gold"
    )
    known = {
        ref.to_urn("PROD", None) for ref in (orders, customers, products, dim_user)
    }
    client = MagicMock()
    client.list_notebook_spark_sessions.return_value = [
        FabricSparkSession(livy_id="livy-1", application_id="app-1", item_id=NB)
    ]
    client.get_spark_sql_plans.return_value = [
        _plan(
            "Tables/gold/dim_user",
            [
                "Tables/silver/orders/_delta_log/0.json",
                "Tables/silver/customers/_delta_log/0.json",
                "Tables/silver/products/_delta_log/0.json",
            ],
        )
    ]
    client.get_notebook_runs.return_value = []
    emitter = NotebookProcessEmitter(
        platform_instance=None,
        env="PROD",
        execution_history_days=30,
        report=FabricOneLakeSourceReport(),
        client=client,
        known_table_urns=known,
        normalize_name=lambda name: name,
        owners_fn=lambda _upn: None,
        reconcile_code=reconcile_code,
    )
    emitted = list(
        emitter.emit(
            [
                PendingNotebook(
                    workspace_id=WS,
                    workspace_name="analytics",
                    notebook_id=NB,
                    display_name="pipeline",
                    description=None,
                    owner_user_principal_name=None,
                    content=_NOTEBOOK,
                    language="python",
                )
            ]
        )
    )
    jobs = [job for job in emitted if isinstance(job, DataJob)]
    assert len(jobs) == 1
    io = jobs[0]._get_aspect(DataJobInputOutputClass)
    assert io is not None
    return io


def test_emitter_uses_code_to_drop_unused_plan_sources() -> None:
    io = _emit_dim_user(reconcile_code=True)
    orders = SparkTableRef(workspace_id=WS, item_id=LH, table="orders", schema="silver")
    customers = SparkTableRef(
        workspace_id=WS, item_id=LH, table="customers", schema="silver"
    )
    products = SparkTableRef(
        workspace_id=WS, item_id=LH, table="products", schema="silver"
    )
    assert io.inputDatasets == sorted(
        [orders.to_urn("PROD", None), customers.to_urn("PROD", None)]
    )
    assert products.to_urn("PROD", None) not in (io.inputDatasets or [])


def _columns(source: str) -> dict[str, dict[str, set[tuple[str, str]]]]:
    analysis = analyze_notebook_code(source)
    assert analysis is not None
    return {
        target.table: {
            column: {(ref.table, upstream) for ref, upstream in refs}
            for column, refs in mapping.items()
        }
        for target, mapping in analysis.columns.items()
    }


def test_mapping_dict_renames_columns() -> None:
    source = """
source = f"{bronze}/Tables/dbo/"
destination = f"{silver}/Tables/silver/"
column_mapping = {"ACCOUNTNUM": "AccountNum", "NAME": "Name"}
table = "customers"
raw = (
    spark.read.format("delta").load(f"{source}{table}")
    .withColumn("SourceSystem", F.lit("SRC"))
)
out = raw.select(
    *[F.col(src).alias(dst) for src, dst in column_mapping.items()],
    F.col("SourceSystem"),
)
out.write.format("delta").save(f"{destination}silver_customers")
"""
    assert _columns(source) == {
        "silver_customers": {
            "AccountNum": {("customers", "ACCOUNTNUM")},
            "Name": {("customers", "NAME")},
        }
    }


def test_inline_alias_and_join_star_keep_upstream_columns() -> None:
    source = """
source = f"{bronze}/Tables/dbo/"
destination = f"{silver}/Tables/silver/"
orders = spark.read.format("delta").load(f"{source}orders").select(
    col("ID").alias("OrderId"),
    upper(col("CUST")).alias("CustId"),
)
customers = spark.read.format("delta").load(f"{source}customers").select(
    col("ID").alias("CustId"),
    col("NAME").alias("CustName"),
)
joined = orders.alias("o").join(customers.alias("c"), "CustId").select(
    "o.*",
    "c.CustName",
)
joined.write.format("delta").save(f"{destination}gold_orders")
"""
    assert _columns(source)["gold_orders"] == {
        "OrderId": {("orders", "ID")},
        "CustId": {("orders", "CUST")},
        "CustName": {("customers", "NAME")},
    }


def test_helper_mapping_and_dataframe_subscript() -> None:
    source = """
source = f"{bronze}/Tables/dbo/"
destination = f"{silver}/Tables/silver/"

def load_table(table_name, column_mapping, add_metadata=False):
    df = spark.read.format("delta").load(f"{source}{table_name}")
    if add_metadata:
        df = df.withColumn("LoadedAt", F.current_timestamp())
    return df.select(
        *[F.col(src).alias(dst) for src, dst in column_mapping.items()],
        *([F.col("LoadedAt")] if add_metadata else []),
    )

party_map = {"NAME": "Name", "RECID": "PartyId"}
party = load_table("party", party_map, add_metadata=True)
customers = spark.read.format("delta").load(f"{source}customers")
named = customers.select(
    F.upper(customers["accountnum"]).alias("AccountNum"),
    customers["name"],
)
party.write.format("delta").save(f"{destination}silver_party")
named.write.format("delta").save(f"{destination}silver_customers")
"""
    analysis = analyze_notebook_code(source)
    assert analysis is not None
    assert (
        CodeTable("party", "dbo")
        in analysis.tables[CodeTable("silver_party", "silver")]
    )
    columns = _columns(source)
    assert columns["silver_party"] == {
        "Name": {("party", "NAME")},
        "PartyId": {("party", "RECID")},
    }
    assert "LoadedAt" not in columns["silver_party"]
    assert columns["silver_customers"] == {
        "AccountNum": {("customers", "accountnum")},
        "name": {("customers", "name")},
    }


def test_saved_select_list_prefixes_columns_from_each_source() -> None:
    # dim-style notebooks build the select list first, then apply it, then
    # carry those prefixed columns through alias.* to the write.
    source = """
source = f"{silver}/Tables/silver/"
destination = f"{gold}/Tables/gold/"
people = spark.read.format("delta").load(f"{source}people")
accounts = spark.read.format("delta").load(f"{source}accounts")
joined = people.alias("p").join(accounts.alias("a"), "id", "left")
person_cols = ["PersonParty"]
account_cols = ["Email"]
select_exprs = (
    [F.col(f"p.{c}").alias(f"person_{c}") for c in person_cols]
    + [F.col(f"a.{c}").alias(f"account_{c}") for c in account_cols]
)
dim_user = joined.select(*select_exprs)
dim_user = dim_user.alias("du").select(F.col("du.*"))
dim_user = dim_user.withColumn("rn", F.lit(1)).drop("rn")
dim_user.write.format("delta").save(f"{destination}dim_user")
"""
    assert _columns(source)["dim_user"] == {
        "person_PersonParty": {("people", "PersonParty")},
        "account_Email": {("accounts", "Email")},
    }


def test_aliased_column_list_prefixes_the_output_name() -> None:
    source = """
source = f"{silver}/Tables/silver/"
destination = f"{gold}/Tables/gold/"
cols = ["Email"]
users = spark.read.format("delta").load(f"{source}users").alias("su")
out = users.select(*[F.col(f"su.{c}").alias(f"user_{c}") for c in cols])
out.write.format("delta").save(f"{destination}dim_user")
"""
    assert _columns(source) == {"dim_user": {"user_Email": {("users", "Email")}}}


def test_code_reconciliation_is_off_unless_enabled() -> None:
    io = _emit_dim_user(reconcile_code=False)
    products = SparkTableRef(
        workspace_id=WS, item_id=LH, table="products", schema="silver"
    )
    assert products.to_urn("PROD", None) in (io.inputDatasets or [])
    assert not io.fineGrainedLineages


_COLUMN_NOTEBOOK = """
source = f"{bronze}/Tables/dbo/"
destination = f"{silver}/Tables/silver/"
column_mapping = {"ACCOUNTNUM": "AccountNum", "NAME": "Name"}
table = "customers"
raw = spark.read.format("delta").load(f"{source}{table}")
out = raw.select(*[F.col(src).alias(dst) for src, dst in column_mapping.items()])
out.write.format("delta").save(f"{destination}silver_customers")
"""


def test_reconciled_job_carries_column_lineage() -> None:
    customers = SparkTableRef(
        workspace_id=WS, item_id=LH, table="customers", schema="dbo"
    )
    silver = SparkTableRef(
        workspace_id=WS, item_id=LH, table="silver_customers", schema="silver"
    )
    known = {ref.to_urn("PROD", None) for ref in (customers, silver)}
    client = MagicMock()
    client.list_notebook_spark_sessions.return_value = [
        FabricSparkSession(livy_id="livy-1", application_id="app-1", item_id=NB)
    ]
    client.get_spark_sql_plans.return_value = [
        _plan(
            "Tables/silver/silver_customers",
            ["Tables/dbo/customers/_delta_log/0.json"],
        )
    ]
    client.get_notebook_runs.return_value = []
    emitter = NotebookProcessEmitter(
        platform_instance=None,
        env="PROD",
        execution_history_days=30,
        report=FabricOneLakeSourceReport(),
        client=client,
        known_table_urns=known,
        normalize_name=lambda name: name,
        owners_fn=lambda _upn: None,
        reconcile_code=True,
    )
    jobs = [
        job
        for job in emitter.emit(
            [
                PendingNotebook(
                    workspace_id=WS,
                    workspace_name="analytics",
                    notebook_id=NB,
                    display_name="customers",
                    description=None,
                    owner_user_principal_name=None,
                    content=_COLUMN_NOTEBOOK,
                    language="python",
                )
            ]
        )
        if isinstance(job, DataJob)
    ]
    assert len(jobs) == 1
    io = jobs[0]._get_aspect(DataJobInputOutputClass)
    assert io is not None and io.fineGrainedLineages
    downstream = silver.to_urn("PROD", None)
    upstream = customers.to_urn("PROD", None)
    assert {
        (entry.downstreams[0], tuple(sorted(entry.upstreams or [])))
        for entry in io.fineGrainedLineages
    } == {
        (
            make_schema_field_urn(downstream, "AccountNum"),
            (make_schema_field_urn(upstream, "ACCOUNTNUM"),),
        ),
        (
            make_schema_field_urn(downstream, "Name"),
            (make_schema_field_urn(upstream, "NAME"),),
        ),
    }
