"""Static notebook table-reference parsing and registry resolution."""

from typing import List, Set

from datahub.ingestion.source.fabric.onelake.notebook_lineage import (
    NotebookTableRegistry,
    TableRef,
    extract_table_references,
    resolve_lineage,
)


def _names(refs: List[TableRef]) -> Set[str]:
    return {".".join(r.parts) for r in refs}


def _ref(name: str) -> TableRef:
    ref = TableRef.from_name(name)
    assert ref is not None
    return ref


def test_extract_sql_notebook_reads_and_writes() -> None:
    source = """
    CREATE TABLE gold.sales AS
    SELECT * FROM silver.orders o JOIN silver.customers c ON o.cid = c.id
    """
    refs = extract_table_references(source, "sql")
    assert "gold.sales" in _names(refs.writes)
    assert {"silver.orders", "silver.customers"} <= _names(refs.reads)


def test_extract_python_spark_sql_and_dataframe() -> None:
    source = """
# CELL
df = spark.read.table("lh.silver.orders")
spark.sql("INSERT INTO lh.gold.summary SELECT * FROM lh.silver.orders")
df.write.saveAsTable("lh.gold.sales")
"""
    refs = extract_table_references(source, "python")
    assert "lh.gold.sales" in _names(refs.writes)
    assert "lh.gold.summary" in _names(refs.writes)
    assert "lh.silver.orders" in _names(refs.reads)


def test_extract_sql_magic_cell() -> None:
    source = """
# CELL
print("hi")
# CELL
# MAGIC %%sql
# MAGIC INSERT INTO gold.t SELECT * FROM silver.s
"""
    refs = extract_table_references(source, "python")
    assert "gold.t" in _names(refs.writes)
    assert "silver.s" in _names(refs.reads)


def test_registry_resolves_full_and_unambiguous_names() -> None:
    registry = NotebookTableRegistry()
    registry.register("lh", "silver", "orders", "urn:li:dataset:orders")
    registry.register("lh", "gold", "sales", "urn:li:dataset:sales")

    # item.schema.table (3 parts)
    assert registry.resolve(_ref("lh.silver.orders")) == "urn:li:dataset:orders"
    # schema.table (2 parts) resolves when unique
    assert registry.resolve(_ref("gold.sales")) == "urn:li:dataset:sales"
    # bare table (1 part) resolves when unique
    assert registry.resolve(_ref("orders")) == "urn:li:dataset:orders"


def test_registry_leaves_ambiguous_bare_name_unresolved() -> None:
    registry = NotebookTableRegistry()
    registry.register("lh_a", "s", "orders", "urn:li:dataset:a")
    registry.register("lh_b", "s", "orders", "urn:li:dataset:b")
    # Same schema.table in two items -> ambiguous, not guessed.
    assert registry.resolve(_ref("orders")) is None


def test_resolve_lineage_counts_unresolved_and_dedupes() -> None:
    registry = NotebookTableRegistry()
    registry.register("lh", "silver", "orders", "urn:li:dataset:orders")
    registry.register("lh", "gold", "sales", "urn:li:dataset:sales")

    source = """
    INSERT INTO lh.gold.sales
    SELECT * FROM lh.silver.orders JOIN lh.silver.missing ON 1=1
    """
    refs = extract_table_references(source, "sql")
    resolved = resolve_lineage(refs, registry)

    assert resolved.output_urns == ["urn:li:dataset:sales"]
    assert resolved.input_urns == ["urn:li:dataset:orders"]
    # lh.silver.missing is not ingested -> one unresolved reference.
    assert resolved.unresolved == 1
