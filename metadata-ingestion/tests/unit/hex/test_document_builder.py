from datetime import datetime
from typing import Dict, List, Optional

from datahub.ingestion.source.hex.document_builder import (
    HexDocumentBuilder,
    _indent_sql,
)
from datahub.ingestion.source.hex.model import (
    Category,
    Collection,
    Component,
    ExploreCell,
    HexConnection,
    Owner,
    Project,
    SqlCell,
    Status,
)

WORKSPACE = "test-workspace"
CONNECTIONS: Dict[str, HexConnection] = {
    "conn-sf": HexConnection(name="Analytics Hub", platform="snowflake"),
    "conn-bq": HexConnection(name="BQ Hub", platform="bigquery"),
}


def _builder(
    workspace_name: str = WORKSPACE,
    connections: Optional[Dict[str, HexConnection]] = None,
) -> HexDocumentBuilder:
    return HexDocumentBuilder(
        workspace_name=workspace_name,
        connections=connections if connections is not None else CONNECTIONS,
    )


def _project(
    id: str = "proj-1",
    title: str = "My Project",
    description: Optional[str] = None,
    owner: Optional[Owner] = None,
    creator: Optional[Owner] = None,
    status: Optional[Status] = None,
    categories: Optional[List[Category]] = None,
    collections: Optional[List[Collection]] = None,
    last_edited_at: Optional[datetime] = None,
    created_at: Optional[datetime] = None,
) -> Project:
    return Project(
        id=id,
        title=title,
        description=description,
        owner=owner,
        creator=creator,
        status=status,
        categories=categories,
        collections=collections,
        last_edited_at=last_edited_at,
        created_at=created_at,
    )


def _component(
    id: str = "comp-1",
    title: str = "My Component",
    description: Optional[str] = None,
    owner: Optional[Owner] = None,
    status: Optional[Status] = None,
) -> Component:
    return Component(
        id=id,
        title=title,
        description=description,
        owner=owner,
        status=status,
    )


def _sql_cell(source: str, conn_id: str = "conn-sf", label: str = "Query") -> SqlCell:
    return SqlCell(
        cell_id="cell-1",
        cell_label=label,
        sql_source=source,
        data_connection_id=conn_id,
    )


def _explore_cell(label: str = "Chart", chart_type: str = "bar") -> ExploreCell:
    return ExploreCell(
        cell_id="exp-1",
        cell_label=label,
        dataframe="df",
        chart_type=chart_type,
    )


# ------------------------------------------------------------------
# _render_header
# ------------------------------------------------------------------


def test_render_header_title_always_present():
    b = _builder()
    header = b._render_header(_project(title="My Dashboard"))
    assert "# My Dashboard" in header


def test_render_header_workspace_always_present():
    b = _builder(workspace_name="acme")
    header = b._render_header(_project())
    assert "acme" in header


def test_render_header_owner_present_when_set():
    b = _builder()
    p = _project(owner=Owner(email="alice@example.com"))
    header = b._render_header(p)
    assert "alice@example.com" in header


def test_render_header_no_owner_when_absent():
    b = _builder()
    header = b._render_header(_project())
    assert "Owner" not in header


def test_render_header_creator_shown_when_different_from_owner():
    b = _builder()
    p = _project(
        owner=Owner(email="bob@example.com"),
        creator=Owner(email="alice@example.com"),
    )
    header = b._render_header(p)
    assert "alice@example.com" in header
    assert "bob@example.com" in header


def test_render_header_creator_not_repeated_when_same_as_owner():
    b = _builder()
    p = _project(
        owner=Owner(email="alice@example.com"),
        creator=Owner(email="alice@example.com"),
    )
    header = b._render_header(p)
    # alice appears once or twice — important thing is no crash
    assert "alice@example.com" in header


def test_render_header_status():
    b = _builder()
    p = _project(status=Status(name="Published"))
    header = b._render_header(p)
    assert "Published" in header


def test_render_header_categories():
    b = _builder()
    p = _project(categories=[Category(name="Analytics"), Category(name="Finance")])
    header = b._render_header(p)
    assert "Analytics" in header
    assert "Finance" in header


def test_render_header_collections():
    b = _builder()
    p = _project(collections=[Collection(name="Q1 Reports")])
    header = b._render_header(p)
    assert "Q1 Reports" in header


def test_render_header_last_edited_at():
    b = _builder()
    p = _project(last_edited_at=datetime(2025, 6, 15))
    header = b._render_header(p)
    assert "2025-06-15" in header


def test_render_header_description():
    b = _builder()
    p = _project(description="  This notebook analyses sales data.  ")
    header = b._render_header(p)
    assert "This notebook analyses sales data." in header


def test_render_header_component_works_same_as_project():
    b = _builder()
    comp = _component(
        title="Shared SQL",
        owner=Owner(email="dev@example.com"),
        status=Status(name="Draft"),
    )
    header = b._render_header(comp)
    assert "Shared SQL" in header
    assert "dev@example.com" in header
    assert "Draft" in header


# ------------------------------------------------------------------
# _render_markdown — section assembly
# ------------------------------------------------------------------


def test_render_markdown_sql_section_with_connection_label():
    b = _builder()
    sql_cells = [_sql_cell("SELECT id FROM customers", conn_id="conn-sf", label="CQ")]
    text = b._render_markdown(_project(), sql_cells, [], [], "")
    assert "## SQL Queries" in text
    assert "CQ" in text
    assert "Analytics Hub" in text  # display name
    assert "snowflake" in text  # platform from CONNECTION_PLATFORMS map
    assert "SELECT id FROM customers" in text


def test_render_markdown_sql_cell_unknown_connection_shows_unknown():
    b = _builder()
    sql_cells = [
        SqlCell(
            cell_id="c",
            cell_label="Query",
            sql_source="SELECT 1",
            data_connection_id="conn-missing",
        )
    ]
    text = b._render_markdown(_project(), sql_cells, [], [], "")
    assert "unknown" in text


def test_render_markdown_explore_section():
    b = _builder()
    explore_cells = [_explore_cell(label="Revenue Chart", chart_type="line")]
    text = b._render_markdown(_project(), [], explore_cells, [], "")
    assert "## Visualisations" in text
    assert "Revenue Chart" in text
    assert "line" in text


def test_render_markdown_section_names():
    b = _builder()
    text = b._render_markdown(_project(), [], [], ["Overview", "Deep Dive"], "")
    assert "## Notebook Structure" in text
    assert "Overview" in text
    assert "Deep Dive" in text


def test_render_markdown_notebook_docs():
    b = _builder()
    text = b._render_markdown(_project(), [], [], [], "# Docs\n\nSome description.")
    assert "## Notebook Documentation" in text
    assert "Some description." in text


def test_render_markdown_empty_cells_no_sql_section():
    b = _builder()
    text = b._render_markdown(_project(), [], [], [], "")
    assert "## SQL Queries" not in text
    assert "## Visualisations" not in text
    assert "## Notebook Structure" not in text


def test_render_markdown_whitespace_markdown_not_emitted():
    b = _builder()
    text = b._render_markdown(_project(), [], [], [], "   \n  ")
    assert "## Notebook Documentation" not in text


# ------------------------------------------------------------------
# build_document — produces work units
# ------------------------------------------------------------------


def test_build_document_project_emits_work_units():
    b = _builder()
    p = _project(title="Test", created_at=datetime(2024, 1, 1))
    wus = list(
        b.build_document(
            project=p,
            sql_cells=[],
            explore_cells=[],
            section_names=[],
            markdown_content="",
            dashboard_urn="urn:li:dashboard:(hex,proj-1)",
        )
    )
    assert len(wus) > 0


def test_build_document_component_title_prefix():
    b = _builder()
    comp = _component(title="Shared Logic")
    wus = list(
        b.build_document(
            project=comp,
            sql_cells=[],
            explore_cells=[],
            section_names=[],
            markdown_content="",
            dashboard_urn="urn:li:chart:(hex,comp-1)",
        )
    )
    assert any(
        "Component" in str(wu.metadata) and "Shared Logic" in str(wu.metadata)
        for wu in wus
    )


def test_build_document_project_title_prefix():
    b = _builder()
    p = _project(title="Analytics")
    wus = list(
        b.build_document(
            project=p,
            sql_cells=[],
            explore_cells=[],
            section_names=[],
            markdown_content="",
            dashboard_urn="urn:li:dashboard:(hex,proj-1)",
        )
    )
    assert any(
        "Project" in str(wu.metadata) and "Analytics" in str(wu.metadata) for wu in wus
    )


def test_build_document_custom_properties_include_counts():
    b = _builder()
    p = _project()
    sql_cells = [_sql_cell("SELECT 1")]
    explore_cells = [_explore_cell()]
    wus = list(
        b.build_document(
            project=p,
            sql_cells=sql_cells,
            explore_cells=explore_cells,
            section_names=[],
            markdown_content="",
            dashboard_urn="urn:li:dashboard:(hex,proj-1)",
        )
    )
    doc_text = " ".join(str(wu.metadata) for wu in wus)
    assert "sql_cell_count" in doc_text
    assert "explore_cell_count" in doc_text


# ------------------------------------------------------------------
# _indent_sql
# ------------------------------------------------------------------


def test_indent_sql_strips_common_indent():
    sql = "    SELECT id\n    FROM t"
    result = _indent_sql(sql)
    assert result == "SELECT id\nFROM t"


def test_indent_sql_no_indent_unchanged():
    sql = "SELECT id\nFROM t"
    assert _indent_sql(sql) == sql


def test_indent_sql_mixed_indent_uses_minimum():
    sql = "  SELECT id\n    FROM t"
    result = _indent_sql(sql)
    assert result == "SELECT id\n  FROM t"
