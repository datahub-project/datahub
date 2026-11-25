"""`datahub tag`, `datahub glossary` and `datahub domain`: manage the catalog's
own objects. To attach them to entities, use `datahub edit`."""

import json
from typing import Dict, List, Optional

import click

from datahub.cli.edit_operations import EXIT_NOT_FOUND, EditCliError
from datahub.configuration.common import GraphError
from datahub.emitter.mce_builder import make_domain_urn, make_tag_urn, make_term_urn
from datahub.ingestion.graph.client import DataHubGraph, get_default_graph
from datahub.ingestion.graph.config import ClientMode
from datahub.upgrade import upgrade

# Distinct from EXIT_NOT_FOUND so scripts can treat a re-run create as success.
EXIT_ALREADY_EXISTS = 6

_LIST_QUERY = """
query listCatalog($input: SearchAcrossEntitiesInput!) {
  searchAcrossEntities(input: $input) {
    total
    searchResults {
      entity {
        urn
        ... on Tag { properties { name description } }
        ... on GlossaryTerm { properties { name description } }
        ... on GlossaryNode { properties { name description } }
        ... on Domain { properties { name description } }
      }
    }
  }
}
"""


def _make_node_urn(node: str) -> str:
    return (
        node
        if node.startswith("urn:li:glossaryNode:")
        else f"urn:li:glossaryNode:{node}"
    )


def _echo(data: Dict) -> None:
    click.echo(json.dumps(data, indent=2))


def _list(entity_type: str, query: str, limit: int) -> None:
    with get_default_graph(ClientMode.CLI) as graph:
        data = graph.execute_graphql(
            _LIST_QUERY,
            variables={
                "input": {
                    "types": [entity_type],
                    "query": query,
                    "start": 0,
                    "count": limit,
                }
            },
        )["searchAcrossEntities"]
    items: List[Dict[str, Optional[str]]] = [
        {
            "urn": r["entity"]["urn"],
            "name": (r["entity"].get("properties") or {}).get("name"),
            "description": (r["entity"].get("properties") or {}).get("description"),
        }
        for r in data["searchResults"]
    ]
    _echo({"total": data["total"], "results": items})


def _create(graph: DataHubGraph, mutation: str, input_type: str, fields: Dict) -> str:
    try:
        data = graph.execute_graphql(
            f"mutation m($input: {input_type}!) {{ {mutation}(input: $input) }}",
            variables={"input": {k: v for k, v in fields.items() if v is not None}},
        )
    except GraphError as e:
        # GMS reports duplicates as a generic 400; surface them distinctly so
        # scripts can treat "already exists" as success.
        if "already exists" in str(e):
            raise EditCliError(
                f"{fields.get('id') or fields.get('name')} already exists",
                error_type="already_exists",
                exit_code=EXIT_ALREADY_EXISTS,
            ) from e
        raise EditCliError(str(e), error_type="create_failed") from e
    return data[mutation]


def _delete(urn: str, mutation: str, force: bool) -> None:
    with get_default_graph(ClientMode.CLI) as graph:
        if not graph.exists(urn):
            raise EditCliError(
                f"Not found: {urn}", error_type="not_found", exit_code=EXIT_NOT_FOUND
            )
        if not force:
            click.confirm(
                f"Delete {urn}? References to it on other entities are removed too.",
                abort=True,
                err=True,
            )
        deleted = graph.execute_graphql(
            f"mutation m($urn: String!) {{ {mutation}(urn: $urn) }}",
            variables={"urn": urn},
        )[mutation]
    _echo({"urn": urn, "deleted": bool(deleted)})


_QUERY_OPTION = click.option("--query", default="*", show_default=True)
_LIMIT_OPTION = click.option(
    "--limit", type=click.IntRange(1, 1000), default=100, show_default=True
)
_FORCE_OPTION = click.option("--force", is_flag=True, help="Do not ask to confirm.")


# --- tags ----------------------------------------------------------------------------


@click.group()
def tag() -> None:
    """Create, list and delete tags. To tag an entity, use `datahub edit add-tag`."""


@tag.command("create")
@click.option("--name", required=True, help="Display name.")
@click.option(
    "--id", "tag_id", default=None, help="Tag id in the URN. Defaults to --name."
)
@click.option("--description", default=None)
@upgrade.check_upgrade
def tag_create(name: str, tag_id: Optional[str], description: Optional[str]) -> None:
    """Create a tag. Fails if a tag with that id already exists."""
    with get_default_graph(ClientMode.CLI) as graph:
        urn = _create(
            graph,
            "createTag",
            "CreateTagInput",
            {"id": tag_id or name, "name": name, "description": description},
        )
    _echo({"created": urn})


@tag.command("list")
@_QUERY_OPTION
@_LIMIT_OPTION
@upgrade.check_upgrade
def tag_list(query: str, limit: int) -> None:
    """List tags."""
    _list("TAG", query, limit)


@tag.command("delete")
@click.option("--tag", "tag_name", required=True, help="Tag name or URN.")
@_FORCE_OPTION
@upgrade.check_upgrade
def tag_delete(tag_name: str, force: bool) -> None:
    """Delete a tag."""
    _delete(make_tag_urn(tag_name), "deleteTag", force)


# --- glossary ------------------------------------------------------------------------


@click.group()
def glossary() -> None:
    """Create, list and delete glossary terms and nodes.

    To attach a term to an entity, use `datahub edit add-term`.
    """


_PARENT_OPTION = click.option(
    "--parent", default=None, help="Parent glossary node id or URN."
)


@glossary.command("create-term")
@click.option("--name", required=True, help="Display name.")
@click.option(
    "--id", "term_id", default=None, help="Term id in the URN. Random if omitted."
)
@click.option("--description", default=None)
@_PARENT_OPTION
@upgrade.check_upgrade
def create_term(
    name: str, term_id: Optional[str], description: Optional[str], parent: Optional[str]
) -> None:
    """Create a glossary term."""
    with get_default_graph(ClientMode.CLI) as graph:
        urn = _create(
            graph,
            "createGlossaryTerm",
            "CreateGlossaryEntityInput",
            {
                "id": term_id,
                "name": name,
                "description": description,
                "parentNode": _make_node_urn(parent) if parent else None,
            },
        )
    _echo({"created": urn})


@glossary.command("create-node")
@click.option("--name", required=True, help="Display name.")
@click.option(
    "--id", "node_id", default=None, help="Node id in the URN. Random if omitted."
)
@click.option("--description", default=None)
@_PARENT_OPTION
@upgrade.check_upgrade
def create_node(
    name: str, node_id: Optional[str], description: Optional[str], parent: Optional[str]
) -> None:
    """Create a glossary node (a folder of terms)."""
    with get_default_graph(ClientMode.CLI) as graph:
        urn = _create(
            graph,
            "createGlossaryNode",
            "CreateGlossaryEntityInput",
            {
                "id": node_id,
                "name": name,
                "description": description,
                "parentNode": _make_node_urn(parent) if parent else None,
            },
        )
    _echo({"created": urn})


@glossary.command("list")
@click.option("--nodes", is_flag=True, help="List nodes instead of terms.")
@_QUERY_OPTION
@_LIMIT_OPTION
@upgrade.check_upgrade
def glossary_list(nodes: bool, query: str, limit: int) -> None:
    """List glossary terms (or nodes)."""
    _list("GLOSSARY_NODE" if nodes else "GLOSSARY_TERM", query, limit)


@glossary.command("delete")
@click.option("--urn", required=True, help="Glossary term or node URN.")
@_FORCE_OPTION
@upgrade.check_upgrade
def glossary_delete(urn: str, force: bool) -> None:
    """Delete a glossary term or node."""
    if not urn.startswith(("urn:li:glossaryTerm:", "urn:li:glossaryNode:")):
        # A bare id is ambiguous between a term and a node, so require the URN.
        raise click.UsageError(
            f"Expected a glossaryTerm or glossaryNode URN, got {urn!r} "
            f"(for a term, that is {make_term_urn(urn)})"
        )
    _delete(urn, "deleteGlossaryEntity", force)


# --- domains -------------------------------------------------------------------------


@click.group()
def domain() -> None:
    """Create, list and delete domains. To assign one, use `datahub edit set-domain`."""


@domain.command("create")
@click.option("--name", required=True, help="Display name.")
@click.option(
    "--id", "domain_id", default=None, help="Domain id in the URN. Random if omitted."
)
@click.option("--description", default=None)
@click.option("--parent", default=None, help="Parent domain id or URN.")
@upgrade.check_upgrade
def domain_create(
    name: str,
    domain_id: Optional[str],
    description: Optional[str],
    parent: Optional[str],
) -> None:
    """Create a domain."""
    with get_default_graph(ClientMode.CLI) as graph:
        urn = _create(
            graph,
            "createDomain",
            "CreateDomainInput",
            {
                "id": domain_id,
                "name": name,
                "description": description,
                "parentDomain": make_domain_urn(parent) if parent else None,
            },
        )
    _echo({"created": urn})


@domain.command("list")
@_QUERY_OPTION
@_LIMIT_OPTION
@upgrade.check_upgrade
def domain_list(query: str, limit: int) -> None:
    """List domains."""
    _list("DOMAIN", query, limit)


@domain.command("delete")
@click.option("--domain", "domain_name", required=True, help="Domain id or URN.")
@_FORCE_OPTION
@upgrade.check_upgrade
def domain_delete(domain_name: str, force: bool) -> None:
    """Delete a domain. DataHub refuses while it has child domains."""
    _delete(make_domain_urn(domain_name), "deleteDomain", force)
