import json
import logging
from typing import Optional

import click
from click_default_group import DefaultGroup

from datahub.api.entities.document import Document
from datahub.api.entities.document.document import (
    DocumentOperationError,
    DocumentValidationError,
)
from datahub.ingestion.graph.client import get_default_graph
from datahub.ingestion.graph.config import ClientMode
from datahub.upgrade import upgrade

logger = logging.getLogger(__name__)


def _get_document_client() -> Document:
    """Get a configured Document API client."""
    try:
        graph = get_default_graph(ClientMode.CLI)
        return Document(graph=graph)
    except Exception as e:
        raise click.UsageError(
            f"Failed to initialize DataHub client. Have you run 'datahub init'? Error: {e}"
        ) from e


@click.group(cls=DefaultGroup, default="list")
def document() -> None:
    """Manage documents in DataHub."""
    pass


@document.command()
@click.option(
    "--text",
    required=False,
    type=str,
    help="The text content of the document",
)
@click.option(
    "--title",
    type=str,
    help="Optional title for the document",
)
@click.option(
    "--id",
    type=str,
    help="Optional custom ID for the document (defaults to auto-generated UUID)",
)
@click.option(
    "--sub-type",
    type=str,
    help='Optional sub-type (e.g., "FAQ", "Tutorial", "Reference")',
)
@click.option(
    "--state",
    type=click.Choice(["PUBLISHED", "UNPUBLISHED"], case_sensitive=False),
    default="UNPUBLISHED",
    help="Document state (default: UNPUBLISHED)",
)
@click.option(
    "--parent",
    type=str,
    help="Optional URN of the parent document",
)
@click.option(
    "--related-asset",
    multiple=True,
    type=str,
    help="Optional related asset URN (can be specified multiple times)",
)
@click.option(
    "--related-document",
    multiple=True,
    type=str,
    help="Optional related document URN (can be specified multiple times)",
)
@click.option(
    "--owner",
    multiple=True,
    type=str,
    help='Optional owner in format "urn:type" (e.g., "urn:li:corpuser:user1:TECHNICAL_OWNER")',
)
@click.option(
    "--file",
    "-f",
    type=click.File("r"),
    help="Read text content from a file instead of --text",
)
@upgrade.check_upgrade
def create(
    text: Optional[str],
    title: Optional[str],
    id: Optional[str],
    sub_type: Optional[str],
    state: str,
    parent: Optional[str],
    related_asset: tuple,
    related_document: tuple,
    owner: tuple,
    file: Optional[click.utils.LazyFile],
) -> None:
    """Create a new document in DataHub."""

    try:
        # Get text content from file if provided
        if file:
            text = file.read()  # type: ignore[attr-defined]
        elif not text:
            raise click.UsageError("Either --text or --file must be provided")

        # Parse owners if provided
        owners = None
        if owner:
            owners = []
            for owner_str in owner:
                parts = owner_str.rsplit(":", 1)
                if len(parts) != 2:
                    raise click.UsageError(
                        f"Invalid owner format: {owner_str}. Expected 'urn:type'"
                    )
                owners.append({"owner": parts[0], "type": parts[1]})

        client = _get_document_client()

        urn = client.create(
            text=text,
            title=title,
            id=id,
            sub_type=sub_type,
            state=state.upper(),
            owners=owners,
            parent_document=parent,
            related_assets=list(related_asset) if related_asset else None,
            related_documents=list(related_document) if related_document else None,
        )

        click.echo(f"✅ Successfully created document: {urn}")
    except DocumentValidationError as e:
        click.echo(f"❌ Validation error: {e}", err=True)
        raise click.Abort() from e
    except DocumentOperationError as e:
        click.echo(f"❌ Operation failed: {e}", err=True)
        raise click.Abort() from e
    except Exception as e:
        click.echo(f"❌ Unexpected error: {e}", err=True)
        raise click.Abort() from e


@document.command()
@click.argument("urn", type=str)
@click.option(
    "--text",
    type=str,
    help="New text content for the document",
)
@click.option(
    "--title",
    type=str,
    help="New title for the document",
)
@click.option(
    "--sub-type",
    type=str,
    help='New sub-type for the document (e.g., "FAQ", "Tutorial")',
)
@click.option(
    "--file",
    "-f",
    type=click.File("r"),
    help="Read text content from a file instead of --text",
)
@upgrade.check_upgrade
def update(
    urn: str,
    text: Optional[str],
    title: Optional[str],
    sub_type: Optional[str],
    file: Optional[click.utils.LazyFile],
) -> None:
    """Update the contents and/or title of an existing document."""

    try:
        # Get text content from file if provided
        if file:
            text = file.read()  # type: ignore[attr-defined]

        if not text and not title and not sub_type:
            raise click.UsageError(
                "At least one of --text, --title, --sub-type, or --file must be provided"
            )

        client = _get_document_client()

        success = client.update(
            urn=urn,
            text=text,
            title=title,
            sub_type=sub_type,
        )

        if success:
            click.echo(f"✅ Successfully updated document: {urn}")
        else:
            click.echo(f"❌ Failed to update document: {urn}", err=True)
    except DocumentValidationError as e:
        click.echo(f"❌ Validation error: {e}", err=True)
        raise click.Abort() from e
    except DocumentOperationError as e:
        click.echo(f"❌ Operation failed: {e}", err=True)
        raise click.Abort() from e
    except Exception as e:
        click.echo(f"❌ Unexpected error: {e}", err=True)
        raise click.Abort() from e


@document.command()
@click.argument("urn", type=str)
@click.option(
    "--format",
    type=click.Choice(["json", "text"], case_sensitive=False),
    default="json",
    help="Output format (default: json)",
)
@upgrade.check_upgrade
def get(urn: str, format: str) -> None:
    """Get a document by its URN."""

    try:
        client = _get_document_client()
        doc = client.get(urn=urn)

        if not doc:
            click.echo(f"❌ Document not found: {urn}", err=True)
            raise click.Abort()

        if format.lower() == "json":
            click.echo(json.dumps(doc, indent=2))
        else:
            # Text format - show key information
            info = doc.get("info", {})
            click.echo(f"URN: {doc.get('urn')}")
            click.echo(f"Type: {doc.get('type')}")
            if doc.get("subType"):
                click.echo(f"Sub-Type: {doc.get('subType')}")
            if info.get("title"):
                click.echo(f"Title: {info.get('title')}")
            if info.get("status"):
                click.echo(f"Status: {info['status'].get('state')}")
            click.echo(f"\nContent:\n{info.get('contents', {}).get('text', '')}")
    except DocumentValidationError as e:
        click.echo(f"❌ Validation error: {e}", err=True)
        raise click.Abort() from e
    except DocumentOperationError as e:
        click.echo(f"❌ Operation failed: {e}", err=True)
        raise click.Abort() from e
    except Exception as e:
        click.echo(f"❌ Unexpected error: {e}", err=True)
        raise click.Abort() from e


@document.command()
@click.option(
    "--query",
    "-q",
    type=str,
    help="Semantic search query",
)
@click.option(
    "--start",
    type=int,
    default=0,
    help="Starting offset for pagination (default: 0)",
)
@click.option(
    "--count",
    "-n",
    type=int,
    default=10,
    help="Number of results to return (default: 10)",
)
@click.option(
    "--type",
    "types",
    multiple=True,
    type=str,
    help="Filter by document sub-type (can be specified multiple times)",
)
@click.option(
    "--domain",
    "domains",
    multiple=True,
    type=str,
    help="Filter by domain URN (can be specified multiple times)",
)
@click.option(
    "--state",
    "states",
    multiple=True,
    type=click.Choice(["PUBLISHED", "UNPUBLISHED"], case_sensitive=False),
    help="Filter by document state (can be specified multiple times)",
)
@click.option(
    "--parent",
    type=str,
    help="Filter by parent document URN",
)
@click.option(
    "--root-only",
    is_flag=True,
    help="Only return root-level documents (no parent)",
)
@click.option(
    "--include-drafts",
    is_flag=True,
    help="Include draft documents in results",
)
@click.option(
    "--format",
    type=click.Choice(["json", "table"], case_sensitive=False),
    default="table",
    help="Output format (default: table)",
)
@upgrade.check_upgrade
def search(
    query: Optional[str],
    start: int,
    count: int,
    types: tuple,
    domains: tuple,
    states: tuple,
    parent: Optional[str],
    root_only: bool,
    include_drafts: bool,
    format: str,
) -> None:
    """Search for documents."""

    try:
        client = _get_document_client()

        results = client.search(
            query=query,
            start=start,
            count=count,
            types=list(types) if types else None,
            domains=list(domains) if domains else None,
            states=[s.upper() for s in states] if states else None,
            parent_document=parent,
            root_only=root_only,
            include_drafts=include_drafts,
        )

        if format.lower() == "json":
            click.echo(json.dumps(results, indent=2))
        else:
            # Table format
            total = results.get("total", 0)
            documents = results.get("documents", [])

            click.echo(f"Found {total} document(s)\n")

            if documents:
                for doc in documents:
                    info = doc.get("info", {})
                    status = info.get("status", {}).get("state", "UNKNOWN")
                    title = info.get("title", "(No title)")
                    urn = doc.get("urn", "")
                    sub_type = doc.get("subType", "")

                    click.echo(f"• {title}")
                    click.echo(f"  URN: {urn}")
                    if sub_type:
                        click.echo(f"  Type: {sub_type}")
                    click.echo(f"  Status: {status}")

                    # Show snippet of content
                    content = info.get("contents", {}).get("text", "")
                    if content:
                        snippet = content[:100].replace("\n", " ")
                        if len(content) > 100:
                            snippet += "..."
                        click.echo(f"  Content: {snippet}")

                    click.echo()
    except DocumentValidationError as e:
        click.echo(f"❌ Validation error: {e}", err=True)
        raise click.Abort() from e
    except DocumentOperationError as e:
        click.echo(f"❌ Operation failed: {e}", err=True)
        raise click.Abort() from e
    except Exception as e:
        click.echo(f"❌ Unexpected error: {e}", err=True)
        raise click.Abort() from e


@document.command()
@click.argument("urn", type=str)
@upgrade.check_upgrade
def publish(urn: str) -> None:
    """Publish a document (make it visible)."""

    try:
        client = _get_document_client()
        success = client.publish(urn=urn)

        if success:
            click.echo(f"✅ Successfully published document: {urn}")
        else:
            click.echo(f"❌ Failed to publish document: {urn}", err=True)
    except DocumentValidationError as e:
        click.echo(f"❌ Validation error: {e}", err=True)
        raise click.Abort() from e
    except DocumentOperationError as e:
        click.echo(f"❌ Operation failed: {e}", err=True)
        raise click.Abort() from e
    except Exception as e:
        click.echo(f"❌ Unexpected error: {e}", err=True)
        raise click.Abort() from e


@document.command()
@click.argument("urn", type=str)
@upgrade.check_upgrade
def unpublish(urn: str) -> None:
    """Unpublish a document (make it not visible)."""

    try:
        client = _get_document_client()
        success = client.unpublish(urn=urn)

        if success:
            click.echo(f"✅ Successfully unpublished document: {urn}")
        else:
            click.echo(f"❌ Failed to unpublish document: {urn}", err=True)
    except DocumentValidationError as e:
        click.echo(f"❌ Validation error: {e}", err=True)
        raise click.Abort() from e
    except DocumentOperationError as e:
        click.echo(f"❌ Operation failed: {e}", err=True)
        raise click.Abort() from e
    except Exception as e:
        click.echo(f"❌ Unexpected error: {e}", err=True)
        raise click.Abort() from e


@document.command()
@click.argument("urn", type=str)
@click.option(
    "--force",
    is_flag=True,
    help="Skip confirmation prompt",
)
@upgrade.check_upgrade
def delete(urn: str, force: bool) -> None:
    """Delete a document."""

    try:
        if not force:
            if not click.confirm(f"Are you sure you want to delete {urn}?"):
                click.echo("Cancelled.")
                return

        client = _get_document_client()
        success = client.delete(urn=urn)

        if success:
            click.echo(f"✅ Successfully deleted document: {urn}")
        else:
            click.echo(f"❌ Failed to delete document: {urn}", err=True)
    except DocumentValidationError as e:
        click.echo(f"❌ Validation error: {e}", err=True)
        raise click.Abort() from e
    except DocumentOperationError as e:
        click.echo(f"❌ Operation failed: {e}", err=True)
        raise click.Abort() from e
    except Exception as e:
        click.echo(f"❌ Unexpected error: {e}", err=True)
        raise click.Abort() from e


# Alias 'list' to 'search' with no arguments
@document.command(name="list")
@click.option(
    "--count",
    "-n",
    type=int,
    default=20,
    help="Number of results to return (default: 20)",
)
@click.option(
    "--state",
    "states",
    multiple=True,
    type=click.Choice(["PUBLISHED", "UNPUBLISHED"], case_sensitive=False),
    help="Filter by document state (can be specified multiple times)",
)
@click.option(
    "--format",
    type=click.Choice(["json", "table"], case_sensitive=False),
    default="table",
    help="Output format (default: table)",
)
@upgrade.check_upgrade
def list_documents(count: int, states: tuple, format: str) -> None:
    """List documents (alias for search with default parameters)."""

    try:
        client = _get_document_client()

        results = client.search(
            start=0,
            count=count,
            states=[s.upper() for s in states] if states else None,
        )

        if format.lower() == "json":
            click.echo(json.dumps(results, indent=2))
        else:
            # Table format
            total = results.get("total", 0)
            documents = results.get("documents", [])

            click.echo(f"Found {total} document(s)\n")

            if documents:
                for doc in documents:
                    info = doc.get("info", {})
                    status = info.get("status", {}).get("state", "UNKNOWN")
                    title = info.get("title", "(No title)")
                    urn = doc.get("urn", "")
                    sub_type = doc.get("subType", "")

                    click.echo(f"• {title}")
                    click.echo(f"  URN: {urn}")
                    if sub_type:
                        click.echo(f"  Type: {sub_type}")
                    click.echo(f"  Status: {status}")
                    click.echo()
    except DocumentValidationError as e:
        click.echo(f"❌ Validation error: {e}", err=True)
        raise click.Abort() from e
    except DocumentOperationError as e:
        click.echo(f"❌ Operation failed: {e}", err=True)
        raise click.Abort() from e
    except Exception as e:
        click.echo(f"❌ Unexpected error: {e}", err=True)
        raise click.Abort() from e
