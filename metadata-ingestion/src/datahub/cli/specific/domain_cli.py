import json
import logging
from pathlib import Path

import click
from click_default_group import DefaultGroup

from datahub.api.entities.domain.domain import Domain
from datahub.emitter.mce_builder import (
    make_group_urn,
    make_user_urn,
    validate_ownership_type,
)
from datahub.ingestion.graph.client import DataHubGraph, get_default_graph
from datahub.ingestion.graph.config import ClientMode
from datahub.metadata.schema_classes import OwnerClass, OwnershipTypeClass
from datahub.specific.domain import DomainPatchBuilder
from datahub.upgrade import upgrade
from datahub.utilities.urns.urn import Urn

logger = logging.getLogger(__name__)


def _get_owner_urn(maybe_urn: str) -> str:
    """Return ``maybe_urn`` if it's already a corpuser/corpGroup URN,
    otherwise mint a corpuser URN."""
    if make_user_urn(maybe_urn) == maybe_urn or make_group_urn(maybe_urn) == maybe_urn:
        return maybe_urn
    if maybe_urn.startswith("urn:li:"):
        raise Exception(
            f"Owner urn {maybe_urn} not recognized as one of the supported types (corpuser, corpGroup)"
        )
    return make_user_urn(maybe_urn)


def _abort_if_non_existent_urn(graph: DataHubGraph, urn: str, operation: str) -> None:
    try:
        parsed_urn: Urn = Urn.from_string(urn)
        entity_type = parsed_urn.get_type()
    except Exception:
        click.secho(f"Provided urn {urn} does not seem valid", fg="red")
        raise click.Abort() from None
    if not graph.exists(urn):
        click.secho(
            f"{entity_type.title()} {urn} does not exist. Will not {operation}.",
            fg="red",
        )
        raise click.Abort()


def _normalize_domain_urn(urn: str) -> str:
    if urn.startswith("urn:li:domain:"):
        return urn
    return f"urn:li:domain:{urn}"


@click.group(cls=DefaultGroup, default="upsert")
def domain() -> None:
    """A group of commands to interact with the Domain entity in DataHub."""


def _mutate(file: Path, upsert: bool) -> None:
    with get_default_graph(ClientMode.CLI) as graph:
        loaded = Domain.from_yaml(Path(file), graph)
        if upsert and not graph.exists(loaded.urn):
            logger.info(f"Domain {loaded.urn} does not exist, will create.")
            upsert = False
        try:
            for mcp in loaded.generate_mcp(upsert=upsert):
                graph.emit(mcp)
            click.secho(f"Update succeeded for urn {loaded.urn}.", fg="green")
        except Exception as e:
            click.secho(
                f"Update failed for id {loaded.id}. due to {e}",
                fg="red",
            )


@domain.command(name="upsert")
@click.option("-f", "--file", required=True, type=click.Path(exists=True))
@upgrade.check_upgrade
def upsert(file: Path) -> None:
    """Upsert a Domain into DataHub (creates if missing, patches otherwise)."""
    _mutate(file, upsert=True)


@domain.command(name="update")
@click.option("-f", "--file", required=True, type=click.Path(exists=True))
@upgrade.check_upgrade
def update(file: Path) -> None:
    """Create or fully replace a Domain in DataHub."""
    _mutate(file, upsert=False)


@domain.command(name="get")
@click.option("--urn", required=True, type=str)
@click.option("--to-file", required=False, type=str)
@upgrade.check_upgrade
def get(urn: str, to_file: str) -> None:
    """Print a Domain from DataHub as JSON; optionally write a YAML copy."""
    urn = _normalize_domain_urn(urn)
    with get_default_graph(ClientMode.CLI) as graph:
        if not graph.exists(urn):
            click.secho(f"Domain {urn} does not exist")
            return
        loaded: Domain = Domain.from_datahub(graph=graph, id=urn)
        click.secho(
            json.dumps(
                loaded.model_dump(exclude_unset=True, exclude_none=True), indent=2
            )
        )
        if to_file:
            loaded.to_yaml(Path(to_file))
            click.secho(f"Domain yaml written to {to_file}", fg="green")


@domain.command(name="delete")
@click.option(
    "--urn", required=False, type=str, help="The urn for the domain to delete"
)
@click.option(
    "-f",
    "--file",
    required=False,
    type=click.Path(exists=True),
    help="A YAML file describing the domain to delete",
)
@click.option("--hard/--soft", required=False, is_flag=True, default=False)
@upgrade.check_upgrade
def delete(urn: str, file: Path, hard: bool) -> None:
    """Delete a Domain. Soft-delete by default; ``--hard`` removes references too."""
    if not urn and not file:
        click.secho("Must provide either an urn or a file to delete a domain", fg="red")
        raise click.Abort()

    with get_default_graph(ClientMode.CLI) as graph:
        domain_urn = _normalize_domain_urn(urn) if urn else ""
        if not urn:
            loaded: Domain = Domain.from_yaml(Path(file), graph)
            domain_urn = loaded.urn

        _abort_if_non_existent_urn(graph, domain_urn, "delete")
        if hard:
            graph.delete_references_to_urn(domain_urn)
        graph.delete_entity(domain_urn, hard=hard)
        click.secho(f"Domain {domain_urn} deleted")


@domain.command(name="set_description")
@click.option("--urn", required=True, type=str)
@click.option("--description", required=False, type=str)
@click.option(
    "--md-file",
    required=False,
    type=click.Path(exists=True),
    help="Markdown file whose contents become the description",
)
@upgrade.check_upgrade
def set_description(urn: str, description: str, md_file: Path) -> None:
    """Set the description of a Domain."""
    urn = _normalize_domain_urn(urn)
    if description is None and not md_file:
        click.secho(
            "Need one of --description or --md-file to populate description",
            fg="red",
        )
        raise click.Abort()
    if description and md_file:
        click.secho(
            "Provide only one of --description or --md-file. You provided both.",
            fg="red",
        )
        raise click.Abort()
    if md_file:
        logger.info(f"Reading description from {md_file}")
        with open(md_file) as fp:
            description = fp.read()

    patch: DomainPatchBuilder = Domain.get_patch_builder(urn)
    patch.set_description(description)
    with get_default_graph(ClientMode.CLI) as graph:
        _abort_if_non_existent_urn(graph, urn, "set description")
        for mcp in patch.build():
            graph.emit(mcp)


@domain.command(name="set_parent")
@click.option("--urn", required=True, type=str)
@click.option(
    "--parent",
    required=False,
    type=str,
    default=None,
    help="Parent domain URN. Omit to make this a top-level domain.",
)
@upgrade.check_upgrade
def set_parent(urn: str, parent: str) -> None:
    """Re-parent a Domain (or promote it to a top-level Domain)."""
    urn = _normalize_domain_urn(urn)
    parent_urn = _normalize_domain_urn(parent) if parent else ""
    patch: DomainPatchBuilder = Domain.get_patch_builder(urn)
    patch.set_parent_domain(parent_urn)
    with get_default_graph(ClientMode.CLI) as graph:
        _abort_if_non_existent_urn(graph, urn, "set parent")
        if parent_urn:
            _abort_if_non_existent_urn(graph, parent_urn, "set parent")
        for mcp in patch.build():
            graph.emit(mcp)


@domain.command(name="add_owner", help="Add an owner to a Domain")
@click.option("--urn", required=True, type=str)
@click.option("--owner", required=True, type=str)
@click.option(
    "--owner-type",
    required=False,
    type=click.Choice(
        [
            OwnershipTypeClass.BUSINESS_OWNER,
            OwnershipTypeClass.TECHNICAL_OWNER,
            OwnershipTypeClass.DATA_STEWARD,
        ],
        case_sensitive=False,
    ),
    default=OwnershipTypeClass.TECHNICAL_OWNER,
)
@upgrade.check_upgrade
def add_owner(urn: str, owner: str, owner_type: str) -> None:
    urn = _normalize_domain_urn(urn)
    patch: DomainPatchBuilder = Domain.get_patch_builder(urn)
    type_str, type_urn = validate_ownership_type(owner_type)
    patch.add_owner(
        owner=OwnerClass(owner=_get_owner_urn(owner), type=type_str, typeUrn=type_urn)
    )
    with get_default_graph(ClientMode.CLI) as graph:
        _abort_if_non_existent_urn(graph, urn, "add owners")
        for mcp in patch.build():
            graph.emit(mcp)


@domain.command(name="remove_owner", help="Remove an owner from a Domain")
@click.option("--urn", required=True, type=str)
@click.argument("owner_urn", required=True, type=str)
@upgrade.check_upgrade
def remove_owner(urn: str, owner_urn: str) -> None:
    urn = _normalize_domain_urn(urn)
    patch: DomainPatchBuilder = Domain.get_patch_builder(urn)
    patch.remove_owner(owner=_get_owner_urn(owner_urn))
    with get_default_graph(ClientMode.CLI) as graph:
        _abort_if_non_existent_urn(graph, urn, "remove owners")
        for mcp in patch.build():
            graph.emit(mcp)
