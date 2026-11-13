import json
import logging
from pathlib import Path

import click
from click_default_group import DefaultGroup
from ruamel.yaml import YAML

from datahub.api.entities.domain.domain import (
    Domain,
    find_domain_urns,
    validate_parent_domain,
)
from datahub.cli.specific.dataproduct_cli import (
    _abort_if_non_existent_urn,
    _get_owner_urn,
)
from datahub.emitter.mce_builder import make_domain_urn, validate_ownership_type
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import get_default_graph
from datahub.ingestion.graph.config import ClientMode
from datahub.metadata.schema_classes import (
    DomainPropertiesClass,
    OwnerClass,
    OwnershipTypeClass,
)
from datahub.specific.domain import DomainPatchBuilder
from datahub.upgrade import upgrade

logger = logging.getLogger(__name__)


@click.group(cls=DefaultGroup, default="upsert")
def domain() -> None:
    """A group of commands to interact with the Domain entity in DataHub."""


def _mutate(file: Path, upsert: bool) -> None:
    with get_default_graph(ClientMode.CLI) as graph:
        try:
            loaded = Domain.from_yaml(Path(file), graph)
        except ValueError as e:
            raise click.ClickException(str(e)) from e
        if upsert and not graph.exists(loaded.urn):
            logger.info(f"Domain {loaded.urn} does not exist, will create.")
            upsert = False
        existing = (
            None if upsert else graph.get_aspect(loaded.urn, DomainPropertiesClass)
        )
        try:
            for mcp in loaded.generate_mcp(upsert=upsert):
                if (
                    existing
                    and isinstance(mcp, MetadataChangeProposalWrapper)
                    and isinstance(mcp.aspect, DomainPropertiesClass)
                ):
                    # The YAML can't express these, so a full replace keeps them.
                    mcp.aspect.created = existing.created
                    mcp.aspect.customProperties = existing.customProperties
                    # Without a display_name the snapshot would fall back to
                    # the id, renaming the domain.
                    if loaded.display_name is None:
                        mcp.aspect.name = existing.name
                graph.emit(mcp)
            click.secho(f"Update succeeded for urn {loaded.urn}.", fg="green")
        except Exception as e:
            # Exit non-zero so CI/GitOps pipelines see a failed apply.
            raise click.ClickException(
                f"Update failed for id {loaded.id} due to {e}"
            ) from e


@domain.command(name="upsert")
@click.option("-f", "--file", required=True, type=click.Path(exists=True))
@upgrade.check_upgrade
def upsert(file: Path) -> None:
    """Create a Domain, or patch the name, description and parent the YAML sets
    on an existing one. Owners and links in the YAML replace the existing ones."""
    _mutate(file, upsert=True)


@domain.command(name="update")
@click.option("-f", "--file", required=True, type=click.Path(exists=True))
@upgrade.check_upgrade
def update(file: Path) -> None:
    """Create a Domain, or replace its properties with the YAML's: an omitted
    description or parent is cleared, while the name, created stamp and custom
    properties are kept. Owners and links are replaced if the YAML sets them and
    left as they are otherwise; use upsert for partial updates."""
    _mutate(file, upsert=False)


@domain.command(name="get")
@click.option("--urn", required=True, type=str)
@click.option("--to-file", required=False, type=str)
@upgrade.check_upgrade
def get(urn: str, to_file: str) -> None:
    """Print a Domain from DataHub as JSON; optionally write a YAML copy."""
    urn = make_domain_urn(urn)
    with get_default_graph(ClientMode.CLI) as graph:
        if not graph.exists(urn):
            raise click.ClickException(f"Domain {urn} does not exist")
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
@upgrade.check_upgrade
def delete(urn: str, file: Path) -> None:
    """Delete a Domain and remove references to it.

    Always a hard delete: the domain entity has no ``status`` aspect, so it
    cannot be soft-deleted. Like the UI, refuses a domain with child domains.
    """
    if not urn and not file:
        click.secho("Must provide either an urn or a file to delete a domain", fg="red")
        raise click.Abort()

    if urn:
        domain_urn = make_domain_urn(urn)
    else:
        # Only the id is needed, so don't resolve or validate the parent.
        try:
            with open(file) as fp:
                domain_urn = Domain.parse_obj_allow_extras(
                    YAML(typ="safe").load(fp)
                ).urn
        except ValueError as e:
            raise click.ClickException(str(e)) from e

    with get_default_graph(ClientMode.CLI) as graph:
        _abort_if_non_existent_urn(graph, domain_urn, "delete")
        # Deleting the references would otherwise strip parentDomain from the
        # children and silently make them top-level.
        children = find_domain_urns(graph, parent_domain_urn=domain_urn)
        if children:
            raise click.ClickException(
                f"Cannot delete {domain_urn}: it has child domains "
                f"({', '.join(sorted(children))}). Move or delete them first."
            )
        # Entity first, then references (as GraphQL deleteDomain does), so a
        # failed delete doesn't leave the domain with its references stripped.
        graph.delete_entity(domain_urn, hard=True)
        graph.delete_references_to_urn(domain_urn)
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
    urn = make_domain_urn(urn)
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
    urn = make_domain_urn(urn)
    patch: DomainPatchBuilder = Domain.get_patch_builder(urn)
    with get_default_graph(ClientMode.CLI) as graph:
        _abort_if_non_existent_urn(graph, urn, "set parent")
        if parent:
            parent_urn = make_domain_urn(parent)
            try:
                validate_parent_domain(graph, urn, parent_urn)
            except ValueError as e:
                raise click.ClickException(str(e)) from e
            patch.set_parent_domain(parent_urn)
        else:
            current = graph.get_aspect(urn, DomainPropertiesClass)
            # JSON Patch rejects removing a field that isn't there.
            if not current or not current.parentDomain:
                click.secho(f"Domain {urn} is already a top-level domain.")
                return
            patch.remove_parent_domain()
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
    urn = make_domain_urn(urn)
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
    urn = make_domain_urn(urn)
    patch: DomainPatchBuilder = Domain.get_patch_builder(urn)
    patch.remove_owner(owner=_get_owner_urn(owner_urn))
    with get_default_graph(ClientMode.CLI) as graph:
        _abort_if_non_existent_urn(graph, urn, "remove owners")
        for mcp in patch.build():
            graph.emit(mcp)
