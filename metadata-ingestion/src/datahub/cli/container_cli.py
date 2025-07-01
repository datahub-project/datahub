import logging

import click

from datahub.ingestion.source.apply.datahub_apply import apply_association_to_container

logger = logging.getLogger(__name__)


@click.group()
def container() -> None:
    """A group of commands to interact with containers in DataHub."""
    pass


@container.command()
@click.option("--container-urn", required=True, type=str)
@click.option("--tag-urn", required=True, type=str)
def tag(container_urn: str, tag_urn: str) -> None:
    """Add patch to add a tag to all datasets in a container"""
    apply_association_to_container(container_urn, tag_urn, "tag")


@container.command()
@click.option("--container-urn", required=True, type=str)
@click.option("--term-urn", required=True, type=str)
def term(container_urn: str, term_urn: str) -> None:
    """Add patch to add a term to all datasets in a container"""
    apply_association_to_container(container_urn, term_urn, "term")


@container.command()
@click.option("--container-urn", required=True, type=str)
@click.option("--owner-urn", required=True, type=str)
def owner(container_urn: str, owner_urn: str) -> None:
    """Add patch to add a owner to all datasets in a container"""
    apply_association_to_container(container_urn, owner_urn, "owner")


@container.command()
@click.option("--container-urn", required=True, type=str)
@click.option("--domain-urn", required=True, type=str)
def domain(container_urn: str, domain_urn: str) -> None:
    """Add patch to add a domain to all datasets in a container"""
    apply_association_to_container(container_urn, domain_urn, "domain")
