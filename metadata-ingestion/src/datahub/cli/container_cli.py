import logging

import click

from datahub.ingestion.graph.client import get_default_graph
from datahub.metadata.schema_classes import (
    GlossaryTermAssociationClass,
    OwnerClass,
    OwnershipTypeClass,
    TagAssociationClass,
)
from datahub.specific.dataset import DatasetPatchBuilder

logger = logging.getLogger(__name__)


@click.group()
def container() -> None:
    """A group of commands to interact with containers in DataHub."""
    pass


def apply_association_to_container(
    container_urn: str,
    association_urn: str,
    association_type: str,
) -> None:
    """
    Common function to add either tags, terms, or owners to child datasets (for now).

    Args:
        container_urn: The URN of the container
        association_urn: The URN of the tag, term, or user to apply
        association_type: One of 'tag', 'term', or 'owner'
    """
    urns = []
    graph = get_default_graph()
    logger.info(f"Using {graph}")
    urns.extend(
        graph.get_urns_by_filter(
            container=container_urn, batch_size=1000, entity_types=["dataset"]
        )
    )

    for urn in urns:
        logger.info(f"Adding {association_type} {association_urn} to {urn}")
        builder = DatasetPatchBuilder(urn)

        if association_type == "tag":
            patches = builder.add_tag(TagAssociationClass(association_urn)).build()
        elif association_type == "term":
            patches = builder.add_term(
                GlossaryTermAssociationClass(association_urn)
            ).build()
        elif association_type == "owner":
            patches = builder.add_owner(
                OwnerClass(
                    owner=association_urn,
                    type=OwnershipTypeClass.TECHNICAL_OWNER,
                )
            ).build()

        for mcp in patches:
            graph.emit(mcp)


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
@click.option("--owner-id", required=True, type=str)
def owner(container_urn: str, owner_id: str) -> None:
    """Add patch to add a owner to all datasets in a container"""
    apply_association_to_container(container_urn, owner_id, "owner")
