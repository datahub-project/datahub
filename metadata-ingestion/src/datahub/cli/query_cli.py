from typing import Any, Dict, List

import click

from datahub.cli.cli_utils import get_entity


@click.group()
def query() -> None:
    """Query Entity for Details"""
    pass


def _get_aspect_from_entity(entity: Dict, aspect_key: str) -> Dict:
    aspects_list = list(entity["value"].values())[0]["aspects"]
    return list(filter(lambda x: aspect_key in x, aspects_list))[0][aspect_key]


def _get_aspect_value(
    urn: str, aspect: str, aspect_type_name: str, aspect_key: str
) -> List[Dict[Any, Any]]:
    entity = get_entity(urn, [aspect])
    return _get_aspect_from_entity(entity, aspect_type_name)[aspect_key]


@query.command()
@click.option("--urn", required=True, type=str)
def owner(urn: str) -> List[Dict[Any, Any]]:
    """Get Owners for an URN"""
    return _get_aspect_value(
        urn, "ownership", "com.linkedin.common.Ownership", "owners"
    )


@query.command()
@click.option("--urn", required=True, type=str)
def tag(urn: str) -> List[Dict[Any, Any]]:
    """Get Tags for an URN"""
    return _get_aspect_value(
        urn, "globalTags", "com.linkedin.common.GlobalTags", "tags"
    )


@query.command()
@click.option("--urn", required=True, type=str)
def glossary_term(urn: str) -> List[Dict[Any, Any]]:
    """Get Glossary Terms for an URN"""
    return _get_aspect_value(
        urn, "glossaryTerms", "com.linkedin.common.GlossaryTerms", "terms"
    )
