from datahub.metadata._urns.urn_defs import *  # noqa: F401
from datahub.utilities.urns._urn_base import Urn  # noqa: F401


def guess_entity_type(urn: str) -> str:
    assert urn.startswith("urn:li:"), "urns must start with urn:li:"
    return urn.split(":")[2]
