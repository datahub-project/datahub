from abc import ABC


class Connection(ABC):
    """Base class for a connection to an external source"""

    # The urn of the connection
    urn: str

    # The urn of the platform
    platform_urn: str

    def __init__(self, urn: str, platform_urn: str):
        self.urn = urn
        self.platform_urn = platform_urn
