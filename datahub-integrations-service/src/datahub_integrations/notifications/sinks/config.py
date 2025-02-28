from typing import Any, Dict


class NotificationSinkConfig:
    """
    Configuration for a notification sink provided via application configuration at boot time.

    Attributes:
        static_config (Dict[str, Any]): Static configuration for a notification sink.
        base_url (str): The base URL where DataHub is deployed.
    """

    def __init__(self, static_config: Dict[str, Any], base_url: str):
        self.static_config = static_config
        self.base_url = base_url
