from typing import Dict, List, Optional

from datahub.configuration.common import ConfigModel


class DatahubClientConfig(ConfigModel):
    """Configuration class for holding connectivity to datahub gms"""

    # TODO: Having a default for the server doesn't make a ton of sense. This should be handled
    # by callers / the CLI, but the actual client should not have any magic.
    server: str
    token: Optional[str] = None
    timeout_sec: Optional[float] = None
    retry_status_codes: Optional[List[int]] = None
    retry_max_times: Optional[int] = None
    extra_headers: Optional[Dict[str, str]] = None
    ca_certificate_path: Optional[str] = None
    client_certificate_path: Optional[str] = None
    disable_ssl_verification: bool = False
