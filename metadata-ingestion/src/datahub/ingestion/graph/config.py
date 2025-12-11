# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from enum import Enum, auto
from typing import Dict, List, Optional

from pydantic import ConfigDict

from datahub.configuration.common import ConfigModel
from datahub.configuration.env_vars import get_datahub_component


class ClientMode(Enum):
    INGESTION = auto()
    CLI = auto()
    SDK = auto()


DATAHUB_COMPONENT_ENV: str = get_datahub_component().lower()


class DatahubClientConfig(ConfigModel):
    """Configuration class for holding connectivity to datahub gms"""

    server: str
    token: Optional[str] = None
    timeout_sec: Optional[float] = None
    retry_status_codes: Optional[List[int]] = None
    retry_max_times: Optional[int] = None
    extra_headers: Optional[Dict[str, str]] = None
    ca_certificate_path: Optional[str] = None
    client_certificate_path: Optional[str] = None
    disable_ssl_verification: bool = False
    openapi_ingestion: Optional[bool] = None
    client_mode: Optional[ClientMode] = None
    datahub_component: Optional[str] = None
    server_config_refresh_interval: Optional[int] = None

    model_config = ConfigDict(extra="ignore")
