# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from pydantic import Field, SecretStr

from datahub.configuration import ConfigModel


class AzureAuthConfig(ConfigModel):
    client_secret: SecretStr = Field(
        description="Azure application client secret used for authentication. This is a confidential credential that should be kept secure."
    )
    client_id: str = Field(
        description="Azure application (client) ID. This is the unique identifier for the registered Azure AD application.",
    )
    tenant_id: str = Field(
        description="Azure tenant (directory) ID. This identifies the Azure AD tenant where the application is registered.",
    )
