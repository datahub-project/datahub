# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from typing import IO, cast

import toml

from datahub.configuration.common import ConfigurationMechanism


class TomlConfigurationMechanism(ConfigurationMechanism):
    """Ability to load configuration from toml files"""

    def load_config(self, config_fp: IO) -> dict:
        config = toml.load(config_fp)
        return cast(dict, config)  # converts MutableMapping -> dict
