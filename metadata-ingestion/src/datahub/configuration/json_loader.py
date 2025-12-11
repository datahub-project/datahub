# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import json
from typing import IO

from datahub.configuration.common import ConfigurationMechanism


class JsonConfigurationMechanism(ConfigurationMechanism):
    """Ability to load configuration from json files"""

    def load_config(self, config_fp: IO) -> dict:
        return json.load(config_fp)
