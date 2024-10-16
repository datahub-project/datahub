# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
from typing import Dict, List, Union

from datahub.secret.secret_store import SecretStore


# Simple SecretStore implementation that fetches Secret values from the local environment.
class EnvironmentSecretStore(SecretStore):
    def __init__(self, config: dict) -> None:
        pass

    def get_secret_values(self, secret_names: List[str]) -> Dict[str, Union[str, None]]:
        values = {}
        for secret_name in secret_names:
            values[secret_name] = os.getenv(secret_name)
        return values

    def get_secret_value(self, secret_name: str) -> Union[str, None]:
        return os.getenv(secret_name)

    def get_id(self) -> str:
        return "env"

    @classmethod
    def create(cls, config: Dict) -> "EnvironmentSecretStore":
        return cls(config)

    def close(self) -> None:
        pass
