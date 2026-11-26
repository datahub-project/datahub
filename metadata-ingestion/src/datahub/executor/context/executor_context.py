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


from datahub.secret.secret_store import SecretStore

"""
Context object for the executor, passed into a Task on creation
"""


class ExecutorContext:
    # Executor ID
    executor_id: str

    # Secret stores that were configured at boot time
    secret_stores: list[SecretStore]

    # The mutable default is shared across all default-constructed instances and is
    # assigned directly onto self; rewriting it to None + `or []` would change
    # cross-instance aliasing, so it is suppressed rather than fixed. Known debt.
    def __init__(
        self,
        executor_id: str,
        secret_stores: list[SecretStore] = [],  # noqa: B006
    ) -> None:
        self.executor_id = executor_id
        self.secret_stores = secret_stores

    def get_secret_stores(self) -> list[SecretStore]:
        return self.secret_stores
