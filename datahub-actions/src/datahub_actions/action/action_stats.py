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

import json


# Class that stores running statistics for a single Action.
# TODO: Invocation time tracking.
class ActionStats:
    # The number of exception raised by the Action.
    exception_count: int = 0

    # The number of events that were actually submitted to the Action
    success_count: int = 0

    def increment_exception_count(self) -> None:
        self.exception_count = self.exception_count + 1

    def get_exception_count(self) -> int:
        return self.exception_count

    def increment_success_count(self) -> None:
        self.success_count = self.success_count + 1

    def get_success_count(self) -> int:
        return self.success_count

    def as_string(self) -> str:
        return json.dumps(self.__dict__, indent=4, sort_keys=True)
