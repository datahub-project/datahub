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


class FilterStats:
    exception_count: int
    processed_count: int
    filtered_count: int

    def __init__(self) -> None:
        self.exception_count = 0
        self.processed_count = 0
        self.filtered_count = 0

    def increment_exception_count(self) -> None:
        self.exception_count = self.exception_count + 1

    def increment_processed_count(self) -> None:
        self.processed_count = self.processed_count + 1

    def increment_filtered_count(self) -> None:
        self.filtered_count = self.filtered_count + 1

    def get_exception_count(self) -> int:
        return self.exception_count

    def get_processed_count(self) -> int:
        return self.processed_count

    def get_filtered_count(self) -> int:
        return self.filtered_count

    def as_string(self) -> str:
        return json.dumps(
            {
                "exception_count": self.exception_count,
                "processed_count": self.processed_count,
                "filtered_count": self.filtered_count,
            },
            indent=4,
            sort_keys=True,
        )
