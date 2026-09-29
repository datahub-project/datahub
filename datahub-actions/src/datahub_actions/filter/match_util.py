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
from typing import Any, Dict, List


def matches(match_val: Any, match_val_to: Any) -> bool:
    if isinstance(match_val, dict):
        return matches_dict(match_val, match_val_to)
    if isinstance(match_val, list):
        return matches_list(match_val, match_val_to)
    return match_val == match_val_to


def matches_list(match_filters: List, match_with: Any) -> bool:
    """Any item in match_filters must equal match_with (OR semantics)."""
    if not isinstance(match_with, str):
        return False
    for f in match_filters:
        if f == match_with:
            return True
    return False


def matches_dict(match_filters: Dict[str, Any], match_with: Any) -> bool:
    """All keys in match_filters must match match_with (AND semantics)."""
    if isinstance(match_with, str):
        try:
            match_with = json.loads(match_with)
        except ValueError:
            pass
    if not isinstance(match_with, dict):
        return False
    for key, val in match_filters.items():
        curr = match_with.get(key)
        if not matches(val, curr):
            return False
    return True
