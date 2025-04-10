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
import logging
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

ASPECT_OWNERSHIP = "ownership"
ASPECT_TAGS = "globalTags"
ASPECT_GLOSSARY_TERMS = "glossaryTerms"
ASPECT_EDITABLE_SCHEMAMETADATA = "editableSchemaMetadata"
ASPECT_DATAHUB_EXECUTION_REQUEST_RESULT = "dataHubExecutionRequestResult"

logger = logging.getLogger(__name__)


def get_aspect_val_as_json(aspect: Optional[Tuple[str, Dict]]) -> Union[None, Dict]:
    if aspect is None or len(aspect) < 2 or aspect[1] is None:
        return None
    aspect_val_str = aspect[1].get("value")
    if aspect_val_str is None:
        return None
    return json.loads(aspect_val_str)


def get_nested_key(in_dict: Any, paths: List[str]) -> List[Dict]:
    curr_val: Any = in_dict
    for i, path in enumerate(paths):
        if path == "*":
            if isinstance(curr_val, list):
                curr_val_clone = curr_val[:]
                curr_val = []
                for elem in curr_val_clone:
                    curr_val.extend(get_nested_key(elem, paths[i + 1 :]))
                return curr_val
        else:
            curr_val = curr_val[path]
    return curr_val


def get_value(aspect: Tuple[str, Dict], path_key: str) -> List[Dict]:
    aspect_val_json = get_aspect_val_as_json(aspect)
    if not aspect_val_json:
        return []
    return get_nested_key(aspect_val_json, path_key.split("/"))


def _get_cur_prev_urns(
    curr_val: List[Dict], prev_val: Optional[List[Dict]], urn_key: str
) -> Tuple[Set[str], Set[str]]:
    curr_urns = set(map(lambda x: x[urn_key], curr_val))
    prev_urns = set()
    if prev_val is not None:
        prev_urns = set(map(lambda x: x[urn_key], prev_val))
    return curr_urns, prev_urns


def get_added_removed_urns(
    curr_urns: Set[str], prev_urns: Set[str]
) -> Tuple[Set[str], Set[str]]:
    return curr_urns - prev_urns, prev_urns - curr_urns


def get_added_removed_objs_from_aspect(
    aspect: Tuple[str, Dict],
    prev_aspect: Optional[Tuple[str, Dict]],
    path_key: str,
    urn_key: str,
) -> Tuple[List, List, List]:
    assert aspect
    curr_vals = get_value(aspect, path_key)
    if prev_aspect is None:
        prev_vals = []
    else:
        prev_vals = get_value(prev_aspect, path_key)

    curr_urns, prev_urns = _get_cur_prev_urns(curr_vals, prev_vals, urn_key)
    added_urns, removed_urns = get_added_removed_urns(curr_urns, prev_urns)

    added_objs = list(filter(lambda x: x[urn_key] in added_urns, curr_vals))
    removed_objs = list(filter(lambda x: x[urn_key] in removed_urns, prev_vals))

    return added_objs, removed_objs, []


def get_delta_from_mcl_ownership_aspect(
    aspect: Tuple[str, Dict], prev_aspect: Optional[Tuple[str, Dict]]
) -> Tuple[List, List, List]:
    return get_added_removed_objs_from_aspect(aspect, prev_aspect, "owners", "owner")


def get_delta_from_mcl_global_tags_aspect(
    aspect: Tuple[str, Dict], prev_aspect: Optional[Tuple[str, Dict]]
) -> Tuple[List, List, List]:
    return get_added_removed_objs_from_aspect(aspect, prev_aspect, "tags", "tag")


def get_delta_from_mcl_editable_schemametadata_aspect(
    aspect: Tuple[str, Dict], prev_aspect: Optional[Tuple[str, Dict]]
) -> Tuple[List, List, List]:
    added = []
    removed = []
    changed: List[Any] = []

    for path_key, item_key in [
        ("editableSchemaFieldInfo/*/glossaryTerms/terms", "urn"),
        ("editableSchemaFieldInfo/*/globalTags/tags", "tag"),
    ]:
        try:
            added_objs, removed_objs, changed_objs = get_added_removed_objs_from_aspect(
                aspect, prev_aspect, path_key, item_key
            )
            added.extend(added_objs)
            removed.extend(removed_objs)
            changed.extend(changed_objs)
        except KeyError:
            pass

    return added, removed, changed


def get_delta_from_mcl_glossary_terms_aspect(
    aspect: Tuple[str, Dict], prev_aspect: Optional[Tuple[str, Dict]]
) -> Tuple[List, List, List]:
    return get_added_removed_objs_from_aspect(aspect, prev_aspect, "terms", "urn")


def get_delta_from_mcl_dataHubExecutionRequestResult_aspect(
    aspect: Tuple[str, Dict], prev_aspect: Optional[Tuple[str, Dict]]
) -> Tuple[List, List, List]:
    aspect_val_json = get_aspect_val_as_json(aspect)
    return [], [], [aspect_val_json]


RECOGNIZED_ASPECT_TO_EXTRACT_DELTA = {
    ASPECT_OWNERSHIP: get_delta_from_mcl_ownership_aspect,
    ASPECT_TAGS: get_delta_from_mcl_global_tags_aspect,
    ASPECT_GLOSSARY_TERMS: get_delta_from_mcl_glossary_terms_aspect,
    ASPECT_EDITABLE_SCHEMAMETADATA: get_delta_from_mcl_editable_schemametadata_aspect,
    ASPECT_DATAHUB_EXECUTION_REQUEST_RESULT: get_delta_from_mcl_dataHubExecutionRequestResult_aspect,
}


def get_helper_for_asepct(aspect_name: str) -> Union[None, Callable]:
    return RECOGNIZED_ASPECT_TO_EXTRACT_DELTA.get(aspect_name, None)
