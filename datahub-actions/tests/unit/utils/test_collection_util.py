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

from datahub_actions.utils.collection_util import flatten_dict, flatten_list


def test_flatten_dict():
    assert flatten_dict(
        {
            "entity": {
                "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)",
                "type": "DATASET",
            },
            "created": {
                "time": 1645021517917,
                "actor": {"urn": "urn:li:corpuser:admin"},
            },
            "urn": "urn:li:actionRequest:03c3fdc7-3147-48a1-af84-a1af1a45a202",
            "type": "TERM_ASSOCIATION",
            "params": {
                "glossaryTermProposal": {
                    "glossaryTerm": {"urn": "urn:li:glossaryTerm:CustomerAccount"}
                },
                "tagProposal": None,
            },
        }
    ) == {
        "entity_urn": "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)",
        "entity_type": "DATASET",
        "created_time": 1645021517917,
        "created_actor_urn": "urn:li:corpuser:admin",
        "urn": "urn:li:actionRequest:03c3fdc7-3147-48a1-af84-a1af1a45a202",
        "type": "TERM_ASSOCIATION",
        "params_glossaryTermProposal_glossaryTerm_urn": "urn:li:glossaryTerm:CustomerAccount",
        "params_tagProposal": None,
    }


def test_flatten_list():
    assert flatten_list([["a"], ["b"], ["c"]]) == ["a", "b", "c"]
