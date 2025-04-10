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

# type: ignore
from datahub_actions.utils.delta_extractor_mcl import (
    get_aspect_val_as_json,
    get_delta_from_mcl_editable_schemametadata_aspect,
    get_delta_from_mcl_global_tags_aspect,
    get_delta_from_mcl_glossary_terms_aspect,
    get_delta_from_mcl_ownership_aspect,
    get_nested_key,
    get_value,
)


def test_get_nested_key():
    assert get_nested_key(
        {
            "owners": [
                {
                    "owner": "urn:li:corpuser:aseem.bansal",
                    "type": "DATAOWNER",
                    "source": {"type": "MANUAL"},
                }
            ],
            "lastModified": {
                "actor": "urn:li:corpuser:admin",
                "time": 1644305321992,
            },
        },
        "owners".split("/"),
    ) == [
        {
            "owner": "urn:li:corpuser:aseem.bansal",
            "type": "DATAOWNER",
            "source": {"type": "MANUAL"},
        }
    ]


def test_get_aspect_val_as_json_with_invalid_or_missing_val():
    assert get_aspect_val_as_json(None) is None
    assert get_aspect_val_as_json(()) is None
    assert (
        get_aspect_val_as_json(("com.linkedin.pegasus2avro.mxe.GenericAspect",)) is None
    )
    assert (
        get_aspect_val_as_json(("com.linkedin.pegasus2avro.mxe.GenericAspect", None))
        is None
    )
    assert (
        get_aspect_val_as_json(("com.linkedin.pegasus2avro.mxe.GenericAspect", {}))
        is None
    )
    assert (
        get_aspect_val_as_json(
            ("com.linkedin.pegasus2avro.mxe.GenericAspect", {"value": None})
        )
        is None
    )


def test_get_aspect_val_as_json_with_valid_val_for_tag():
    assert get_aspect_val_as_json(
        (
            "com.linkedin.pegasus2avro.mxe.GenericAspect",
            {
                "value": b'{"tags":[{"tag":"urn:li:tag:Legacy"},{"tag":"urn:li:tag:test"}]}',
                "contentType": "application/json",
            },
        )
    ) == {"tags": [{"tag": "urn:li:tag:Legacy"}, {"tag": "urn:li:tag:test"}]}


def test_get_value_for_term():
    assert get_value(
        (
            "com.linkedin.pegasus2avro.mxe.GenericAspect",
            {
                "value": b'{"auditStamp":{"actor":"urn:li:corpuser:admin","time":1644249286298},"terms":[{"urn":"urn:li:glossaryTerm:SavingAccount"}]}',
                "contentType": "application/json",
            },
        ),
        "terms",
    ) == [{"urn": "urn:li:glossaryTerm:SavingAccount"}]


def test_get_value_for_term_from_schema():
    assert get_value(
        (
            "com.linkedin.pegasus2avro.mxe.GenericAspect",
            {
                "value": b'{"editableSchemaFieldInfo":[{"fieldPath":"user_id","glossaryTerms":{"auditStamp":{"actor":"urn:li:corpuser:admin","time":1644942935508},"terms":[{"urn":"urn:li:glossaryTerm:Classification.Confidential"}]}}]}',
                "contentType": "application/json",
            },
        ),
        "editableSchemaFieldInfo/*/glossaryTerms/terms",
    ) == [{"urn": "urn:li:glossaryTerm:Classification.Confidential"}]


def test_get_value_for_tag_from_schema():
    assert get_value(
        (
            "com.linkedin.pegasus2avro.mxe.GenericAspect",
            {
                "value": b'{"editableSchemaFieldInfo":[{"globalTags":{"tags":[{"tag":"urn:li:tag:test"}]},"fieldPath":"user_id","glossaryTerms":{"auditStamp":{"actor":"urn:li:corpuser:admin","time":1645006089215},"terms":[]}}]}',
                "contentType": "application/json",
            },
        ),
        "editableSchemaFieldInfo/*/globalTags/tags",
    ) == [{"tag": "urn:li:tag:test"}]


def test_get_delta_from_mcl_global_tags_aspect():
    assert get_delta_from_mcl_global_tags_aspect(
        (
            "com.linkedin.pegasus2avro.mxe.GenericAspect",
            {
                "value": b'{"tags":[{"tag":"urn:li:tag:Legacy"},{"tag":"urn:li:tag:test"}]}',
                "contentType": "application/json",
            },
        ),
        None,
    ) == ([{"tag": "urn:li:tag:Legacy"}, {"tag": "urn:li:tag:test"}], [], [])


def test_get_delta_from_mcl_glossary_terms_aspect_empty_change():
    assert get_delta_from_mcl_glossary_terms_aspect(
        (
            "com.linkedin.pegasus2avro.mxe.GenericAspect",
            {
                "value": b'{"auditStamp":{"actor":"urn:li:corpuser:admin","time":1644249047091},"terms":[]}',
                "contentType": "application/json",
            },
        ),
        None,
    ) == ([], [], [])


def test_get_delta_from_mcl_glossary_terms_aspect_add_term():
    assert get_delta_from_mcl_glossary_terms_aspect(
        (
            "com.linkedin.pegasus2avro.mxe.GenericAspect",
            {
                "value": b'{"auditStamp":{"actor":"urn:li:corpuser:admin","time":1644249286298},"terms":[{"urn":"urn:li:glossaryTerm:SavingAccount"}]}',
                "contentType": "application/json",
            },
        ),
        None,
    ) == ([{"urn": "urn:li:glossaryTerm:SavingAccount"}], [], [])


def test_get_added_removed_objs_from_aspect_for_owner_aspect():
    assert get_delta_from_mcl_ownership_aspect(
        (
            "com.linkedin.pegasus2avro.mxe.GenericAspect",
            {
                "value": b'{"owners":[{"owner":"urn:li:corpuser:aseem.bansal","type":"DATAOWNER","source":{"type":"MANUAL"}}],"lastModified":{"actor":"urn:li:corpuser:datahub","time":1643907350936}}',
                "contentType": "application/json",
            },
        ),
        None,
    ) == (
        [
            {
                "owner": "urn:li:corpuser:aseem.bansal",
                "type": "DATAOWNER",
                "source": {"type": "MANUAL"},
            }
        ],
        [],
        [],
    )


def test_get_delta_from_mcl_glossary_terms_aspect_add_term_from_schema():
    assert get_delta_from_mcl_editable_schemametadata_aspect(
        (
            "com.linkedin.pegasus2avro.mxe.GenericAspect",
            {
                "value": b'{"editableSchemaFieldInfo":[{"fieldPath":"user_id","glossaryTerms":{"auditStamp":{"actor":"urn:li:corpuser:admin","time":1644942935508},"terms":[{"urn":"urn:li:glossaryTerm:Classification.Confidential"}]}}]}',
                "contentType": "application/json",
            },
        ),
        None,
    ) == ([{"urn": "urn:li:glossaryTerm:Classification.Confidential"}], [], [])


def test_get_delta_from_mcl_glossary_terms_aspect_add_tag_from_schema():
    assert get_delta_from_mcl_editable_schemametadata_aspect(
        (
            "com.linkedin.pegasus2avro.mxe.GenericAspect",
            {
                "value": b'{"editableSchemaFieldInfo":[{"globalTags":{"tags":[{"tag":"urn:li:tag:test"}]},"fieldPath":"user_id","glossaryTerms":{"auditStamp":{"actor":"urn:li:corpuser:admin","time":1645006089215},"terms":[]}}]}',
                "contentType": "application/json",
            },
        ),
        None,
    ) == ([{"tag": "urn:li:tag:test"}], [], [])
