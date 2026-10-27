from typing import Dict, List, Optional, Tuple

import pytest
from pydantic import ValidationError
from requests_mock import Mocker

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sac.sac import SACSource, SACSourceConfig
from datahub.ingestion.source.sac.sac_common import Resource

_TENANT_URL = "http://tenant"
_TOKEN_URL = "http://tenant.authentication/oauth/token"

# Same physical public root, returned localized: story as "Public", application as "Öffentlich".
_STORY_RESOURCE_ID = "LXTH4JCE36EOYLU41PIINLYPU9XRYM26"
_APPLICATION_RESOURCE_ID = "EOYLU41PIILXTH4JCE36NLYPU9XRYM26"
# A shared team/workspace root ("Default") that also comes back under isPublic=1.
_WORKSPACE_RESOURCE_ID = "NLYPU9XRYM26LXTH4JCE36EOYLU41PII"


def _resource_entity(
    resource_id: str, ancestor_path_raw: Optional[str]
) -> Dict[str, object]:
    entity: Dict[str, object] = {
        "resourceId": resource_id,
        "name": f"Name {resource_id}",
        "resourceType": "STORY",
        "resourceSubtype": "",
        "storyId": f"STORY:t.4:{resource_id}",
        "createdTime": "/Date(1667544309783)/",
        "createdBy": "JOHN_DOE",
        "modifiedBy": "JOHN_DOE",
        "modifiedTime": "/Date(1673067981272)/",
        "isMobile": 0,
        "openURL": f"/sap/fpa/ui/tenants/3c44c/bo/story/{resource_id}",
    }
    if ancestor_path_raw is not None:
        entity["ancestorPath"] = ancestor_path_raw
    return entity


def _get_resources(
    requests_mock: Mocker,
    resources: List[Tuple[str, Optional[str]]],
    *,
    resources_payload: Optional[Dict[str, object]] = None,
    model_expansion_status: Optional[int] = None,
    **config_overrides: object,
) -> Tuple[SACSource, List[Resource]]:
    requests_mock.post(_TOKEN_URL, json={"access_token": "token", "expires_in": 3599})
    requests_mock.get(
        f"{_TENANT_URL}/api/v1/Resources",
        json=resources_payload
        if resources_payload is not None
        else {"d": {"results": [_resource_entity(rid, ap) for rid, ap in resources]}},
    )
    for resource_id, _ in resources:
        escaped = resource_id.replace("'", "''")
        if model_expansion_status is not None:
            requests_mock.get(
                f"{_TENANT_URL}/api/v1/Resources('{escaped}')/resourceModels",
                status_code=model_expansion_status,
            )
        else:
            requests_mock.get(
                f"{_TENANT_URL}/api/v1/Resources('{escaped}')/resourceModels",
                json={"d": {"results": []}},
            )
    requests_mock.get(f"{_TENANT_URL}/api/v1/dataimport/models", json={"models": []})

    config = SACSourceConfig(
        tenant_url=_TENANT_URL,
        token_url=_TOKEN_URL,
        client_id="foo",
        client_secret="bar",
        **config_overrides,
    )
    source = SACSource(config, PipelineContext(run_id="sac-unit"))
    return source, list(source.get_resources())


_BOTH_ROOTS: List[Tuple[str, Optional[str]]] = [
    (_STORY_RESOURCE_ID, '["Public","Folder 1","Folder 2"]'),
    (_APPLICATION_RESOURCE_ID, '["Öffentlich","Folder 1","Folder 2"]'),
]


def test_folder_pattern_matches_canonicalized_root(requests_mock):
    # folder_pattern runs after canonicalization, so a Public rule matches both the
    # English-rooted story and the German-rooted application.
    _, resources = _get_resources(
        requests_mock,
        _BOTH_ROOTS,
        folder_pattern={"allow": ["Public/Folder 1/.*"]},
    )

    assert {resource.resource_id for resource in resources} == {
        _STORY_RESOURCE_ID,
        _APPLICATION_RESOURCE_ID,
    }
    assert all(
        resource.ancestor_path == "Public/Folder 1/Folder 2" for resource in resources
    )


def test_folder_pattern_against_localized_root_matches_nothing(requests_mock):
    # Both roots canonicalize to "Public", so a rule targeting "Öffentlich" matches nothing.
    _, resources = _get_resources(
        requests_mock,
        _BOTH_ROOTS,
        folder_pattern={"allow": ["Öffentlich/.*"]},
    )

    assert resources == []


def test_disabling_canonicalization_preserves_localized_root(requests_mock):
    # With public_root_folder_name=null the German resource keeps its "Öffentlich" root.
    _, resources = _get_resources(
        requests_mock,
        _BOTH_ROOTS,
        public_root_folder_name=None,
        folder_pattern={"allow": ["Öffentlich/.*"]},
    )

    assert {resource.resource_id for resource in resources} == {
        _APPLICATION_RESOURCE_ID
    }
    assert resources[0].ancestor_path == "Öffentlich/Folder 1/Folder 2"


def test_unrecognized_root_is_not_merged_into_public(requests_mock):
    # A team/workspace root ("Default") returned under isPublic=1 keeps its own root
    # instead of being rewritten to the public root and merged into the public tree.
    _, resources = _get_resources(
        requests_mock,
        [
            (_STORY_RESOURCE_ID, '["Public","Folder 1"]'),
            (_WORKSPACE_RESOURCE_ID, '["Default","Folder 1"]'),
        ],
    )

    paths = {resource.resource_id: resource.ancestor_path for resource in resources}
    assert paths[_STORY_RESOURCE_ID] == "Public/Folder 1"
    assert paths[_WORKSPACE_RESOURCE_ID] == "Default/Folder 1"


def test_unparseable_ancestor_path_warns_and_drops_browse_path(requests_mock):
    source, resources = _get_resources(
        requests_mock, [(_STORY_RESOURCE_ID, '["Public",')]
    )

    assert len(resources) == 1
    assert resources[0].ancestor_path is None
    assert [w.title for w in source.report.warnings] == [
        "Unparseable SAC ancestor path"
    ]


def test_non_list_ancestor_path_warns_and_drops_browse_path(requests_mock):
    source, resources = _get_resources(requests_mock, [(_STORY_RESOURCE_ID, '{"a":1}')])

    assert len(resources) == 1
    assert resources[0].ancestor_path is None
    assert [w.title for w in source.report.warnings] == [
        "Unexpected SAC ancestor path shape"
    ]


def test_model_expansion_failure_warns_and_emits_resource_without_models(requests_mock):
    # A per-resource resourceModels failure must not abort the run: the story is still
    # emitted (with no models) and the failure is surfaced as a warning.
    source, resources = _get_resources(
        requests_mock,
        [(_STORY_RESOURCE_ID, '["Public","Folder 1"]')],
        model_expansion_status=500,
    )

    assert len(resources) == 1
    assert resources[0].resource_id == _STORY_RESOURCE_ID
    assert resources[0].resource_models == frozenset()
    assert "Could not read models for a resource" in [
        w.title for w in source.report.warnings
    ]


def test_v2_list_body_without_results_wrapper_is_parsed(requests_mock):
    # Some SAC v2 responses return the entity list directly under "d" rather than nested
    # under "d"/"results"; both envelope shapes must be parsed.
    _, resources = _get_resources(
        requests_mock,
        [(_STORY_RESOURCE_ID, '["Public","Folder 1"]')],
        resources_payload={"d": [_resource_entity(_STORY_RESOURCE_ID, '["Public"]')]},
    )

    assert [resource.resource_id for resource in resources] == [_STORY_RESOURCE_ID]
    assert resources[0].ancestor_path == "Public"


@pytest.mark.parametrize("value", ["", "   "])
def test_blank_public_root_folder_name_rejected(value):
    with pytest.raises(ValidationError):
        SACSourceConfig(
            tenant_url=_TENANT_URL,
            token_url=_TOKEN_URL,
            client_id="foo",
            client_secret="bar",
            public_root_folder_name=value,
        )
