from typing import Dict, List
from unittest.mock import MagicMock

import pytest
import requests_mock

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.gitlab_documents.gitlab_documents_config import (
    GitLabDocumentsSourceConfig,
)
from datahub.ingestion.source.gitlab_documents.gitlab_documents_source import (
    GitLabDocumentsSource,
)
from datahub.metadata.schema_classes import DocumentInfoClass

BASE = "https://gitlab.example.com"
GROUP_API = f"{BASE}/api/v4/groups/acme%2Fdata/projects"


def _project_api(path: str) -> str:
    return f"{BASE}/api/v4/projects/{path.replace('/', '%2F')}"


def _source(**config: object) -> GitLabDocumentsSource:
    return GitLabDocumentsSource(
        GitLabDocumentsSourceConfig.model_validate(
            {"gitlab_token": "glpat-test", "base_url": BASE, **config}
        ),
        PipelineContext(run_id="test-run"),
    )


def _mock_project(
    m: requests_mock.Mocker, path: str, branch: str, files: Dict[str, str]
) -> None:
    api = _project_api(path)
    m.get(f"{api}/repository/branches/{branch}", json={"commit": {"id": f"sha-{path}"}})
    m.get(
        f"{api}/repository/tree",
        json=[{"id": f"blob-{name}", "type": "blob", "path": name} for name in files],
    )
    for name, text in files.items():
        m.get(f"{api}/repository/files/{name}/raw", text=text)


def _mock_group(m: requests_mock.Mocker, projects: List[Dict[str, object]]) -> None:
    m.get(GROUP_API, json=projects)


def _infos(source: GitLabDocumentsSource) -> List[DocumentInfoClass]:
    return [
        info
        for wu in source.get_workunits_internal()
        if isinstance(wu, MetadataWorkUnit)
        and (info := wu.get_aspect_of_type(DocumentInfoClass)) is not None
    ]


def test_config_needs_a_repository_or_a_group() -> None:
    with pytest.raises(ValueError, match="repository, repositories or group"):
        _source()


def test_config_parses_repositories_on_one_instance_and_drops_duplicates() -> None:
    config = GitLabDocumentsSourceConfig.model_validate(
        {
            "gitlab_token": "glpat-test",
            "repository": f"{BASE}/acme/one/-/tree/main",
            "repositories": ["acme/two", f"{BASE}/acme/one", "acme/two"],
        }
    )
    assert config.base_url == BASE
    assert config.explicit_repositories == ["acme/one", "acme/two"]


def test_group_run_imports_each_project_with_its_own_default_branch() -> None:
    source = _source(group="acme/data", create_repo_root_document=True)
    with requests_mock.Mocker() as m:
        _mock_group(
            m,
            [
                {"path_with_namespace": "acme/data/a", "default_branch": "main"},
                {"path_with_namespace": "acme/data/sub/b", "default_branch": "trunk"},
            ],
        )
        _mock_project(m, "acme/data/a", "main", {"a.md": "# A"})
        _mock_project(m, "acme/data/sub/b", "trunk", {"b.md": "# B"})
        infos = _infos(source)

    repos = {info.customProperties["gitlab_repo"] for info in infos}
    assert repos == {"acme/data/a", "acme/data/sub/b"}
    root_titles = {
        info.title
        for info in infos
        if info.customProperties.get("is_repo_root_document") == "true"
    }
    assert root_titles == {"a", "b"}
    assert source.report.repositories_imported == 2
    assert not source.report.failures


def test_group_projects_nest_under_the_parent_document() -> None:
    parent = "urn:li:document:handbook"
    source = _source(group="acme/data", parent_document_urn=parent)
    with requests_mock.Mocker() as m:
        _mock_group(
            m, [{"path_with_namespace": "acme/data/a", "default_branch": "main"}]
        )
        _mock_project(m, "acme/data/a", "main", {"a.md": "# A"})
        infos = _infos(source)

    [root] = [
        info
        for info in infos
        if info.customProperties.get("is_repo_root_document") == "true"
    ]
    assert root.parentDocument is not None
    assert root.parentDocument.document == parent


def test_project_pattern_narrows_group_projects_but_not_named_ones() -> None:
    source = _source(
        group="acme/data",
        repositories=["acme/other/named"],
        project_pattern={"deny": ["acme/data/skip.*"]},
    )
    with requests_mock.Mocker() as m:
        _mock_group(
            m,
            [
                {"path_with_namespace": "acme/data/keep", "default_branch": "main"},
                {"path_with_namespace": "acme/data/skip-me", "default_branch": "main"},
            ],
        )
        m.get(_project_api("acme/other/named"), json={"default_branch": "main"})
        _mock_project(m, "acme/data/keep", "main", {"k.md": "# K"})
        _mock_project(m, "acme/other/named", "main", {"n.md": "# N"})
        infos = _infos(source)

    assert {info.customProperties["gitlab_repo"] for info in infos} == {
        "acme/data/keep",
        "acme/other/named",
    }


def test_configured_branch_overrides_each_projects_default_branch() -> None:
    source = _source(group="acme/data", branch="docs", create_repo_root_document=False)
    with requests_mock.Mocker() as m:
        _mock_group(
            m, [{"path_with_namespace": "acme/data/a", "default_branch": "main"}]
        )
        _mock_project(m, "acme/data/a", "docs", {"a.md": "# A"})
        infos = _infos(source)

    assert [info.customProperties["gitlab_branch"] for info in infos] == ["docs"]


def _keep_previous_urns(source: GitLabDocumentsSource, urn: str) -> MagicMock:
    handler = source.stale_entity_removal_handler
    handler.checkpointing_enabled = True
    handler.add_entity_to_state = MagicMock()  # type: ignore[method-assign]
    source.state_provider.get_last_checkpoint = MagicMock(  # type: ignore[method-assign]
        return_value=MagicMock(state=MagicMock(urns=[urn]))
    )
    return handler.add_entity_to_state  # type: ignore[return-value]


def test_a_failing_project_is_skipped_and_previous_documents_are_kept() -> None:
    source = _source(group="acme/data", create_repo_root_document=False)
    previous_urn = "urn:li:document:gitlab.acme.data.broken.old-1234"
    add_entity_to_state = _keep_previous_urns(source, previous_urn)
    with requests_mock.Mocker() as m:
        _mock_group(
            m,
            [
                {"path_with_namespace": "acme/data/broken", "default_branch": "main"},
                {"path_with_namespace": "acme/data/ok", "default_branch": "main"},
            ],
        )
        m.get(
            f"{_project_api('acme/data/broken')}/repository/branches/main",
            status_code=404,
        )
        _mock_project(m, "acme/data/ok", "main", {"ok.md": "# OK"})
        infos = _infos(source)

    assert {info.customProperties["gitlab_repo"] for info in infos} == {"acme/data/ok"}
    assert list(source.report.repositories_failed) == ["acme/data/broken"]
    assert "Repository skipped" in {w.title for w in source.report.warnings}
    assert not source.report.failures
    add_entity_to_state.assert_any_call("", previous_urn)


def test_run_fails_when_no_project_could_be_imported() -> None:
    source = _source(group="acme/data")
    with requests_mock.Mocker() as m:
        _mock_group(
            m, [{"path_with_namespace": "acme/data/a", "default_branch": "main"}]
        )
        m.get(
            f"{_project_api('acme/data/a')}/repository/branches/main", status_code=403
        )
        with pytest.raises(RuntimeError, match="any of the 1 repositories"):
            list(source.get_workunits_internal())


def test_projects_without_a_default_branch_are_skipped_and_named() -> None:
    # A token below Reporter sees the project but not its default branch.
    source = _source(group="acme/data")
    with requests_mock.Mocker() as m:
        _mock_group(m, [{"path_with_namespace": "acme/data/a", "default_branch": None}])
        with pytest.raises(RuntimeError, match="skipped"):
            list(source.get_workunits_internal())

    [warning] = list(source.report.warnings)
    assert warning.title == "Repository skipped"
    assert "Reporter" in "".join(warning.context)


def test_group_larger_than_max_projects_keeps_previous_documents() -> None:
    source = _source(group="acme/data", max_projects=1, create_repo_root_document=False)
    previous_urn = "urn:li:document:gitlab.acme.data.b.old-1234"
    add_entity_to_state = _keep_previous_urns(source, previous_urn)
    with requests_mock.Mocker() as m:
        _mock_group(
            m,
            [
                {"path_with_namespace": "acme/data/a", "default_branch": "main"},
                {"path_with_namespace": "acme/data/b", "default_branch": "main"},
            ],
        )
        _mock_project(m, "acme/data/a", "main", {"a.md": "# A"})
        _infos(source)

    assert "Group projects truncated by max_projects" in {
        w.title for w in source.report.warnings
    }
    add_entity_to_state.assert_any_call("", previous_urn)


def test_empty_group_imports_nothing_and_keeps_previous_documents() -> None:
    source = _source(group="acme/data")
    previous_urn = "urn:li:document:gitlab.acme.data.a.old-1234"
    add_entity_to_state = _keep_previous_urns(source, previous_urn)
    with requests_mock.Mocker() as m:
        _mock_group(m, [])
        assert _infos(source) == []

    assert "No repositories to import" in {w.title for w in source.report.warnings}
    add_entity_to_state.assert_called_once_with("", previous_urn)


def test_test_connection_checks_the_first_group_project() -> None:
    config = {"gitlab_token": "glpat-test", "base_url": BASE, "group": "acme/data"}
    with requests_mock.Mocker() as m:
        _mock_group(
            m, [{"path_with_namespace": "acme/data/a", "default_branch": "main"}]
        )
        m.get(
            f"{_project_api('acme/data/a')}/repository/branches/main",
            json={"commit": {"id": "abc123"}},
        )
        m.get(f"{_project_api('acme/data/a')}/repository/tree", json=[])
        report = GitLabDocumentsSource.test_connection(config)
    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is True

    with requests_mock.Mocker() as m:
        _mock_group(m, [])
        report = GitLabDocumentsSource.test_connection(config)
    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is False
