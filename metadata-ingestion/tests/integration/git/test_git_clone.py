import doctest
import os

import pytest
from pydantic import SecretStr

from datahub.configuration.common import ConfigurationWarning
from datahub.configuration.git import GitInfo, GitReference
from datahub.ingestion.source.git.git_import import GitClone

LOOKML_TEST_SSH_KEY = os.environ.get("DATAHUB_LOOKML_GIT_TEST_SSH_KEY")


def test_base_url_guessing():
    # Basic GitHub repo.
    config = GitInfo(repo="https://github.com/datahub-project/datahub", branch="master")
    assert config.repo_ssh_locator == "git@github.com:datahub-project/datahub.git"

    # Defaults to GitHub.
    config = GitInfo(repo="datahub-project/datahub", branch="master")
    assert (
        config.get_url_for_file_path("docker/README.md")
        == "https://github.com/datahub-project/datahub/blob/master/docker/README.md"
    )
    assert config.repo_ssh_locator == "git@github.com:datahub-project/datahub.git"

    # GitLab repo (notice the trailing slash).
    config_ref = GitReference(
        repo="https://gitlab.com/gitlab-tests/sample-project/", branch="master"
    )
    assert (
        config_ref.get_url_for_file_path("hello_world.md")
        == "https://gitlab.com/gitlab-tests/sample-project/-/blob/master/hello_world.md"
    )

    # Three-tier GitLab repo.
    config = GitInfo(
        repo="https://gitlab.com/gitlab-com/gl-infra/reliability", branch="master"
    )
    assert (
        config.get_url_for_file_path("onboarding/gitlab.nix")
        == "https://gitlab.com/gitlab-com/gl-infra/reliability/-/blob/master/onboarding/gitlab.nix"
    )
    assert (
        config.repo_ssh_locator == "git@gitlab.com:gitlab-com/gl-infra/reliability.git"
    )

    # Overrides.
    config = GitInfo(
        repo="https://gitea.com/gitea/tea",
        branch="main",
        url_template="https://gitea.com/gitea/tea/src/branch/{branch}/{file_path}",
        repo_ssh_locator="https://gitea.com/gitea/tea.git",
    )
    assert (
        config.get_url_for_file_path("cmd/admin.go")
        == "https://gitea.com/gitea/tea/src/branch/main/cmd/admin.go"
    )
    assert config.repo_ssh_locator == "https://gitea.com/gitea/tea.git"

    # Deprecated: base_url.
    with pytest.warns(ConfigurationWarning, match="base_url is deprecated"):
        config = GitInfo.parse_obj(
            dict(
                repo="https://github.com/datahub-project/datahub",
                branch="master",
                base_url="http://mygithubmirror.local",
            )
        )


def test_github_branch():
    config = GitInfo(
        repo="owner/repo",
    )
    assert config.branch_for_clone is None

    config = GitInfo(
        repo="owner/repo",
        branch="main",
    )
    assert config.branch_for_clone == "main"


def test_sanitize_repo_url():
    import datahub.ingestion.source.git.git_import

    assert doctest.testmod(datahub.ingestion.source.git.git_import) == (
        0,
        3,
    )  # 0 failures, 3 tests


def test_git_clone_public(tmp_path):
    git_clone = GitClone(str(tmp_path))
    checkout_dir = git_clone.clone(
        ssh_key=None,
        repo_url="https://gitlab.com/gitlab-tests/sample-project",
        branch="90c439634077a85bcf42d38c2c79cd94664a94ad",
    )
    assert checkout_dir.exists()
    assert set(os.listdir(checkout_dir)) == {
        ".git",
        "README.md",
        "hello_world.md",
        "fork-sample-project.png",
    }


@pytest.mark.skipif(
    LOOKML_TEST_SSH_KEY is None,
    reason="DATAHUB_LOOKML_GIT_TEST_SSH_KEY env variable is not configured",
)
def test_git_clone_private(tmp_path):
    git_clone = GitClone(str(tmp_path))
    secret_key = SecretStr(LOOKML_TEST_SSH_KEY) if LOOKML_TEST_SSH_KEY else None

    checkout_dir = git_clone.clone(
        ssh_key=secret_key,
        repo_url="git@github.com:acryldata/long-tail-companions-looker",
        branch="d380a2b777ec6f4653626f39c68dba85893faa74",
    )
    assert checkout_dir.exists()
    assert set(os.listdir(checkout_dir)) == set(
        [
            ".datahub",
            "models",
            "README.md",
            ".github",
            ".git",
            "views",
            "manifest_lock.lkml",
            "manifest.lkml",
        ]
    )
