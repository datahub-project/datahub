import os

import pytest
from pydantic import SecretStr

from datahub.configuration.github import GitHubInfo
from datahub.ingestion.source.git.git_import import GitClone

LOOKML_TEST_SSH_KEY = os.environ.get("DATAHUB_LOOKML_GIT_TEST_SSH_KEY")


@pytest.mark.skipif(
    LOOKML_TEST_SSH_KEY is None,
    reason="DATAHUB_LOOKML_GIT_TEST_SSH_KEY env variable is not configured",
)
def test_git_clone(tmp_path):
    git_clone = GitClone(str(tmp_path))
    secret_key = SecretStr(LOOKML_TEST_SSH_KEY) if LOOKML_TEST_SSH_KEY else None

    checkout_dir = git_clone.clone(
        ssh_key=secret_key,
        repo_url="git@github.com:acryldata/long-tail-companions-looker",
        branch="d380a2b777ec6f4653626f39c68dba85893faa74",
    )
    assert os.path.exists(checkout_dir)
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


def test_github_branch():
    config = GitHubInfo(
        repo="owner/repo",
    )
    assert config.branch_for_clone is None

    config = GitHubInfo(
        repo="owner/repo",
        branch="main",
    )
    assert config.branch_for_clone == "main"
