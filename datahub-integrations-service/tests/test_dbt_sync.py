import pathlib
from typing import Iterator

import git
import pytest

from datahub_integrations.dbt.dbt_sync_back import AddTagOperation, RemoveTagOperation
from datahub_integrations.dbt.dbt_utils import DbtProject
from datahub_integrations.dbt.git_utils import GitHubRepoConfig, GitHubRepoWrapper


@pytest.fixture(scope="module")
def sample_dbt_repo_raw(
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[GitHubRepoWrapper]:
    base_temp_dir = tmp_path_factory.mktemp("dbt_repo")
    config = GitHubRepoConfig(
        github_repo_org="hsheth2",
        github_repo_name="sample-dbt",
        token="",
    )

    repo = GitHubRepoWrapper(config, base_temp_dir=base_temp_dir)
    yield repo


def test_git_clone(sample_dbt_repo_raw: GitHubRepoWrapper) -> None:
    sample_dbt_repo_raw.force_checkout("5e0e4b878027c5e7343264235d46e9a999a5a3e9")
    assert sample_dbt_repo_raw.repo_dir.exists()
    assert (sample_dbt_repo_raw.repo_dir / "README.md").exists()


@pytest.fixture(scope="module")
def sample_dbt_proj_module(
    sample_dbt_repo_raw: GitHubRepoWrapper, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[DbtProject]:
    sample_dbt_repo_raw.force_checkout("5e0e4b878027c5e7343264235d46e9a999a5a3e9")

    proj = DbtProject(sample_dbt_repo_raw.repo_dir, tmp_path_factory.mktemp("dbt_proj"))
    yield proj


@pytest.fixture
def sample_dbt_proj(
    sample_dbt_repo_raw: GitHubRepoWrapper, sample_dbt_proj_module: DbtProject
) -> Iterator[DbtProject]:
    sample_dbt_repo_raw.force_checkout("5e0e4b878027c5e7343264235d46e9a999a5a3e9")
    yield sample_dbt_proj_module


def _remove_index_directive(diff: str) -> str:
    return "\n".join(line for line in diff.split("\n") if not line.startswith("index"))


def assert_git_diff(dir: pathlib.Path, expected_diff: str) -> None:
    # TODO: Should use the repo obj from GitHubRepoWrapper.
    repo = git.Repo(str(dir))

    # Signal intent to stage all files in the repo.
    # See https://stackoverflow.com/a/857696/5004662
    repo.git.add("-N", ".")

    diff = repo.git.diff("--patch")
    diff = _remove_index_directive(diff)
    assert diff == expected_diff


def test_tag_op(sample_dbt_proj: DbtProject) -> None:
    dbt_unique_id = "model.sample_dbt.monthly_billing_with_cust"

    # Test adding a tag.
    add_op = AddTagOperation(
        dataset_urn="-dummy-",
        tag="urn:li:tag:my_new_tag",
    )
    add_op.apply_with_dbt_id(dbt_unique_id, sample_dbt_proj)

    assert_git_diff(
        sample_dbt_proj.dbt_dir,
        """diff --git a/models/base.yml b/models/base.yml
--- a/models/base.yml
+++ b/models/base.yml
@@ -93,3 +93,6 @@ models:
         tests:
           - not_null
           - is_email
+    config:
+      tags:
+        - my_new_tag""",
    )

    # Test removing a tag.
    remove_op = RemoveTagOperation(
        dataset_urn="-dummy-",
        tag="urn:li:tag:my_new_tag",
    )
    remove_op.apply_with_dbt_id(dbt_unique_id, sample_dbt_proj)

    assert_git_diff(
        sample_dbt_proj.dbt_dir,
        """diff --git a/models/base.yml b/models/base.yml
--- a/models/base.yml
+++ b/models/base.yml
@@ -93,3 +93,5 @@ models:
         tests:
           - not_null
           - is_email
+    config:
+      tags: []""",
    )
