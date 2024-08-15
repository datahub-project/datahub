import pathlib
import unittest.mock
from typing import Iterator

import datahub.metadata.schema_classes as models
import git
import pytest

from datahub_integrations.actions.replay_source import read_events_file
from datahub_integrations.dbt.dbt_sync_back import (
    DEFAULT_YML_FILE_CREATION_MODE,
    AddTagOperation,
    AddTermOperation,
    AlreadySyncedError,
    ApplyContext,
    DbtOperationExtractConfig,
    OperationTarget,
    RemoveTagOperation,
    extract_dbt_operation,
    truncate_docs,
)
from datahub_integrations.dbt.dbt_utils import AdvancedDbtProject, DbtProject
from datahub_integrations.dbt.git_utils import GitHubRepoConfig, GitHubRepoWrapper

events_dir = pathlib.Path(__file__).parent


@pytest.fixture(scope="module")
def sample_dbt_repo_raw(
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[GitHubRepoWrapper]:
    base_temp_dir = tmp_path_factory.mktemp("dbt_repo")
    config = GitHubRepoConfig(
        github_repo_org="hsheth2",
        github_repo_name="sample-dbt",
        auth="fake",
    )

    repo = GitHubRepoWrapper(config, base_temp_dir=base_temp_dir)
    yield repo


def test_git_clone(sample_dbt_repo_raw: GitHubRepoWrapper) -> None:
    sample_dbt_repo_raw.force_checkout("5e0e4b878027c5e7343264235d46e9a999a5a3e9")
    assert sample_dbt_repo_raw.repo_dir.exists()
    assert (sample_dbt_repo_raw.repo_dir / "README.md").exists()


@pytest.fixture(scope="module")
def sample_dbt_proj_module(
    sample_dbt_repo_raw: GitHubRepoWrapper,
) -> Iterator[DbtProject]:
    sample_dbt_repo_raw.force_checkout("5e0e4b878027c5e7343264235d46e9a999a5a3e9")

    proj = DbtProject(sample_dbt_repo_raw.repo_dir)
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


@pytest.mark.skip(reason="This code path is not being used, and is pretty slow to run.")
def test_advanced_locator(
    sample_dbt_proj: DbtProject,
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    advanced_dbt_project = AdvancedDbtProject(
        sample_dbt_proj.dbt_dir, tmp_path_factory.mktemp("dbt_proj")
    )

    assert (
        advanced_dbt_project.get_original_file_path(
            "model.sample_dbt.monthly_billing_with_cust"
        )
        == "models/billing/monthly_billing_with_cust.sql"
    )


def test_term_event_extractor() -> None:
    graph = unittest.mock.MagicMock()
    config = DbtOperationExtractConfig()

    op = extract_dbt_operation(
        list(read_events_file(events_dir / "events_add_term.json"))[1],
        config=config,
        graph=graph,
    )
    assert op == (
        OperationTarget(
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:dbt,pagila.public.an-aliased-view-for-monthly-billing,PROD)"
        ),
        AddTermOperation(
            term="urn:li:glossaryTerm:a30484c3-3a68-468f-8902-08e019e3437d"
        ),
    )

    # TODO: We need tests for the other extractors as well.


def test_tag_op(sample_dbt_proj: DbtProject) -> None:
    graph = unittest.mock.MagicMock()
    context = ApplyContext(
        graph=graph,
        proj=sample_dbt_proj,
        yml_file_creation_mode=DEFAULT_YML_FILE_CREATION_MODE,
    )

    urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,proj.dataset.monthly_billing_with_cust,PROD)"
    dbt_unique_id = "model.sample_dbt.monthly_billing_with_cust"
    original_file_path = "models/billing/monthly_billing_with_cust.sql"

    graph.get_aspect.return_value = models.DatasetPropertiesClass(
        customProperties={
            "dbt_unique_id": dbt_unique_id,
            "dbt_file_path": original_file_path,
        },
    )

    target = OperationTarget(dataset_urn=urn)

    # Test adding two tags.
    add_op = AddTagOperation(tag="urn:li:tag:my_new_tag", tag_pretty_name="My New Tag")
    add_op.apply(target, context)

    add_op = AddTagOperation(
        tag="urn:li:tag:my_second_tag", tag_pretty_name="My Second Tag"
    )
    add_op.apply(target, context)

    assert_git_diff(
        sample_dbt_proj.dbt_dir,
        """diff --git a/models/base.yml b/models/base.yml
--- a/models/base.yml
+++ b/models/base.yml
@@ -93,3 +93,7 @@ models:
         tests:
           - not_null
           - is_email
+    config:
+      tags:
+        - my_new_tag  # My New Tag
+        - my_second_tag # My Second Tag""",
    )

    # Test removing a tag that doesn't exist.
    remove_op = RemoveTagOperation(tag="urn:li:tag:this_is_not_a_tag")
    with pytest.raises(AlreadySyncedError):
        remove_op.apply(target, context)

    # Test removing the tags.
    RemoveTagOperation(tag="urn:li:tag:my_new_tag").apply(target, context)
    RemoveTagOperation(tag="urn:li:tag:my_second_tag").apply(target, context)

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


def test_docs_truncation() -> None:
    assert truncate_docs("a" * 20, maxlen=30) == "a" * 20
    assert truncate_docs("a" * 40, maxlen=30) == "a" * 30 + "..."
    assert truncate_docs("a" * 20 + "\n" + "b" * 100, maxlen=30) == "a" * 20 + "..."
