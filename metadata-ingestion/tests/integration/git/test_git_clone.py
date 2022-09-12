import os

from pydantic import SecretStr

from datahub.ingestion.source.git.git_import import GitClone


def test_git_clone(pytestconfig, tmp_path):

    git_clone = GitClone(tmp_path)
    with open(
        pytestconfig.rootpath / "tests/integration/git/datahub_git_ssh_ci", "r"
    ) as fp:
        secret_key = SecretStr(fp.read())

    checkout_dir = git_clone.clone(
        ssh_key=secret_key,
        repo_url="git@github.com:acryldata/long-tail-companions-looker",
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
        ]
    )
