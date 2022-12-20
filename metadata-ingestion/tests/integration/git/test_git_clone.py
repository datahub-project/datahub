import os

from pydantic import SecretStr

from datahub.ingestion.source.git.git_import import GitClone


def test_git_clone(pytestconfig, tmp_path):
    git_clone = GitClone(str(tmp_path))
    secret_env_variable = "DATAHUB_LOOKML_GIT_TEST_SSH_KEY"
    if os.environ.get(secret_env_variable) is not None:
        secret_key = SecretStr(os.environ.get(secret_env_variable))  # type: ignore
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
    else:
        print(
            "Skipping test as env variable DATAHUB_LOOKML_GIT_TEST_SSH_KEY is not configured"
        )
