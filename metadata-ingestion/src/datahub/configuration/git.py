import os
from typing import Any, Dict, Optional

from pydantic import Field, FilePath, SecretStr, validator

from datahub.configuration.common import ConfigModel
from datahub.configuration.validate_field_rename import pydantic_renamed_field

_GITHUB_PREFIX = "https://github.com/"
_GITLAB_PREFIX = "https://gitlab.com/"

_GITHUB_URL_TEMPLATE = "{repo_url}/blob/{branch}/{file_path}"
_GITLAB_URL_TEMPLATE = "{repo_url}/-/blob/{branch}/{file_path}"


class GitReference(ConfigModel):
    """Reference to a hosted Git repository. Used to generate "view source" links."""

    repo: str = Field(
        description="Name of your Git repo e.g. https://github.com/datahub-project/datahub or https://gitlab.com/gitlab-org/gitlab. If organization/repo is provided, we assume it is a GitHub repo."
    )
    branch: str = Field(
        "main",
        description="Branch on which your files live by default. Typically main or master. This can also be a commit hash.",
    )

    url_template: Optional[str] = Field(
        None,
        description=f"Template for generating a URL to a file in the repo e.g. '{_GITHUB_URL_TEMPLATE}'. We can infer this for GitHub and GitLab repos, and it is otherwise required."
        "It supports the following variables: {repo_url}, {branch}, {file_path}",
    )

    _deprecated_base_url = pydantic_renamed_field(
        "base_url",
        "url_template",
        transform=lambda url: _GITHUB_URL_TEMPLATE,
    )

    @validator("repo", pre=True)
    def simplify_repo_url(cls, repo: str) -> str:
        if repo.startswith("github.com/"):
            repo = f"https://{repo}"
        elif repo.startswith("gitlab.com"):
            repo = f"https://{repo}"
        elif repo.count("/") == 1:
            repo = f"https://github.com/{repo}"

        if repo.endswith("/"):
            repo = repo[:-1]

        return repo

    @validator("url_template", always=True)
    def infer_url_template(cls, url_template: Optional[str], values: dict) -> str:
        if url_template is not None:
            return url_template

        repo: str = values["repo"]
        if repo.startswith(_GITHUB_PREFIX):
            return _GITHUB_URL_TEMPLATE
        elif repo.startswith(_GITLAB_PREFIX):
            return _GITLAB_URL_TEMPLATE
        else:
            raise ValueError(
                "Unable to infer URL template from repo. Please set url_template manually."
            )

    def get_url_for_file_path(self, file_path: str) -> str:
        assert self.url_template
        return self.url_template.format(
            repo_url=self.repo, branch=self.branch, file_path=file_path
        )


class GitInfo(GitReference):
    """A reference to a Git repository, including a deploy key that can be used to clone it."""

    deploy_key_file: Optional[FilePath] = Field(
        None,
        description="A private key file that contains an ssh key that has been configured as a deploy key for this repository. Use a file where possible, else see deploy_key for a config field that accepts a raw string.",
    )
    deploy_key: Optional[SecretStr] = Field(
        None,
        description="A private key that contains an ssh key that has been configured as a deploy key for this repository. See deploy_key_file if you want to use a file that contains this key.",
    )

    repo_ssh_locator: Optional[str] = Field(
        None,
        description="The url to call `git clone` on. We infer this for github and gitlab repos, but it is required for other hosts.",
    )

    @validator("deploy_key_file")
    def deploy_key_file_should_be_readable(
        cls, v: Optional[FilePath]
    ) -> Optional[FilePath]:
        if v is not None:
            # pydantic does existence checks, we just need to check if we can read it
            if not os.access(v, os.R_OK):
                raise ValueError(f"Unable to read deploy key file {v}")
        return v

    @validator("deploy_key", pre=True, always=True)
    def deploy_key_filled_from_deploy_key_file(
        cls, v: Optional[SecretStr], values: Dict[str, Any]
    ) -> Optional[SecretStr]:
        if v is None:
            deploy_key_file = values.get("deploy_key_file")
            if deploy_key_file is not None:
                with open(deploy_key_file, "r") as fp:
                    deploy_key = SecretStr(fp.read())
                    return deploy_key
        return v

    @validator("repo_ssh_locator", always=True)
    def infer_repo_ssh_locator(
        cls, repo_ssh_locator: Optional[str], values: dict
    ) -> str:
        if repo_ssh_locator is not None:
            return repo_ssh_locator

        repo: str = values["repo"]
        if repo.startswith(_GITHUB_PREFIX):
            return f"git@github.com:{repo[len(_GITHUB_PREFIX):]}.git"
        elif repo.startswith(_GITLAB_PREFIX):
            return f"git@gitlab.com:{repo[len(_GITLAB_PREFIX):]}.git"
        else:
            raise ValueError(
                "Unable to infer repo_ssh_locator from repo. Please set repo_ssh_locator manually."
            )

    @property
    def branch_for_clone(self) -> Optional[str]:
        # If branch was manually set, we should use it. Otherwise return None.
        # We do this because we want to use the default branch unless they override it.
        # While our default for branch is "main", they could be using "master" or something else.
        # It's ok if the URLs we generate are slightly incorrect, but changing branch to be
        # required would be a breaking change.

        if "branch" in self.__fields_set__:
            return self.branch
        return None
