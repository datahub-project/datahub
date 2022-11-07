import os
from typing import Any, Dict, Optional

from pydantic import Field, FilePath, SecretStr, validator

from datahub.configuration.common import ConfigModel, ConfigurationError


class GitHubReference(ConfigModel):
    repo: str = Field(
        description="Name of your github repository in org/repo format. e.g. repo for https://github.com/datahub-project/datahub is `datahub-project/datahub`."
    )
    branch: str = Field(
        "main",
        description="Branch on which your files live by default. Typically main or master.",
    )
    base_url: str = Field(
        "https://github.com",
        description="Base url for Github. Used to construct clickable links on the UI.",
    )

    @validator("repo")
    def repo_should_be_org_slash_repo(cls, repo: str) -> str:
        if "/" not in repo or len(repo.split("/")) != 2:
            raise ConfigurationError(
                "github repo should be in organization/repo form e.g. datahub-project/datahub"
            )
        return repo

    def get_url_for_file_path(self, file_path: str) -> str:
        return f"{self.base_url}/{self.repo}/blob/{self.branch}/{file_path}"


class GitHubInfo(GitHubReference):
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
        description="Auto-inferred from repo as git@github.com:{repo}, but you can override this if needed.",
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
    def auto_infer_from_repo(cls, v: Optional[str], values: Dict[str, Any]) -> str:
        if v is None:
            return f"git@github.com:{values.get('repo')}"
        return v
