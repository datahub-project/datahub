from pydantic import Field, validator

from datahub.configuration.common import ConfigModel, ConfigurationError


class GitHubInfo(ConfigModel):
    repo: str = Field(
        description="Name of your github repo. e.g. repo for https://github.com/datahub-project/datahub is `datahub-project/datahub`."
    )
    branch: str = Field(
        "main",
        description="Branch on which your files live by default. Typically main or master.",
    )
    base_url: str = Field("https://github.com", description="Base url for Github")

    @validator("repo")
    def repo_should_be_org_slash_repo(cls, repo: str) -> str:
        if "/" not in repo or len(repo.split("/")) != 2:
            raise ConfigurationError(
                "github repo should be in organization/repo form e.g. acryldata/datahub-helm"
            )
        return repo

    def get_url_for_file_path(self, file_path: str) -> str:
        return f"{self.base_url}/{self.repo}/blob/{self.branch}/{file_path}"
