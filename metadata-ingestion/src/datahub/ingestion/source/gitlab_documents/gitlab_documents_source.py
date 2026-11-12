"""Ingest documents from a GitLab project."""

from urllib.parse import quote

from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import SourceCapability
from datahub.ingestion.source.github_documents.github_api import (
    RepositoryDocumentsClient,
)
from datahub.ingestion.source.github_documents.github_documents_config import (
    RepositoryDocumentsSourceConfigBase,
)
from datahub.ingestion.source.github_documents.github_documents_source import (
    RepositoryDocumentsSourceBase,
)
from datahub.ingestion.source.gitlab_documents.gitlab_api import GitLabApiClient
from datahub.ingestion.source.gitlab_documents.gitlab_documents_config import (
    GitLabDocumentsSourceConfig,
)


@platform_name("GitLab")
@config_class(GitLabDocumentsSourceConfig)
@support_status(SupportStatus.ALPHA)
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
class GitLabDocumentsSource(RepositoryDocumentsSourceBase):
    """
    Ingest markdown and text files from a GitLab project as DataHub Document entities.

    Works with GitLab.com and self-managed GitLab through the REST API (v4).
    Preserves the project's folder structure as parent-child document
    relationships, and behaves like the ``github-documents`` source otherwise.
    """

    platform = "gitlab"
    config: GitLabDocumentsSourceConfig

    @classmethod
    def _parse_config(cls, config_dict: dict) -> GitLabDocumentsSourceConfig:
        return GitLabDocumentsSourceConfig.parse_obj(config_dict)

    @classmethod
    def _make_client(
        cls, config: RepositoryDocumentsSourceConfigBase
    ) -> RepositoryDocumentsClient:
        assert isinstance(config, GitLabDocumentsSourceConfig)
        return GitLabApiClient(
            config.gitlab_token.get_secret_value(), base_url=config.base_url
        )

    def _repository_url(self, owner_repo: str) -> str:
        return f"{self.config.base_url}/{owner_repo}"

    def _build_external_url(
        self, owner_repo: str, branch: str, path: str, *, is_directory: bool
    ) -> str:
        project_url = self._repository_url(owner_repo)
        if not path:
            return project_url
        kind = "tree" if is_directory else "blob"
        return f"{project_url}/-/{kind}/{quote(branch)}/{quote(path.lstrip('/'))}"
