from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import SourceCapability
from datahub.ingestion.source.github_documents.github_api import (
    GitHubApiClient,
    StaticTokenProvider,
)
from datahub.ingestion.source.github_documents.github_documents_config import (
    GitHubDocumentsSourceConfig,
)
from datahub.ingestion.source.repository_documents.client import (
    RepositoryDocumentsClient,
)
from datahub.ingestion.source.repository_documents.config import (
    RepositoryDocumentsSourceConfigBase,
)
from datahub.ingestion.source.repository_documents.source import (
    EXTRACTION_ALGO_VERSION as EXTRACTION_ALGO_VERSION,
    LAST_EXPORTED_CONTENT_HASH_KEY as LAST_EXPORTED_CONTENT_HASH_KEY,
    PROP_IMPORT_SOURCE_ID as PROP_IMPORT_SOURCE_ID,
    RepositoryDocumentsSourceBase,
    compute_file_content_hash as compute_file_content_hash,
    compute_source_hash as compute_source_hash,
)

PROP_GITHUB_FILE_PATH = "github_file_path"
PROP_GITHUB_DIRECTORY_PATH = "github_directory_path"


@platform_name("GitHub")
@config_class(GitHubDocumentsSourceConfig)
@support_status(SupportStatus.ALPHA)
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
class GitHubDocumentsSource(RepositoryDocumentsSourceBase):
    """
    Ingest markdown and text files from a GitHub repository as DataHub Document entities.

    Preserves repository folder structure as parent-child document relationships. By default
    documents are ingested as native (editable) documents; set ``document_import_mode`` to
    ``EXTERNAL`` for read-only references back to GitHub.

    Markdown concepts inside OKF bundles are mapped to OKF and native aspects
    when enable_okf is set. Other files retain their original text.
    """

    platform = "github"
    platform_display_name = "GitHub"
    config: GitHubDocumentsSourceConfig
    client: GitHubApiClient

    @classmethod
    def _parse_config(cls, config_dict: dict) -> GitHubDocumentsSourceConfig:
        return GitHubDocumentsSourceConfig.parse_obj(config_dict)

    @classmethod
    def _make_client(
        cls, config: RepositoryDocumentsSourceConfigBase
    ) -> RepositoryDocumentsClient:
        assert isinstance(config, GitHubDocumentsSourceConfig)
        return GitHubApiClient(
            StaticTokenProvider(config.github_token.get_secret_value())
        )

    def _repository_url(self, owner_repo: str) -> str:
        return f"https://github.com/{owner_repo}"

    def _build_external_url(
        self, owner_repo: str, branch: str, path: str, *, is_directory: bool
    ) -> str:
        return f"{self._repository_url(owner_repo)}/blob/{branch}/{path.lstrip('/')}"
