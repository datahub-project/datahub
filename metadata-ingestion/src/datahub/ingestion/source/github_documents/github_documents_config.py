from pydantic import Field, SecretStr, field_validator

from datahub.configuration.validate_field_removal import pydantic_removed_field
from datahub.ingestion.source.github_documents.github_api import parse_repo_identifier
from datahub.ingestion.source.repository_documents.config import (
    RepositoryDocumentsSourceConfigBase,
)


class GitHubDocumentsSourceConfig(RepositoryDocumentsSourceConfigBase):
    """
    Configuration for ingesting markdown and text documents from a GitHub repository.
    """

    github_token: SecretStr = Field(
        description="GitHub access token (PAT or GitHub App installation token)."
    )
    repository: str = Field(
        description=(
            "Repository to ingest, as 'owner/repo' or 'https://github.com/owner/repo'."
        )
    )

    # Never read by the source. Accepted with a warning so recipes that set
    # them keep working; the config rejects unknown keys otherwise.
    _document_mapping_removed = pydantic_removed_field(
        "document_mapping", month="November", year=2026
    )
    _hierarchy_removed = pydantic_removed_field(
        "hierarchy", month="November", year=2026
    )

    @field_validator("github_token")
    @classmethod
    def validate_github_token(cls, value: SecretStr) -> SecretStr:
        if not value or not value.get_secret_value().strip():
            raise ValueError("github_token is required and cannot be empty")
        return value

    @field_validator("repository")
    @classmethod
    def validate_repository(cls, value: str) -> str:
        return parse_repo_identifier(value)
