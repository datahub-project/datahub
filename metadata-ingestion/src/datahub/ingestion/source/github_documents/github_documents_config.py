from pydantic import Field, SecretStr, field_validator

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
