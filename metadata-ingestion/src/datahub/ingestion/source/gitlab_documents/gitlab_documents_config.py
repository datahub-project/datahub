"""Configuration for the gitlab-documents ingestion source."""

from typing import Dict

from pydantic import Field, SecretStr, field_validator, model_validator

from datahub.ingestion.source.github_documents.github_documents_config import (
    RepositoryDocumentsSourceConfigBase,
)
from datahub.ingestion.source.gitlab_documents.gitlab_api import (
    DEFAULT_GITLAB_URL,
    parse_project_path,
)


class GitLabDocumentsSourceConfig(RepositoryDocumentsSourceConfigBase):
    """
    Configuration for ingesting markdown and text documents from a GitLab project.
    """

    gitlab_token: SecretStr = Field(
        description=(
            "GitLab personal, project, or group access token with the read_api "
            "scope. Project and group tokens need at least the Reporter role."
        )
    )
    base_url: str = Field(
        default=DEFAULT_GITLAB_URL,
        description=(
            "GitLab instance URL. Set this for self-managed GitLab, for example "
            "'https://gitlab.example.com'. Inferred from repository when that is a URL."
        ),
    )
    repository: str = Field(
        description=(
            "Project to ingest, as its full path ('group/subgroup/project') or URL "
            "('https://gitlab.com/group/project')."
        )
    )

    @field_validator("gitlab_token")
    @classmethod
    def validate_gitlab_token(cls, value: SecretStr) -> SecretStr:
        if not value or not value.get_secret_value().strip():
            raise ValueError("gitlab_token is required and cannot be empty")
        return value

    @field_validator("base_url")
    @classmethod
    def validate_base_url(cls, value: str) -> str:
        value = value.strip().rstrip("/")
        if not value.startswith(("http://", "https://")):
            raise ValueError("base_url must start with http:// or https://")
        return value

    @model_validator(mode="before")
    @classmethod
    def resolve_repository(cls, values: object) -> object:
        if not isinstance(values, dict):
            return values
        repository = values.get("repository")
        base_url = values.get("base_url")
        if not isinstance(repository, str):
            return values
        resolved: Dict[str, object] = dict(values)
        base_url, project_path = parse_project_path(
            repository, base_url if isinstance(base_url, str) else None
        )
        resolved["repository"] = project_path
        resolved["base_url"] = base_url
        return resolved
