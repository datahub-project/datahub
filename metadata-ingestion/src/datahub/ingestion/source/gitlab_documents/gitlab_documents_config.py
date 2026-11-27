from typing import Dict, List, Optional

from pydantic import Field, SecretStr, field_validator, model_validator

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.validate_field_deprecation import pydantic_field_deprecated
from datahub.ingestion.source.gitlab_documents.gitlab_api import (
    DEFAULT_GITLAB_URL,
    parse_project_path,
)
from datahub.ingestion.source.repository_documents.config import (
    RepositoryDocumentsSourceConfigBase,
)

_GROUP_ONLY_OPTIONS = frozenset(
    ("include_subgroups", "include_archived", "project_pattern", "max_projects")
)


class GitLabDocumentsSourceConfig(RepositoryDocumentsSourceConfigBase):
    """
    Configuration for ingesting markdown and text documents from GitLab projects.
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
            "'https://gitlab.example.com'. Inferred from the first project URL in repositories."
        ),
    )
    repositories: List[str] = Field(
        default_factory=list,
        description=(
            "Projects to ingest, each as a full path ('group/subgroup/project') or "
            "URL ('https://gitlab.com/group/project') on the same GitLab instance. "
            "Each project is read from its default branch unless branch is set, gets "
            "its own root document, nested under parent_document_urn when one is "
            "set, and is skipped with a warning if it cannot be read. Can be "
            "combined with group."
        ),
    )
    # Overrides the required str on the base config so recipes can omit it.
    repository: Optional[str] = Field(  # type: ignore[assignment]
        default=None,
        json_schema_extra={"hidden_from_docs": True},
        description="Deprecated: use repositories.",
    )
    _repository_deprecated = pydantic_field_deprecated(
        "repository",
        message=(
            "repository is deprecated and will be removed in a future release. "
            "Use repositories, a list of project paths, instead. Each project in "
            "repositories is read from its default branch unless branch is set."
        ),
    )
    group: Optional[str] = Field(
        default=None,
        description=(
            "Full path of a group ('group' or 'group/subgroup'). Every project in it "
            "is ingested, narrowed by project_pattern. Needs a token that can list the "
            "group's projects, such as a group access token with at least the "
            "Reporter role."
        ),
    )
    include_subgroups: bool = Field(
        default=True,
        description="With group, also ingest projects in its subgroups.",
    )
    include_archived: bool = Field(
        default=False,
        description="With group, also ingest archived projects.",
    )
    project_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns for the full paths of projects found through group, for "
            "example 'acme/data/.*'. Projects named in repositories are "
            "always ingested."
        ),
    )
    max_projects: int = Field(
        default=100,
        ge=1,
        description=(
            "Maximum number of projects to ingest from group per run, counting only "
            "those project_pattern allows. If more match, the extra projects are "
            "skipped with a report warning and documents imported by earlier runs "
            "are kept."
        ),
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

    @model_validator(mode="after")
    def resolve_repositories(self) -> "GitLabDocumentsSourceConfig":
        entries = [
            entry
            for entry in [self.repository, *self.repositories]
            if entry is not None
        ]
        if not entries and not self.group:
            raise ValueError("Set repositories or group.")

        # A project URL sets base_url unless a self-managed URL was configured.
        # Recipes always carry the gitlab.com default, so that counts as unset.
        explicit_base_url = (
            self.base_url
            if "base_url" in self.model_fields_set
            and self.base_url != DEFAULT_GITLAB_URL
            else None
        )
        first_url = next(
            (e for e in entries if e.strip().startswith(("http://", "https://"))), None
        )
        base_url = (
            explicit_base_url
            or (parse_project_path(first_url, None)[0] if first_url else None)
            or self.base_url
        )
        parsed = [parse_project_path(entry, base_url)[1] for entry in entries]
        if self.repository is not None:
            self.repository, *parsed = parsed
        self.repositories = parsed
        self.base_url = base_url
        if self.group:
            self.group = self.group.strip().strip("/")
            if not self.group:
                raise ValueError("group cannot be empty.")
        else:
            group_only = sorted(self.model_fields_set & _GROUP_ONLY_OPTIONS)
            if group_only:
                raise ValueError(f"{', '.join(group_only)} only apply with group.")
        return self

    @property
    def configured_branch(self) -> Optional[str]:
        # branch defaults to "main", which rarely fits every project of a group,
        # so it only overrides a project's own default branch when it was set.
        return self.branch if "branch" in self.model_fields_set else None

    @property
    def explicit_repositories(self) -> List[str]:
        # GitLab paths are case-insensitive, so 'Acme/Docs' and 'acme/docs' are one
        # project; the first spelling wins.
        named = [self.repository] if self.repository else []
        unique: Dict[str, str] = {}
        for repository in [*named, *self.repositories]:
            unique.setdefault(repository.casefold(), repository)
        return list(unique.values())

    @property
    def multi_repository(self) -> bool:
        return bool(self.group or self.repositories)
