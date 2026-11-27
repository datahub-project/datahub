from typing import Dict, List, Optional, Tuple, cast
from urllib.parse import quote

from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import SourceCapability
from datahub.ingestion.source.gitlab_documents.gitlab_api import GitLabApiClient
from datahub.ingestion.source.gitlab_documents.gitlab_documents_config import (
    GitLabDocumentsSourceConfig,
)
from datahub.ingestion.source.repository_documents.client import (
    RepositoryDocumentsClient,
    RepositoryTarget,
)
from datahub.ingestion.source.repository_documents.source import (
    RepositoryDocumentsSourceBase,
)


@platform_name("GitLab")
@config_class(GitLabDocumentsSourceConfig)
@support_status(SupportStatus.ALPHA)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
class GitLabDocumentsSource(RepositoryDocumentsSourceBase[GitLabDocumentsSourceConfig]):
    """
    Ingest markdown and text files from GitLab projects as DataHub Document entities.

    Works with GitLab.com and self-managed GitLab through the REST API (v4).
    Preserves each project's folder structure as parent-child document
    relationships, and behaves like the ``github-documents`` source otherwise.
    Imports one project, a list of projects, or every project in a group.
    """

    platform = "gitlab"
    platform_display_name = "GitLab"
    client: GitLabApiClient

    @classmethod
    def _parse_config(cls, config_dict: dict) -> GitLabDocumentsSourceConfig:
        return GitLabDocumentsSourceConfig.parse_obj(config_dict)

    @classmethod
    def _make_client(cls, config: GitLabDocumentsSourceConfig) -> GitLabApiClient:
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

    @property
    def _multi_repository(self) -> bool:
        return self.config.multi_repository

    def _targets(self) -> List[RepositoryTarget]:
        if not self.config.multi_repository:
            return super()._targets()

        targets: List[RepositoryTarget] = []
        for repository, default_branch in self._candidate_repositories():
            branch = self._configured_branch() or default_branch
            if branch is None:
                self._skip_failed_repository(
                    RepositoryTarget(repository=repository, branch=""),
                    RuntimeError(
                        "No default branch. The project may be empty, or the "
                        "token's role cannot read its repository (it needs at "
                        "least Reporter). Set branch to read a specific branch."
                    ),
                )
                continue
            targets.append(RepositoryTarget(repository=repository, branch=branch))
        return targets

    def _configured_branch(self) -> Optional[str]:
        # branch defaults to "main", which rarely fits every project of a group,
        # so it only overrides the projects' own default branch when it was set.
        return self.config.branch if "branch" in self.config.model_fields_set else None

    def _candidate_repositories(self) -> List[Tuple[str, Optional[str]]]:
        candidates: Dict[str, Optional[str]] = {
            repository: (
                None
                if self._configured_branch()
                else self.client.get_default_branch(repository)
            )
            for repository in self.config.explicit_repositories
        }
        if self.config.group:
            for repository, default_branch in self._discover_group_projects(
                self.config.group
            ):
                candidates.setdefault(repository, default_branch)
        return list(candidates.items())

    def _discover_group_projects(self, group: str) -> List[Tuple[str, Optional[str]]]:
        listing = self.client.list_group_projects(
            group,
            include_subgroups=self.config.include_subgroups,
            include_archived=self.config.include_archived,
            max_projects=self.config.max_projects,
        )
        if listing.truncated:
            self._keep_previous_documents()
            self.report.warning(
                title="Group projects truncated by max_projects",
                message=(
                    "The group has more projects than max_projects allows, so "
                    "only the first ones were imported and documents imported by "
                    "earlier runs are kept. Raise max_projects or narrow "
                    "project_pattern."
                ),
                context=f"group={group}, max_projects={self.config.max_projects}",
            )
        return [
            (project.path, project.default_branch)
            for project in listing.projects
            if self.config.project_pattern.allowed(project.path)
        ]

    @classmethod
    def _connection_target(
        cls, client: RepositoryDocumentsClient, config: GitLabDocumentsSourceConfig
    ) -> RepositoryTarget:
        if not config.multi_repository:
            return super()._connection_target(client, config)
        gitlab = cast(GitLabApiClient, client)
        configured_branch = (
            config.branch if "branch" in config.model_fields_set else None
        )
        explicit = config.explicit_repositories
        if explicit:
            repository = explicit[0]
            branch = configured_branch or gitlab.get_default_branch(repository)
        else:
            listing = gitlab.list_group_projects(
                config.group or "",
                include_subgroups=config.include_subgroups,
                include_archived=config.include_archived,
                max_projects=1,
            )
            if not listing.projects:
                raise RuntimeError(f"No projects found in group '{config.group}'.")
            repository = listing.projects[0].path
            branch = configured_branch or listing.projects[0].default_branch
        if not branch:
            raise RuntimeError(
                f"Could not find a default branch for {repository}. Check the "
                "token's role (at least Reporter) or set branch."
            )
        return RepositoryTarget(repository=repository, branch=branch)
