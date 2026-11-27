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
from datahub.ingestion.source.gitlab_documents.gitlab_api import (
    GitLabApiClient,
    GitLabProject,
)
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
        for repository, candidate in self._candidate_repositories().items():
            default_branch, lookup_error = candidate
            branch = self.config.configured_branch or default_branch
            if branch is None:
                reason = (
                    f"Could not read the project: {lookup_error}"
                    if lookup_error
                    else "No default branch. The project may be empty, or the "
                    "token's role cannot read its repository (it needs at least "
                    "Reporter). Set branch to read a specific branch."
                )
                self._skip_failed_repository(repository, None, RuntimeError(reason))
                continue
            targets.append(RepositoryTarget(repository=repository, branch=branch))
        return targets

    def _candidate_repositories(
        self,
    ) -> Dict[str, Tuple[Optional[str], Optional[str]]]:
        """Each repository's default branch, or None with the lookup error."""
        candidates: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
        for repository in self.config.explicit_repositories:
            if self.config.configured_branch:
                candidates[repository] = (None, None)
            else:
                branch = self.client.get_default_branch(repository)
                error = None if branch else self.client.last_api_error
                candidates[repository] = (branch, error)
        if self.config.group:
            for project in self._group_projects_or_skip(self.config.group):
                known_branch, _ = candidates.get(project.path, (None, None))
                if known_branch is None:
                    candidates[project.path] = (project.default_branch, None)
        return candidates

    def _group_projects_or_skip(self, group: str) -> List[GitLabProject]:
        if not self.config.explicit_repositories:
            return self._discover_group_projects(group)
        try:
            return self._discover_group_projects(group)
        except Exception as exc:
            # The named projects can still be imported; the group's documents
            # from earlier runs are kept.
            self._keep_previous_documents()
            self.report.warning(
                title="Group projects not listed",
                message=(
                    "The group's projects could not be listed, so only the projects "
                    "named in repository or repositories were imported, and "
                    "documents imported by earlier runs are kept."
                ),
                context=f"group={group}",
                exc=exc,
            )
            return []

    def _discover_group_projects(self, group: str) -> List[GitLabProject]:
        listing = self.client.list_group_projects(
            group,
            include_subgroups=self.config.include_subgroups,
            include_archived=self.config.include_archived,
            max_projects=self.config.max_projects,
            allowed=self.config.project_pattern.allowed,
        )
        if listing.truncated:
            self._keep_previous_documents()
            self.report.warning(
                title="Group projects truncated by max_projects",
                message=(
                    "More projects matched than max_projects allows, so only the "
                    "first ones were imported and documents imported by earlier "
                    "runs are kept. Raise max_projects or narrow project_pattern."
                ),
                context=f"group={group}, max_projects={self.config.max_projects}",
            )
        return listing.projects

    @classmethod
    def _connection_target(
        cls, client: RepositoryDocumentsClient, config: GitLabDocumentsSourceConfig
    ) -> RepositoryTarget:
        if not config.multi_repository:
            return super()._connection_target(client, config)
        gitlab = cast(GitLabApiClient, client)
        # Ingestion skips a project without a readable branch and imports the
        # rest, so the check uses the first project a run would import.
        for repository in config.explicit_repositories:
            branch = config.configured_branch or gitlab.get_default_branch(repository)
            if branch:
                return RepositoryTarget(repository=repository, branch=branch)
        if config.group:
            listing = gitlab.list_group_projects(
                config.group,
                include_subgroups=config.include_subgroups,
                include_archived=config.include_archived,
                max_projects=config.max_projects,
                allowed=config.project_pattern.allowed,
            )
            for project in listing.projects:
                branch = config.configured_branch or project.default_branch
                if branch:
                    return RepositoryTarget(repository=project.path, branch=branch)
        raise RuntimeError(
            "No configured project has a default branch the token can read. Check "
            "the group path, project_pattern and the token's role (at least "
            "Reporter), or set branch."
        )
