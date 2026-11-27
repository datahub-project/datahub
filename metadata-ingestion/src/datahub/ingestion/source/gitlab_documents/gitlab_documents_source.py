from typing import Dict, Iterable, List, Optional
from urllib.parse import quote

from pydantic import BaseModel

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


class _Candidate(BaseModel, frozen=True):
    default_branch: Optional[str] = None
    lookup_error: Optional[str] = None


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
            branch = self.config.configured_branch or candidate.default_branch
            if branch is None:
                reason = (
                    f"Could not read the project: {candidate.lookup_error}"
                    if candidate.lookup_error
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
    ) -> Dict[str, _Candidate]:
        candidates: Dict[str, _Candidate] = {}
        for repository in self.config.explicit_repositories:
            if self.config.configured_branch:
                candidates[repository] = _Candidate()
            else:
                branch = self.client.get_default_branch(repository)
                candidates[repository] = _Candidate(
                    default_branch=branch,
                    lookup_error=None if branch else self.client.last_api_error,
                )
        if self.config.group:
            # GitLab paths are case-insensitive: a project named in repositories with
            # different casing from the group listing is the same project, so it
            # keeps its configured spelling instead of being imported twice.
            named = {repository.casefold(): repository for repository in candidates}
            for project in self._group_projects_or_skip(self.config.group):
                repository = named.get(project.path.casefold(), project.path)
                known = candidates.get(repository)
                if known is None or known.default_branch is None:
                    candidates[repository] = _Candidate(
                        default_branch=project.default_branch
                    )
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
                    "named in repositories were imported, and "
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
    def _connection_targets(
        cls, client: RepositoryDocumentsClient, config: GitLabDocumentsSourceConfig
    ) -> Iterable[RepositoryTarget]:
        if not config.multi_repository:
            yield from super()._connection_targets(client, config)
            return
        if not isinstance(client, GitLabApiClient):
            raise TypeError(f"Expected a GitLabApiClient, got {type(client).__name__}")
        gitlab = client
        found = False
        for repository in config.explicit_repositories:
            branch = config.configured_branch or gitlab.get_default_branch(repository)
            if branch:
                found = True
                yield RepositoryTarget(repository=repository, branch=branch)
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
                    found = True
                    yield RepositoryTarget(repository=project.path, branch=branch)
        if not found:
            raise RuntimeError(
                "No configured project has a default branch the token can read. "
                "Check the group path, project_pattern and the token's role (at "
                "least Reporter), or set branch."
            )
