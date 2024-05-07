import contextlib
import hashlib
import pathlib
from typing import Iterator

import github.Repository
from datahub.configuration.common import ConfigModel
from git import Actor, Repo
from github import Auth, Github
from loguru import logger
from pygitops._util import checkout_pull_branch
from pygitops.exceptions import PyGitOpsError
from pygitops.operations import (
    feature_branch,
    get_default_branch,
    get_updated_repo,
    stage_commit_push_changes,
)


class GitHubRepoConfig(ConfigModel):
    github_repo_org: str
    github_repo_name: str
    token: str


class GitHubRepoWrapper:
    def __init__(self, config: GitHubRepoConfig, base_temp_dir: pathlib.Path):
        self.config = config
        self.repo_dir = base_temp_dir / self._make_unique_clone_suffix(config)

        logger.info(f"Cloning repo {self.get_owner_and_repo()} to {self.repo_dir}")
        self.repo: Repo = get_updated_repo(
            repo_url=self._make_remote_url(self.config),
            clone_dir=self.repo_dir,
            force=True,
        )
        self.actor = Actor("acryl-bot", "github-bot@acryl.io")

    @classmethod
    def _make_remote_url(cls, config: GitHubRepoConfig) -> str:
        return f"https://{config.token}:@github.com/{config.github_repo_org}/{config.github_repo_name}.git"

    @classmethod
    def _make_unique_clone_suffix(cls, config: GitHubRepoConfig) -> str:
        remote_url = cls._make_remote_url(config)

        suffix = hashlib.sha256()
        suffix.update(remote_url.encode())
        return f"{config.github_repo_name}-{suffix.digest().hex()[:16]}"

    def get_owner_and_repo(self) -> str:
        return f"{self.config.github_repo_org}/{self.config.github_repo_name}"

    @contextlib.contextmanager
    def feature_branch(self, branch_name: str) -> Iterator[None]:
        with feature_branch(self.repo, branch_name):
            yield

    @contextlib.contextmanager
    def long_running_branch(self, branch_name: str) -> Iterator[None]:
        # The `feature_branch` context manager throws an error if the branch already exists.
        # We want to reuse the branch if it already exists.
        try:
            self.repo.heads[self.get_default_branch()].checkout(force=True)

            # TODO: This makes the assumption that the branch is deleted when the PR is merged.
            # If that's not the case, then we need to check for open PRs first and use the branch
            # only if the associated PR is open.
            origin = self.repo.remotes.origin
            origin.fetch(prune=True)

            # TODO: Improve handling for when the branch is deleted on the remote but not locally.
            checkout_pull_branch(self.repo, branch_name, force=True)

            yield
        except PyGitOpsError:
            # If the branch doesn't exist in the remote, we can just
            # create a new branch locally.
            with feature_branch(self.repo, branch_name):
                yield

    def stage_commit_and_push(
        self,
        branch_name: str,
        commit_message: str,
        force: bool = False,
    ) -> None:
        stage_commit_push_changes(
            self.repo,
            branch_name,
            self.actor,
            commit_message=commit_message,
            kwargs_to_push=dict(force_with_lease=True) if force else {},
        )

    def get_default_branch(self) -> str:
        return get_default_branch(self.repo)

    def force_checkout(self, branch_name: str) -> None:
        self.repo.git.checkout(branch_name, force=True)

        # Clean up any untracked files.
        self.repo.git.clean("-df")

    def upsert_github_pr(
        self,
        title: str,
        body_for_create: str,
        body_for_append: str,
        branch: str,
    ) -> None:
        """Create or update a GitHub PR for the current branch."""

        github_api = Github(auth=Auth.Token(self.config.token))
        owner = self.config.github_repo_org
        repo: github.Repository.Repository = github_api.get_repo(
            self.get_owner_and_repo(), lazy=True
        )

        base_branch = self.get_default_branch()

        # Check if the PR already exists.
        prs = repo.get_pulls(
            state="open",
            head=f"{owner}:{branch}",
        )
        if prs.totalCount >= 1:
            if prs.totalCount > 1:
                logger.warning(
                    f"Found {prs.totalCount} PRs for branch {branch}, using the first one."
                )
            pr = next(iter(prs))

            current_body = pr.body
            updated_body = f"{current_body}{body_for_append}"

            logger.info(f"PR already opened, editing instead: {pr.html_url}")
            pr.edit(title=title, body=updated_body)
        else:
            # If it doesn't exist, create it.
            pr = repo.create_pull(
                title=title,
                body=body_for_create,
                head=branch,
                base=base_branch,
            )
            logger.info(f"Created PR {pr.html_url}")
